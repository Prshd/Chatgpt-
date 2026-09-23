package main

import (
    "archive/zip"
    "bufio"
    "database/sql"
    "embed"
    "encoding/csv"
    "encoding/json"
    "fmt"
    "html/template"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "time"
    "runtime"

    "github.com/xuri/excelize/v2"
    _ "modernc.org/sqlite"
)

//go:embed static/*
var staticFS embed.FS

type importJob struct {
    mu sync.RWMutex
    ID string
    DatasetID int64
    Name, Kind, Status, Error string
    Rows int64
    StartedAt, UpdatedAt time.Time
    Bytes int64
    TotalBytes int64
    Rate float64
}
type App struct { db *sql.DB; heavy sync.Mutex; jobs sync.Map }
type record struct { Date, RRN, Ref string; Amount int64; Raw string }
type rowMapper struct { headers []string; dateIdx,rrnIdx,refIdx,amountIdx int }

func newRowMapper(headers []string) rowMapper {
    m:=rowMapper{headers:headers,dateIdx:-1,rrnIdx:-1,refIdx:-1,amountIdx:-1}
    for i,h:=range headers {
        n:=norm(h)
        switch n {
        case "date","businessdate","transactiondate","valuedate","postingdate": if m.dateIdx<0 {m.dateIdx=i}
        case "rrn","retrievalreference","retrievalreferencenumber": if m.rrnIdx<0 {m.rrnIdx=i}
        case "reference","ref","transactionreference","externalreference": if m.refIdx<0 {m.refIdx=i}
        case "amount","transactionamount","credit","debit": if m.amountIdx<0 {m.amountIdx=i}
        }
    }
    return m
}

func (m rowMapper) record(vals []string) record {
    get:=func(i int) string { if i>=0 && i<len(vals) { return vals[i] }; return "" }
    // Keep Raw JSON compatible with the existing mapRow behaviour while avoiding
    // the repeated header normalization/search done for every Excel row.
    raw:=make(map[string]any,len(m.headers))
    for i,k:=range m.headers { if i<len(vals) { raw[k]=vals[i] } }
    b,_:=json.Marshal(raw)
    return record{Date:get(m.dateIdx),RRN:get(m.rrnIdx),Ref:get(m.refIdx),Amount:parseAmount(get(m.amountIdx)),Raw:string(b)}
}

type importRequest struct { Kind string }

func main() {
    dir, err := os.UserConfigDir(); if err != nil { log.Fatal(err) }
    dir = filepath.Join(dir, "SmartReconciliation")
    if err = os.MkdirAll(dir, 0755); err != nil { log.Fatal(err) }
    db, err := sql.Open("sqlite", filepath.Join(dir, "recon.db")); if err != nil { log.Fatal(err) }
    defer db.Close()
    db.SetMaxOpenConns(1)
    if err = initDB(db); err != nil { log.Fatal(err) }
    app := &App{db: db}
    mux := http.NewServeMux()
    mux.HandleFunc("/", app.index)
    mux.HandleFunc("/api/health", app.health)
    mux.HandleFunc("/api/datasets", app.datasets)
    mux.HandleFunc("/api/import", app.importFile)
    mux.HandleFunc("/api/import/status", app.importStatus)
    mux.HandleFunc("/api/import/progress", app.importProgress)
    mux.HandleFunc("/api/clear", app.clearData)
    mux.HandleFunc("/api/pair/reconcile", app.pairReconcile)
    mux.HandleFunc("/api/pair/results", app.pairResults)
    mux.HandleFunc("/api/outstanding", app.outstanding)
    mux.HandleFunc("/api/stats", app.stats)
    addr := "127.0.0.1:8765"
    log.Printf("Smart Reconciliation Version 100.2 listening on http://%s", addr)
    log.Fatal(http.ListenAndServe(addr, mux))
}

func initDB(db *sql.DB) error {
    _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY; PRAGMA foreign_keys=ON; PRAGMA cache_size=-65536; PRAGMA mmap_size=268435456; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS datasets(id INTEGER PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, rows INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY, dataset_id INTEGER NOT NULL, business_date TEXT, rrn TEXT, reference TEXT, amount_cents INTEGER NOT NULL DEFAULT 0, raw_json TEXT, deleted INTEGER NOT NULL DEFAULT 0, FOREIGN KEY(dataset_id) REFERENCES datasets(id));
CREATE INDEX IF NOT EXISTS idx_tx_ds_date ON transactions(dataset_id,business_date);
CREATE INDEX IF NOT EXISTS idx_tx_ds_rrn_amt_date ON transactions(dataset_id,rrn,amount_cents,business_date);
CREATE INDEX IF NOT EXISTS idx_tx_ds_ref_amt_date ON transactions(dataset_id,reference,amount_cents,business_date);
CREATE TABLE IF NOT EXISTS pair_runs(id INTEGER PRIMARY KEY, left_dataset INTEGER NOT NULL, right_dataset INTEGER NOT NULL, stage INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, started_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS ux_pair_run_active ON pair_runs(left_dataset,right_dataset) WHERE status IN ('running','paused');
CREATE TABLE IF NOT EXISTS pair_matches(run_id INTEGER NOT NULL, left_id INTEGER NOT NULL, right_id INTEGER NOT NULL, stage INTEGER NOT NULL, PRIMARY KEY(run_id,left_id), UNIQUE(run_id,right_id));
CREATE INDEX IF NOT EXISTS idx_pm_run_stage ON pair_matches(run_id,stage);
`)
    return err
}

func (a *App) index(w http.ResponseWriter, r *http.Request) { b,_:=staticFS.ReadFile("static/index.html"); w.Header().Set("Content-Type","text/html; charset=utf-8"); _,_=w.Write(b) }
func (a *App) health(w http.ResponseWriter,r *http.Request){ writeJSON(w,map[string]any{"ok":true,"sqlite":true,"version":"100.2"}) }

func (a *App) datasets(w http.ResponseWriter,r *http.Request){
    rows,err:=a.db.Query("SELECT id,name,kind,rows,created_at FROM datasets ORDER BY id DESC"); if err!=nil{http.Error(w,err.Error(),500);return}; defer rows.Close()
    out:=[]map[string]any{}; for rows.Next(){var id int64; var n,k,ct string; var nrows int64; if err:=rows.Scan(&id,&n,&k,&nrows,&ct);err!=nil{continue}; out=append(out,map[string]any{"id":id,"name":n,"kind":k,"rows":nrows,"created_at":ct})}; writeJSON(w,out)
}

func (a *App) importFile(w http.ResponseWriter,r *http.Request){
    if r.Method!="POST" { http.Error(w,"POST required",405); return }

    // Streaming raw-file upload path. The browser sends the file as the request
    // body so the server never builds a giant multipart buffer in RAM.
    if r.Header.Get("Content-Type") != "" && !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")),"multipart/") {
        kind:=strings.ToLower(strings.TrimSpace(r.Header.Get("X-Import-Kind"))); if kind=="" { kind="gl" }
        name:=filepath.Base(r.Header.Get("X-File-Name")); if name=="" || name=="." { name="upload.bin" }
        job:=newImportJob(name,kind,r.ContentLength)
        a.jobs.Store(job.ID,job)
        tmp,err:=os.CreateTemp("","smart-recon-upload-*")
        if err!=nil { a.finishJobError(job,err); http.Error(w,err.Error(),500); return }
        tmpPath:=tmp.Name()
        var copied int64
        buf:=make([]byte,1024*1024)
        for {
            n,e:=r.Body.Read(buf)
            if n>0 { wn,we:=tmp.Write(buf[:n]); if we!=nil {tmp.Close();os.Remove(tmpPath);a.finishJobError(job,we);http.Error(w,we.Error(),500);return};copied+=int64(wn);a.updateJobBytes(job,copied) }
            if e==io.EOF {break}; if e!=nil {tmp.Close();os.Remove(tmpPath);a.finishJobError(job,e);http.Error(w,e.Error(),500);return}
        }
        if err=tmp.Close();err!=nil {os.Remove(tmpPath);a.finishJobError(job,err);http.Error(w,err.Error(),500);return}
        go a.runImportJob(job,tmpPath)
        writeJSON(w,map[string]any{"ok":true,"job_id":job.ID,"name":name,"kind":kind})
        return
    }

    // Backward-compatible multipart path.
    if err:=r.ParseMultipartForm(8<<20); err!=nil { http.Error(w,err.Error(),400); return }
    kind:=strings.ToLower(strings.TrimSpace(r.FormValue("kind"))); if kind=="" { kind="gl" }
    file,head,err:=r.FormFile("file"); if err!=nil { http.Error(w,"file required",400); return }; defer file.Close()
    name:=filepath.Base(head.Filename)
    job:=newImportJob(name,kind,head.Size)
    a.jobs.Store(job.ID,job)
    tmp,err:=os.CreateTemp("","smart-recon-upload-*")
    if err!=nil {a.finishJobError(job,err);http.Error(w,err.Error(),500);return}
    tmpPath:=tmp.Name()
    if _,err=io.Copy(tmp,file);err!=nil {tmp.Close();os.Remove(tmpPath);a.finishJobError(job,err);http.Error(w,err.Error(),500);return}
    if err=tmp.Close();err!=nil {os.Remove(tmpPath);a.finishJobError(job,err);http.Error(w,err.Error(),500);return}
    go a.runImportJob(job,tmpPath)
    writeJSON(w,map[string]any{"ok":true,"job_id":job.ID,"name":name,"kind":kind})
}

func newImportJob(name,kind string,total int64)*importJob{
    now:=time.Now()
    return &importJob{ID:fmt.Sprintf("%d-%d",now.UnixNano(),runtime.NumGoroutine()),Name:name,Kind:kind,Status:"uploading",StartedAt:now,UpdatedAt:now,TotalBytes:total}
}
func (a *App) updateJobBytes(j *importJob,n int64){j.mu.Lock();j.Bytes=n;j.UpdatedAt=time.Now();if j.TotalBytes>0 && n>=j.TotalBytes && j.Status=="uploading"{j.Status="queued"};j.mu.Unlock()}
func (a *App) finishJobError(j *importJob,e error){j.mu.Lock();j.Status="error";j.Error=e.Error();j.UpdatedAt=time.Now();j.mu.Unlock()}
func (a *App) runImportJob(j *importJob,tmpPath string){
    defer os.Remove(tmpPath)
    j.mu.Lock();j.Status="importing";j.UpdatedAt=time.Now();j.mu.Unlock()
    f,e:=os.Open(tmpPath)
    if e==nil {
        var n int64
        var lastRows,lastBytes int64
        var last=time.Now()
        ds,n,e=a.importReaderWithProgress(f,j.Name,j.Kind,func(rows int64,committedBytes int64){
            now:=time.Now(); elapsed:=now.Sub(last).Seconds()
            j.mu.Lock();j.Rows=rows;j.UpdatedAt=now
            if elapsed>0 {j.Rate=float64(rows-lastRows)/elapsed}
            j.mu.Unlock()
            lastRows=rows;lastBytes=committedBytes;_ = lastBytes;last=now
        })
        _=f.Close()
        j.mu.Lock();j.DatasetID=ds;j.Rows=n;j.UpdatedAt=time.Now();j.mu.Unlock()
    }
    j.mu.Lock()
    if e!=nil {j.Status="error";j.Error=e.Error()} else {j.Status="done"}
    j.UpdatedAt=time.Now()
    j.mu.Unlock()
}
func (a *App) importStatus(w http.ResponseWriter,r *http.Request){
    id:=r.URL.Query().Get("job_id"); v,ok:=a.jobs.Load(id); if !ok {http.Error(w,"job not found",404);return}
    writeJSON(w,jobSnapshot(v.(*importJob)))
}
func jobSnapshot(j *importJob)map[string]any{
    j.mu.RLock();defer j.mu.RUnlock()
    pct:=0.0;if j.TotalBytes>0 {pct=float64(j.Bytes)*100/float64(j.TotalBytes);if pct>100{pct=100}}
    return map[string]any{"ok":true,"job_id":j.ID,"dataset_id":j.DatasetID,"name":j.Name,"kind":j.Kind,"status":j.Status,"rows":j.Rows,"bytes":j.Bytes,"total_bytes":j.TotalBytes,"upload_percent":pct,"rate":j.Rate,"error":j.Error,"updated_at":j.UpdatedAt.Format(time.RFC3339Nano)}
}
func (a *App) importProgress(w http.ResponseWriter,r *http.Request){
    id:=r.URL.Query().Get("job_id");v,ok:=a.jobs.Load(id);if !ok{http.Error(w,"job not found",404);return}
    j:=v.(*importJob);w.Header().Set("Content-Type","text/event-stream");w.Header().Set("Cache-Control","no-cache");w.Header().Set("Connection","keep-alive");w.Header().Set("X-Accel-Buffering","no")
    fl,ok:=w.(http.Flusher);if !ok{http.Error(w,"streaming unsupported",500);return}
    ticker:=time.NewTicker(300*time.Millisecond);defer ticker.Stop()
    send:=func(){b,_:=json.Marshal(jobSnapshot(j));fmt.Fprintf(w,"data: %s\n\n",b);fl.Flush()}
    send()
    for {select{case <-r.Context().Done():return;case <-ticker.C:send();snap:=jobSnapshot(j);if snap["status"]=="done"||snap["status"]=="error"||snap["status"]=="cancelled"{return}}}
}
func (a *App) importReaderWithProgress(f io.Reader, name, kind string, progress func(int64,int64))(int64,int64,error){
    a.heavy.Lock(); defer a.heavy.Unlock()
    res,err:=a.db.Exec("INSERT INTO datasets(name,kind,rows,created_at) VALUES(?,?,0,?)",name,kind,time.Now().UTC().Format(time.RFC3339));if err!=nil{return 0,0,err}
    ds,err:=res.LastInsertId();if err!=nil{return 0,0,err}
    const batchSize int64=50000
    var tx *sql.Tx
    var insert *sql.Stmt
    var count int64
    beginBatch:=func() error{var e error;tx,e=a.db.Begin();if e!=nil{return e};insert,e=tx.Prepare("INSERT INTO transactions(dataset_id,business_date,rrn,reference,amount_cents,raw_json) VALUES(?,?,?,?,?,?)");if e!=nil{_ = tx.Rollback();return e};return nil}
    commitBatch:=func() error{if insert!=nil{_ = insert.Close()};insert=nil;if tx!=nil{if e:=tx.Commit();e!=nil{return e}};tx=nil;_,e:=a.db.Exec("UPDATE datasets SET rows=? WHERE id=?",count,ds);return e}
    if err=beginBatch();err!=nil{return ds,count,err}
    fail:=func(e error)(int64,int64,error){if insert!=nil{_ = insert.Close()};if tx!=nil{_ = tx.Rollback()};return ds,count,e}
    add:=func(x record) error{
        if _,e:=insert.Exec(ds,normalizeDate(x.Date),clean(x.RRN),clean(x.Ref),x.Amount,x.Raw);e!=nil{return e}
        count++
        if count%batchSize==0{if e:=commitBatch();e!=nil{return e};if progress!=nil{progress(count,0)};if e:=beginBatch();e!=nil{return e}}
        return nil
    }
    ext:=strings.ToLower(filepath.Ext(name))
    switch ext{
    case ".xlsx",".xlsm":
        tmp,err:=os.CreateTemp("","recon-*.xlsx");if err!=nil{return fail(err)};tmpPath:=tmp.Name();defer os.Remove(tmpPath)
        if _,err=io.Copy(tmp,f);err!=nil{tmp.Close();return fail(err)};if err=tmp.Close();err!=nil{return fail(err)}
        book,err:=excelize.OpenFile(tmpPath);if err!=nil{return fail(err)};defer book.Close()
        sheets:=book.GetSheetList();if len(sheets)==0{return fail(fmt.Errorf("workbook has no sheets"))}
        rows,err:=book.Rows(sheets[0]);if err!=nil{return fail(err)};defer rows.Close()
        var mapper rowMapper
        for rows.Next(){vals,e:=rows.Columns();if e!=nil{return fail(e)};if mapper.headers==nil{mapper=newRowMapper(vals);continue};x:=mapper.record(vals);if isEmptyRecord(x){continue};if e=add(x);e!=nil{return fail(e)}}
    case ".csv",".txt":
        cr:=csv.NewReader(bufio.NewReader(f));cr.FieldsPerRecord=-1;var mapper rowMapper
        for{vals,e:=cr.Read();if e==io.EOF{break};if e!=nil{return fail(e)};if mapper.headers==nil{mapper=newRowMapper(vals);continue};x:=mapper.record(vals);if isEmptyRecord(x){continue};if e=add(x);e!=nil{return fail(e)}}
    default:
        dec:=json.NewDecoder(bufio.NewReader(f));var v any;if err:=dec.Decode(&v);err!=nil{return fail(err)};for _,obj:=range toObjects(v){x:=mapObject(obj);if isEmptyRecord(x){continue};if err:=add(x);err!=nil{return fail(err)}}
    }
    if insert!=nil{_ = insert.Close()};if tx!=nil{if err=tx.Commit();err!=nil{return ds,count,err};tx=nil}
    if _,err=a.db.Exec("UPDATE datasets SET rows=? WHERE id=?",count,ds);err!=nil{return ds,count,err}
    if progress!=nil{progress(count,0)}
    return ds,count,nil
}


