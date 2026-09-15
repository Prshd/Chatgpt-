package main

import (
    "database/sql"
    "embed"
    "encoding/json"
    "fmt"
    "html/template"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "time"

    _ "modernc.org/sqlite"
)

//go:embed static/*
var staticFS embed.FS

type App struct{ db *sql.DB }

func main() {
    dir, err := os.UserConfigDir(); if err != nil { log.Fatal(err) }
    dir = filepath.Join(dir, "SmartReconciliation")
    if err = os.MkdirAll(dir, 0755); err != nil { log.Fatal(err) }
    db, err := sql.Open("sqlite", filepath.Join(dir, "recon.db")); if err != nil { log.Fatal(err) }
    defer db.Close()
    if err = initDB(db); err != nil { log.Fatal(err) }
    app := &App{db: db}
    mux := http.NewServeMux()
    mux.HandleFunc("/", app.index)
    mux.HandleFunc("/api/health", app.health)
    mux.HandleFunc("/api/datasets", app.datasets)
    mux.HandleFunc("/api/import", app.importUnsupported)
    mux.HandleFunc("/api/pair/reconcile", app.pairReconcile)
    mux.HandleFunc("/api/pair/results", app.pairResults)
    addr := "127.0.0.1:8765"
    log.Printf("Smart Reconciliation listening on http://%s", addr)
    log.Fatal(http.ListenAndServe(addr, mux))
}

func initDB(db *sql.DB) error {
    _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;
CREATE TABLE IF NOT EXISTS datasets(id INTEGER PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, rows INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY, dataset_id INTEGER NOT NULL, business_date TEXT, rrn TEXT, reference TEXT, amount_cents INTEGER NOT NULL DEFAULT 0, raw_json TEXT, matched INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS idx_tx_dataset_date ON transactions(dataset_id,business_date);
CREATE INDEX IF NOT EXISTS idx_tx_dataset_rrn_amount ON transactions(dataset_id,rrn,amount_cents);
CREATE INDEX IF NOT EXISTS idx_tx_dataset_ref_amount ON transactions(dataset_id,reference,amount_cents);
CREATE TABLE IF NOT EXISTS pair_runs(id INTEGER PRIMARY KEY, left_dataset INTEGER NOT NULL, right_dataset INTEGER NOT NULL, stage INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, started_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pair_matches(run_id INTEGER NOT NULL, left_id INTEGER NOT NULL, right_id INTEGER NOT NULL, stage INTEGER NOT NULL, PRIMARY KEY(run_id,left_id), UNIQUE(run_id,right_id));`)
    return err
}

func (a *App) index(w http.ResponseWriter, r *http.Request) { b,_:=staticFS.ReadFile("static/index.html"); w.Header().Set("Content-Type","text/html; charset=utf-8"); _,_=w.Write(b) }
func (a *App) health(w http.ResponseWriter,r *http.Request){ writeJSON(w,map[string]any{"ok":true,"sqlite":true}) }
func (a *App) datasets(w http.ResponseWriter,r *http.Request){ rows,err:=a.db.Query("SELECT id,name,kind,rows,created_at FROM datasets ORDER BY id DESC"); if err!=nil{http.Error(w,err.Error(),500);return}; defer rows.Close(); out:=[]map[string]any{}; for rows.Next(){var id,n,k,ct string; var nrows int64; if err:=rows.Scan(&id,&n,&k,&nrows,&ct);err!=nil{continue}; out=append(out,map[string]any{"id":id,"name":n,"kind":k,"rows":nrows,"created_at":ct})}; writeJSON(w,out) }
func (a *App) importUnsupported(w http.ResponseWriter,r *http.Request){http.Error(w,"Importer is being implemented in the rebuild; SQLite schema is ready.",501)}

func (a *App) pairReconcile(w http.ResponseWriter,r *http.Request){
    if r.Method!="POST"{http.Error(w,"POST required",405);return}
    var req struct{Left,Right int64}; if json.NewDecoder(r.Body).Decode(&req)!=nil || req.Left<=0 || req.Right<=0 || req.Left==req.Right {http.Error(w,"left and right dataset ids required",400);return}
    now:=time.Now().UTC().Format(time.RFC3339)
    res,err:=a.db.Exec("INSERT INTO pair_runs(left_dataset,right_dataset,status,started_at,updated_at) VALUES(?,?,?,?,?)",req.Left,req.Right,"running",now,now); if err!=nil{http.Error(w,err.Error(),500);return}; id,_:=res.LastInsertId()
    if err:=runPairStages(a.db,id,req.Left,req.Right);err!=nil{_,_=a.db.Exec("UPDATE pair_runs SET status='error',updated_at=? WHERE id=?",time.Now().UTC().Format(time.RFC3339),id);http.Error(w,err.Error(),500);return}
    writeJSON(w,map[string]any{"run_id":id,"status":"done"})
}

func runPairStages(db *sql.DB,runID,left,right int64) error {
    for stage:=1;stage<=4;stage++ { tx,err:=db.Begin(); if err!=nil{return err}; var cond string; switch stage {case 1: cond="same-day rrn"; case 2: cond="same-day reference"; case 3: cond="cross-day rrn"; case 4: cond="cross-day reference"}; _=cond
        // Matching is deliberately SQL-driven and only accepts unique key+amount candidates.
        // Stage 1/2 require equal business dates; stage 3/4 permit different dates.
        key:="rrn"; if stage==2||stage==4 {key="reference"}; day:="AND l.business_date=r.business_date"; if stage>=3 {day=""}
        q:=fmt.Sprintf(`INSERT OR IGNORE INTO pair_matches(run_id,left_id,right_id,stage)
SELECT ?,l.id,r.id,? FROM transactions l JOIN transactions r ON l.%s<>'' AND l.%s=r.%s AND l.amount_cents=r.amount_cents %s
WHERE l.dataset_id=? AND r.dataset_id=? AND l.deleted=0 AND r.deleted=0 AND l.matched=0 AND r.matched=0
AND (SELECT COUNT(*) FROM transactions x WHERE x.dataset_id=l.dataset_id AND x.deleted=0 AND x.matched=0 AND x.%s=l.%s AND x.amount_cents=l.amount_cents)=1
AND (SELECT COUNT(*) FROM transactions y WHERE y.dataset_id=r.dataset_id AND y.deleted=0 AND y.matched=0 AND y.%s=r.%s AND y.amount_cents=r.amount_cents)=1`,key,key,key,day,key,key,key,key)
        if _,err=tx.Exec(q,runID,stage,left,right);err!=nil{tx.Rollback();return err}
        if _,err=tx.Exec(`UPDATE transactions SET matched=1 WHERE id IN (SELECT left_id FROM pair_matches WHERE run_id=? AND stage=?)`,runID,stage);err!=nil{tx.Rollback();return err}
        if _,err=tx.Exec(`UPDATE transactions SET matched=1 WHERE id IN (SELECT right_id FROM pair_matches WHERE run_id=? AND stage=?)`,runID,stage);err!=nil{tx.Rollback();return err}
        if err=tx.Commit();err!=nil{return err}
    }
    _,err:=db.Exec("UPDATE pair_runs SET stage=4,status='done',updated_at=? WHERE id=?",time.Now().UTC().Format(time.RFC3339),runID); return err
}

func (a *App) pairResults(w http.ResponseWriter,r *http.Request){ runID,_:=strconv.ParseInt(r.URL.Query().Get("run_id"),10,64); limit:=500; off,_:=strconv.Atoi(r.URL.Query().Get("offset")); rows,err:=a.db.Query(`SELECT m.left_id,m.right_id,m.stage FROM pair_matches m WHERE m.run_id=? ORDER BY m.left_id LIMIT ? OFFSET ?`,runID,limit,off); if err!=nil{http.Error(w,err.Error(),500);return}; defer rows.Close(); out:=[]map[string]any{}; for rows.Next(){var l,rr,st int64; rows.Scan(&l,&rr,&st); out=append(out,map[string]any{"left_id":l,"right_id":rr,"stage":st})}; writeJSON(w,map[string]any{"rows":out,"next_offset":off+len(out)}) }
func writeJSON(w http.ResponseWriter,v any){w.Header().Set("Content-Type","application/json"); _=json.NewEncoder(w).Encode(v)}
var _ = template.HTMLEscapeString
var _ = strings.TrimSpace
