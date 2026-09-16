$ErrorActionPreference = 'Stop'
$path = Join-Path $env:GITHUB_WORKSPACE 'recon-src/src/static/index.html'
if (!(Test-Path $path)) { throw "Frontend not found: $path" }
$html = Get-Content -Raw -Encoding UTF8 $path
$marker = 'CEFT_RECON_VISIBLE_4STAGE_PATCH_V1'
if ($html.Contains($marker)) { Write-Host '4-stage visibility patch already present'; exit 0 }

$patch = @'
<!-- CEFT_RECON_VISIBLE_4STAGE_PATCH_V1 -->
<style>
#ceft-visible-4stage{margin:10px 14px;padding:12px;border:2px solid #1565C0;border-radius:9px;background:#F7FAFF;box-shadow:0 2px 5px rgba(0,0,0,.08)}
#ceft-visible-4stage .v4-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px}
#ceft-visible-4stage .v4-title{font-weight:800;color:#0D47A1;font-size:12px}
#ceft-visible-4stage .v4-note{font-size:10px;color:#667085}
#ceft-visible-4stage .v4-grid{display:grid;grid-template-columns:repeat(2,minmax(300px,1fr));gap:9px}
#ceft-visible-4stage .v4-card{background:#fff;border:1.5px solid #BFD7FF;border-radius:8px;padding:9px}
#ceft-visible-4stage .v4-card.bank{border-color:#A5D6A7}
#ceft-visible-4stage .v4-name{font-weight:800;font-size:11px}
#ceft-visible-4stage .v4-map{font-size:9.5px;margin-left:auto}
#ceft-visible-4stage .v4-buttons{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px;margin-top:8px}
#ceft-visible-4stage .v4-btn{border:0;border-radius:5px;color:#fff;padding:6px 4px;font-size:10px;font-weight:800;cursor:pointer;min-height:30px}
#ceft-visible-4stage .v4-btn:disabled{opacity:.42;cursor:not-allowed;filter:grayscale(.35)}
#ceft-visible-4stage .v4-b1{background:#1565C0}.v4-b2{background:#1B5E20}.v4-b3{background:#E65100}.v4-b4{background:#6A1B9A}
#ceft-visible-4stage .v4-status{margin-top:6px;font-size:10px;color:#667085;min-height:14px}
#ceft-visible-4stage .v4-progress{display:none;margin-top:6px;height:7px;background:#E5E7EB;border-radius:99px;overflow:hidden}
#ceft-visible-4stage .v4-fill{height:100%;width:0%;background:#1565C0;transition:width .2s linear}
#ceft-visible-4stage .v4-ready{display:inline-block;padding:2px 7px;border-radius:9px;font-size:9px;font-weight:800;background:#FFF3CD;color:#856404}
@media(max-width:800px){#ceft-visible-4stage .v4-grid{grid-template-columns:1fr}#ceft-visible-4stage .v4-buttons{grid-template-columns:repeat(2,1fr)}}
</style>
<script>
(function(){
  'use strict';
  const ROOT_ID='ceft-visible-4stage';
  const sides={
    ceft:{right:'CEFT Debit',prefix:'two-ceft',name:'GL Credit → CEFT Debit'},
    banker:{right:'Cash at Banker Credit',prefix:'two-banker',name:'GL Credit → Cash at Banker Credit'}
  };
  function esc(s){return String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}
  function host(){return document.getElementById('pg-upload')||document.querySelector('.pg.on')||document.body;}
  function state(kind){
    try{
      const x=window.TWO_PIPELINE_STATE&&window.TWO_PIPELINE_STATE[kind];
      if(x)return x;
    }catch(e){}
    return null;
  }
  function sideHasData(kind){
    try{
      const ids=window.RECON_SERVER_DATASET_IDS||{};
      if(kind==='ceft') return (ids.cr||[]).length>0&&(ids.dr||[]).length>0;
      return (ids.cr||[]).length>0&&(ids.bkcredit||[]).length>0;
    }catch(e){return false;}
  }
  function mappingReady(kind){
    try{
      const ids=window.RECON_SERVER_DATASET_IDS||{};
      const has=kind==='ceft' ? (ids.cr||[]).length>0&&(ids.dr||[]).length>0 : (ids.cr||[]).length>0&&(ids.bkcredit||[]).length>0;
      if(has)return true;
    }catch(e){}
    const page=document.getElementById('pg-upload');
    if(!page)return false;
    const cr=page.querySelector('#up-cr-filelist')?.children.length>0;
    const dr=page.querySelector('#up-dr-filelist')?.children.length>0;
    const bk=page.querySelector('#up-bkc-filelist')?.children.length>0;
    return kind==='ceft'?cr&&dr:cr&&bk;
  }
  function invoke(kind,stage){
    const fn=window.twoPipeRunStage;
    if(typeof fn==='function'){return fn(kind,stage);}
    const legacy=window.runTwoPipeline;
    if(stage===1&&typeof legacy==='function')return legacy(kind);
    alert('The 4-stage pipeline engine is not available in this build. Please rebuild from the current CEFT Recon source.');
  }
  function update(kind){
    const s=sides[kind], ready=mappingReady(kind), has=sideHasData(kind), cfg=state(kind);
    const badge=document.getElementById('v4-map-'+kind), status=document.getElementById('v4-status-'+kind);
    if(badge){badge.textContent=ready?'✓ Mapping/data ready':'Map/upload required';badge.style.background=ready?'#D1FAE5':'#FFF3CD';badge.style.color=ready?'#065F46':'#856404';}
    if(status){status.textContent=ready?'Ready — run Stage 1 Index. Stages unlock sequentially after each checkpoint.':'Upload the required files and complete column mapping. The controls remain visible so they cannot disappear.';}
    for(let i=1;i<=4;i++){
      const b=document.getElementById('v4-'+kind+'-'+i); if(!b)continue;
      let enabled=false;
      if(i===1) enabled=ready;
      else if(cfg){
        if(i===2) enabled=!!cfg.reconcileId;
        else if(i===3) enabled=!!cfg.reconcileId;
        else if(i===4) enabled=!!cfg.reconcileId;
      }
      b.disabled=!enabled;
    }
    const p=document.getElementById('v4-progress-'+kind); if(p&&ready)p.style.display='block';
  }
  function ensure(){
    const h=document.getElementById('pg-upload');
    if(!h)return;
    let root=document.getElementById(ROOT_ID);
    if(!root){
      root=document.createElement('div');root.id=ROOT_ID;
      root.innerHTML='<div class="v4-head"><span class="v4-title">🧩 4-STAGE RECONCILIATION — ALWAYS VISIBLE</span><span class="v4-note">Index → Match → Duplicate Check → Apply Results. Controls stay visible even when mapping is incomplete.</span></div><div class="v4-grid">'+Object.entries(sides).map(([k,s])=>'<div class="v4-card '+(k==='banker'?'bank':'')+'"><div class="v4-head"><span class="v4-name" style="color:'+(k==='banker'?'#1B5E20':'#1565C0')+'">'+s.name+'</span><span id="v4-map-'+k+'" class="v4-map v4-ready">Map/upload required</span></div><div class="v4-buttons">'+[1,2,3,4].map(i=>'<button id="v4-'+k+'-'+i+'" class="v4-btn v4-b'+i+'" onclick="window.__ceftVisible4Stage(\''+k+'\','+i+')">'+(['📋 1. Index','🧠 2. Match','🔍 3. Duplicate Check','✅ 4. Apply Results'][i-1])+'</button>').join('')+'</div><div id="v4-progress-'+k+'" class="v4-progress"><div id="v4-fill-'+k+'" class="v4-fill"></div></div><div id="v4-status-'+k+'" class="v4-status"></div></div>').join('')+'</div>';
      const anchor=h.querySelector('#up-mapping-wrap')||h.querySelector('#recon-memory-guard');
      if(anchor)anchor.parentNode.insertBefore(root,anchor);else h.appendChild(root);
    }
    update('ceft');update('banker');
  }
  window.__ceftVisible4Stage=function(kind,stage){
    const b=document.getElementById('v4-'+kind+'-'+stage); if(b&&b.disabled)return;
    const status=document.getElementById('v4-status-'+kind); if(status)status.textContent='Running Stage '+stage+'…';
    const r=invoke(kind,stage);
    if(r&&typeof r.then==='function')r.then(()=>update(kind)).catch(e=>{if(status)status.textContent='✗ '+(e?.message||e);update(kind);});
    else update(kind);
  };
  window.__ceftVisible4Refresh=ensure;
  let last=0;
  function tick(){const h=document.getElementById('pg-upload');if(!h)return;const n=Date.now();if(n-last>300){last=n;ensure();}}
  new MutationObserver(tick).observe(document.documentElement,{childList:true,subtree:true,attributes:true});
  setInterval(tick,700);
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',tick);else tick();
})();
</script>
'@

$close = '</body>'
$idx = $html.LastIndexOf($close, [System.StringComparison]::OrdinalIgnoreCase)
if ($idx -lt 0) { throw 'Could not find closing </body> tag in index.html' }
$html = $html.Substring(0,$idx) + $patch + "`r`n" + $html.Substring($idx)
Set-Content -Path $path -Value $html -Encoding UTF8
Write-Host 'Applied robust visible 4-stage reconciliation UI patch.'
