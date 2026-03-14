:root{--bg:#0f0f0f;--surface:#1a1a1a;--surf2:#222;--surf3:#2a2a2a;--border:#2e2e2e;--accent:#f5a623;--green:#4ade80;--red:#f87171;--text:#f0f0f0;--muted:#666;--mono:'DM Mono',monospace;--sans:'DM Sans',sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:0 24px;height:58px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.logo{display:flex;align-items:center;gap:10px}
.logo-icon{width:34px;height:34px;border-radius:9px;background:linear-gradient(135deg,#f5a623,#e8965a);display:flex;align-items:center;justify-content:center;font-size:18px}
.logo-text{font-weight:800;font-size:16px}
.shop-badge{font-family:var(--mono);font-size:12px;padding:4px 10px;border-radius:6px;background:rgba(245,166,35,.1);color:var(--accent);border:1px solid rgba(245,166,35,.2)}
.tabs{display:flex;gap:2px;padding:0 24px;background:var(--surface);border-bottom:1px solid var(--border)}
.tab{padding:12px 20px;font-size:13px;font-weight:600;color:var(--muted);border:none;background:none;cursor:pointer;border-bottom:2px solid transparent;transition:all .2s;font-family:var(--sans)}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.tab:hover:not(.active){color:var(--text)}
.badge{display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:10px;background:var(--accent);color:#000;font-size:11px;font-weight:700;margin-left:6px}
main{padding:20px 24px;max-width:1080px;margin:0 auto}
.config-panel{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:32px;max-width:500px;margin:40px auto}
.config-panel h2{font-size:20px;font-weight:800;margin-bottom:6px}
.config-panel p{font-size:13px;color:var(--muted);margin-bottom:24px}
.field{margin-bottom:14px}
.field label{display:block;font-size:11px;color:var(--muted);font-weight:700;margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px}
.field input{width:100%;padding:11px 14px;border-radius:10px;border:1px solid var(--border);background:var(--surf2);color:var(--text);font-size:14px;font-family:var(--mono);outline:none;transition:border-color .2s}
.field input:focus{border-color:var(--accent)}
.hint{font-size:11px;color:var(--muted);margin-top:5px;line-height:1.5}
.hint code{color:var(--accent);font-family:var(--mono)}
.btn{padding:10px 20px;border-radius:10px;border:none;font-size:14px;font-weight:700;cursor:pointer;transition:all .15s;font-family:var(--sans)}
.btn-primary{background:linear-gradient(135deg,#f5a623,#e8965a);color:#000}
.btn-primary:hover{opacity:.9;transform:translateY(-1px)}
.btn-ghost{background:transparent;color:var(--muted);border:1px solid var(--border)}
.btn-ghost:hover{color:var(--text);border-color:#555}
.btn-green{background:rgba(74,222,128,.12);color:var(--green);border:1px solid rgba(74,222,128,.25)}
.btn-green:hover{background:rgba(74,222,128,.22)}
.btn-sm{padding:7px 14px;font-size:12px;border-radius:8px}
.refresh-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
.refresh-row h2{font-size:17px;font-weight:700}
.last-refresh{font-size:12px;color:var(--muted);font-family:var(--mono)}
.filter-row{display:flex;gap:8px;margin-bottom:14px;align-items:center}
.search-wrap{flex:1;display:flex;align-items:center;gap:8px;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:9px 14px;transition:border-color .2s}
.search-wrap:focus-within{border-color:var(--accent)}
.search-wrap input{flex:1;background:none;border:none;outline:none;color:var(--text);font-size:14px;font-family:var(--sans)}
.search-wrap input::placeholder{color:var(--muted)}
.s-ico{font-size:14px;color:var(--muted)}
.clear-btn{background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:0 4px;line-height:1;transition:color .2s}
.clear-btn:hover{color:var(--text)}
.flt-btn{display:flex;align-items:center;gap:6px;padding:9px 14px;border-radius:10px;border:1px solid var(--border);background:var(--surface);color:var(--muted);font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap;transition:all .2s;font-family:var(--sans)}
.flt-btn:hover{border-color:#444;color:var(--text)}
.flt-btn.on{background:rgba(245,166,35,.1);border-color:rgba(245,166,35,.35);color:var(--accent)}
.sort-wrap{position:relative}
.sort-drop{position:absolute;top:calc(100% + 8px);right:0;background:#181818;border:1px solid #333;border-radius:12px;padding:6px;min-width:210px;box-shadow:0 16px 48px rgba(0,0,0,.7);z-index:300;animation:calDrop .15s ease}
.sort-opt{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:8px;cursor:pointer;font-size:13px;color:var(--muted);transition:all .15s;border:none;background:none;width:100%;font-family:var(--sans);text-align:left}
.sort-opt:hover{background:var(--surf2);color:var(--text)}
.sort-opt.active{color:var(--accent);background:rgba(245,166,35,.08)}
.sort-opt .si{font-size:14px;width:20px;text-align:center}
.sort-opt .sd{margin-left:auto;font-size:11px;color:#555}
.sort-opt.active .sd{color:var(--accent)}
.stock-wrap{position:relative}
.stock-drop{position:absolute;top:calc(100% + 8px);right:0;background:#181818;border:1px solid #333;border-radius:16px;padding:18px;width:360px;box-shadow:0 20px 60px rgba(0,0,0,.8);z-index:300;animation:calDrop .15s ease}
.stock-suggestions{display:flex;flex-direction:column;gap:2px;max-height:220px;overflow-y:auto;margin-bottom:6px}
.stock-sugg{display:flex;align-items:center;justify-content:space-between;padding:8px 10px;border-radius:8px;cursor:pointer;font-size:13px;color:var(--text);transition:background .12s;border:none;background:none;width:100%;font-family:var(--sans);text-align:left}
.stock-sugg:hover{background:var(--surf2)}
.stock-sugg.selected{color:var(--red);background:rgba(248,113,113,.08)}
.stock-divider{font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;padding:6px 4px 4px}
.stock-tag{display:flex;align-items:center;gap:5px;padding:4px 10px;border-radius:20px;background:rgba(248,113,113,.12);border:1px solid rgba(248,113,113,.25);color:var(--red);font-size:12px;font-weight:600;cursor:pointer;transition:all .15s}
.stock-tag:hover{background:rgba(248,113,113,.22)}
.cal-wrap{position:relative}
.cal-drop{position:absolute;top:calc(100% + 8px);right:0;background:#181818;border:1px solid #333;border-radius:16px;padding:18px;width:286px;box-shadow:0 20px 60px rgba(0,0,0,.75);z-index:300;animation:calDrop .15s ease}
@keyframes calDrop{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:translateY(0)}}
.cal-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
.cal-head-title{font-size:14px;font-weight:700}
.cal-nav{background:var(--surf2);border:1px solid var(--border);color:var(--text);width:28px;height:28px;border-radius:7px;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:14px;transition:background .15s;flex-shrink:0;font-family:var(--sans)}
.cal-nav:hover{background:var(--surf3)}
.cal-wdays{display:grid;grid-template-columns:repeat(7,1fr);margin-bottom:6px}
.cal-wdays span{text-align:center;font-size:11px;font-weight:700;color:var(--muted);padding:3px 0}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:3px}
.cd{aspect-ratio:1;display:flex;align-items:center;justify-content:center;border-radius:8px;font-size:13px;cursor:pointer;border:none;background:none;color:var(--text);transition:background .12s;font-family:var(--sans);position:relative}
.cd:hover:not(.cd-empty){background:var(--surf2)}
.cd.cd-empty{cursor:default;pointer-events:none}
.cd.cd-other{color:#3a3a3a}
.cd.cd-today{color:var(--accent);font-weight:700}
.cd.cd-sel{background:var(--accent)!important;color:#000!important;font-weight:700}
.cd-dot{position:absolute;bottom:3px;left:50%;transform:translateX(-50%);width:4px;height:4px;border-radius:50%;background:var(--accent)}
.cd.cd-sel .cd-dot{background:#000}
.cal-footer{display:flex;align-items:center;justify-content:space-between;margin-top:12px;padding-top:12px;border-top:1px solid var(--border)}
.cal-footer span{font-size:12px;color:var(--muted)}
.cal-reset{background:none;border:none;color:var(--muted);font-size:12px;cursor:pointer;font-family:var(--sans);padding:4px 8px;border-radius:6px;transition:all .15s}
.cal-reset:hover{color:var(--text);background:var(--surf2)}
.orders-grid{display:flex;flex-direction:column;gap:10px}
.order-card{background:var(--surface);border:1px solid var(--border);border-radius:13px;overflow:hidden;transition:border-color .2s,box-shadow .2s;animation:slideIn .25s ease}
@keyframes slideIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
.order-card:hover{border-color:#363636;box-shadow:0 4px 20px rgba(0,0,0,.3)}
.order-head{display:flex;align-items:center;gap:14px;padding:15px 18px;cursor:pointer;user-select:none}
.order-num{font-family:var(--mono);font-size:14px;font-weight:500;color:var(--accent);min-width:70px}
.order-meta{flex:1}
.order-customer{font-size:14px;font-weight:600}
.order-info{font-size:12px;color:var(--muted);margin-top:2px}
.order-count{font-size:13px;font-weight:700;padding:5px 11px;border-radius:7px;background:rgba(245,166,35,.1);color:var(--accent);white-space:nowrap}
.order-flag{font-size:20px;min-width:26px;text-align:center}
.note-dot{width:8px;height:8px;border-radius:50%;background:var(--accent);flex-shrink:0;box-shadow:0 0 7px rgba(245,166,35,.7)}
.chevron{color:var(--muted);transition:transform .2s;font-size:11px;margin-left:2px}
.order-body{padding:0 18px 18px;border-top:1px solid var(--border)}
.items-list{margin-top:14px;display:flex;flex-direction:column;gap:8px}
.item-row{display:flex;align-items:center;gap:14px;padding:10px 14px;border-radius:10px;background:var(--surf2)}
.item-img{width:52px;height:52px;border-radius:8px;object-fit:cover;flex-shrink:0;background:var(--surf3)}
.item-img-placeholder{width:52px;height:52px;border-radius:8px;flex-shrink:0;background:var(--surf3);display:flex;align-items:center;justify-content:center;font-size:22px}
.item-qty-big{min-width:44px;height:44px;border-radius:10px;background:rgba(245,166,35,.15);border:2px solid rgba(245,166,35,.3);color:var(--accent);font-family:var(--mono);font-size:20px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.item-name{font-size:14px;font-weight:700;line-height:1.3}
.item-variant{font-size:12px;color:var(--muted);margin-top:3px}
.item-sku{font-family:var(--mono);font-size:11px;color:var(--muted);margin-left:auto}
.box-row{flex-direction:column;align-items:stretch;gap:10px}
.box-header{display:flex;align-items:center;gap:12px}
.box-qty{background:rgba(167,139,250,.2);color:#a78bfa;border-color:rgba(167,139,250,.3)}
.box-label{font-size:12px;color:#a78bfa;margin-top:2px}
.box-items{display:grid;grid-template-columns:1fr 1fr;gap:6px}
.box-item{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;background:rgba(255,255,255,.04)}
.box-item-img{width:36px;height:36px;border-radius:6px;object-fit:cover;flex-shrink:0;background:var(--surf3)}
.box-item-img-ph{width:36px;height:36px;border-radius:6px;flex-shrink:0;background:var(--surf3);display:flex;align-items:center;justify-content:center;font-size:16px}
.box-item-qty{min-width:26px;height:26px;border-radius:6px;background:rgba(245,166,35,.15);color:var(--accent);font-size:13px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-family:var(--mono)}
.box-item-name{font-size:12px;font-weight:600;line-height:1.3;color:var(--text)}
.order-note{margin-top:10px;padding:10px 14px;border-radius:8px;background:rgba(245,166,35,.06);border-left:3px solid var(--accent);font-size:13px;color:#ccc;line-height:1.5}
.order-actions{display:flex;justify-content:flex-end;margin-top:14px}
.pack-mode{margin-top:14px}
.pack-progress{display:flex;align-items:center;gap:10px;margin-bottom:12px}
.pack-bar-bg{flex:1;height:6px;background:var(--surf3);border-radius:3px;overflow:hidden}
.pack-bar-fill{height:100%;background:linear-gradient(90deg,var(--accent),var(--green));border-radius:3px;transition:width .3s ease}
.pack-bar-txt{font-size:12px;font-weight:700;color:var(--muted);white-space:nowrap;font-family:var(--mono)}
.pack-item{display:flex;align-items:center;gap:14px;padding:12px 16px;border-radius:11px;background:var(--surf2);margin-bottom:8px;transition:all .3s ease;cursor:pointer}
.pack-item.done{opacity:.35;background:rgba(74,222,128,.06);border:1px solid rgba(74,222,128,.15)}
.pack-item.done .pack-item-name{text-decoration:line-through;color:var(--muted)}
.pack-check{width:32px;height:32px;border-radius:8px;flex-shrink:0;border:2px solid var(--border);background:none;display:flex;align-items:center;justify-content:center;font-size:16px;transition:all .2s;cursor:pointer}
.pack-item.done .pack-check{background:var(--green);border-color:var(--green);color:#000}
.pack-item-img{width:44px;height:44px;border-radius:7px;object-fit:cover;flex-shrink:0;background:var(--surf3)}
.pack-item-img-ph{width:44px;height:44px;border-radius:7px;flex-shrink:0;background:var(--surf3);display:flex;align-items:center;justify-content:center;font-size:20px}
.pack-item-qty{min-width:38px;height:38px;border-radius:9px;background:rgba(245,166,35,.15);border:2px solid rgba(245,166,35,.25);color:var(--accent);font-family:var(--mono);font-size:18px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.pack-item-name{font-size:14px;font-weight:700;flex:1}
.pack-item-var{font-size:12px;color:var(--muted);margin-top:2px}
.pack-footer{display:flex;gap:8px;margin-top:14px;padding-top:14px;border-top:1px solid var(--border)}
.archive-list{display:flex;flex-direction:column;gap:8px}
.arc-row{display:flex;align-items:center;justify-content:space-between;padding:13px 18px;border-radius:11px;background:var(--surface);border:1px solid var(--border);animation:slideIn .2s ease}
.arc-left{display:flex;align-items:center;gap:12px}
.arc-num{font-family:var(--mono);font-size:14px;color:var(--muted)}
.arc-time{font-size:12px;color:var(--muted)}
.done-badge{padding:3px 9px;border-radius:5px;font-size:11px;font-weight:700;background:rgba(74,222,128,.1);color:var(--green);border:1px solid rgba(74,222,128,.2)}
.img-upload-area{border:2px dashed var(--border);border-radius:14px;padding:36px 20px;text-align:center;cursor:pointer;transition:all .2s;margin-bottom:16px}
.img-upload-area:hover,.img-upload-area.drag{border-color:var(--accent);background:rgba(245,166,35,.04)}
.img-upload-icon{font-size:40px;margin-bottom:10px}
.img-upload-title{font-size:15px;font-weight:700;margin-bottom:6px}
.img-upload-sub{font-size:12px;color:var(--muted)}
.img-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px}
.img-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;transition:border-color .2s}
.img-card:hover{border-color:#444}
.img-card img{width:100%;height:120px;object-fit:cover;display:block}
.img-card-body{padding:10px 12px}
.img-card-name{font-size:13px;font-weight:700;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.img-card-actions{display:flex;gap:6px;margin-top:8px}
.img-card-del{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--red);font-size:11px;padding:5px 10px;border-radius:6px;cursor:pointer;flex:1;font-family:var(--sans);transition:background .15s}
.img-card-del:hover{background:rgba(248,113,113,.2)}
.img-card-edit{background:rgba(245,166,35,.08);border:1px solid rgba(245,166,35,.2);color:var(--accent);font-size:11px;padding:5px 10px;border-radius:6px;cursor:pointer;flex:1;font-family:var(--sans);transition:background .15s}
.img-card-edit:hover{background:rgba(245,166,35,.18)}
.prod-chip-list{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px;max-height:160px;overflow-y:auto;padding:2px}
.prod-chip{padding:5px 12px;border-radius:20px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid var(--border);background:var(--surf2);color:var(--muted);transition:all .15s;user-select:none}
.prod-chip:hover{border-color:#555;color:var(--text)}
.prod-chip.sel{background:rgba(245,166,35,.15);border-color:rgba(245,166,35,.4);color:var(--accent)}
.prod-chip-search{display:flex;align-items:center;gap:8px;background:var(--surf2);border:1px solid var(--border);border-radius:8px;padding:7px 10px;margin-bottom:8px}
.prod-chip-search input{flex:1;background:none;border:none;outline:none;color:var(--text);font-size:13px;font-family:var(--sans)}
.prod-chip-search input::placeholder{color:var(--muted)}
.sel-summary{font-size:12px;color:var(--accent);margin-top:6px;min-height:18px}
.empty{text-align:center;padding:70px 20px;color:var(--muted)}
.empty-icon{font-size:44px;margin-bottom:14px}
.empty h3{font-size:17px;font-weight:700;color:#555;margin-bottom:6px}
.loading{text-align:center;padding:60px}
.spinner{width:34px;height:34px;border-radius:50%;margin:0 auto 14px;border:3px solid rgba(245,166,35,.15);border-top-color:var(--accent);animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.error-bar{padding:11px 15px;border-radius:9px;margin-bottom:14px;background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--red);font-size:13px}
@keyframes fadeOut{to{opacity:0;transform:translateX(30px);max-height:0;padding:0;margin:0}}
.fulfilling{animation:fadeOut .38s ease forwards;pointer-events:none;overflow:hidden}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
