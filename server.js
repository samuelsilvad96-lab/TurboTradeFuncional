'use strict';
/* TurboTrade AI · Sinais na nuvem
   Roda 24h num servidor: conecta na Deriv, gera sinais e opera (loss virtual, ciclo, gale, martingale).
   Variáveis de ambiente: DERIV_TOKEN, DERIV_APP_ID, PAINEL_SENHA, ACCOUNT_TYPE (demo|real), CONFIRM_REAL=SIM (só p/ real),
   AUTO_ON_START=1 (opcional), DATA_DIR (opcional), PORT (automático). */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const TOKEN = (process.env.DERIV_TOKEN || '').trim();
const APP_ID = (process.env.DERIV_APP_ID || '').trim();
const ACC_TYPE = (process.env.ACCOUNT_TYPE || 'demo').trim().toLowerCase();
const SENHA = process.env.PAINEL_SENHA || '';
const API_BASE = process.env.DERIV_API_BASE || 'https://api.derivws.com';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const AUTO_ON_START = process.env.AUTO_ON_START === '1';
const log = (...a) => console.log(new Date().toISOString(), ...a);

if (!SENHA || SENHA.length < 6) { console.error('ERRO: defina PAINEL_SENHA (mínimo 6 caracteres) nas variáveis de ambiente.'); process.exit(1); }
if (ACC_TYPE !== 'demo' && ACC_TYPE !== 'real') { console.error('ERRO: ACCOUNT_TYPE deve ser demo ou real.'); process.exit(1); }

const SYMBOLS = [
  { symbol: 'R_10', name: 'Volatility 10 Index' }, { symbol: 'R_25', name: 'Volatility 25 Index' },
  { symbol: 'R_50', name: 'Volatility 50 Index' }, { symbol: 'R_75', name: 'Volatility 75 Index' },
  { symbol: 'R_100', name: 'Volatility 100 Index' }, { symbol: '1HZ10V', name: 'Volatility 10 (1s)' },
  { symbol: '1HZ25V', name: 'Volatility 25 (1s)' }, { symbol: '1HZ50V', name: 'Volatility 50 (1s)' },
  { symbol: '1HZ75V', name: 'Volatility 75 (1s)' }, { symbol: '1HZ100V', name: 'Volatility 100 (1s)' },
];

/* ---------- estado e persistência ---------- */
const sgDef = { type: 'DIGIT', prof: 'aggressive', base: .35, stake: .35, mart: 2.1, stop: 250, autoMul: true, auto: false, vOn: false, vN: 2, gOn: true, gN: 2, syms: {} };
const STATE_FILE = path.join(DATA_DIR, 'state.json');
let saved = {};
try { saved = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch (e) {}
let accType = (saved.acct === 'real' || saved.acct === 'demo') ? saved.acct : ACC_TYPE;   // conta ativa (trocável no painel)
const sg = Object.assign({}, sgDef, saved.cfg || {});
sg.auto = AUTO_ON_START ? !!sg.auto : false;           // por segurança, reinício do servidor não religa o Auto-Trade
sg.pl = Number(saved.pl) || 0; sg.h = Array.isArray(saved.h) ? saved.h.slice(0, 30) : [];
sg.q = []; sg.uid = 0; sg.sigs = []; sg.d = {}; sg.p = {}; sg.key = null; sg.to = null;
sg.vc = 0; sg.vReal = false; sg.vp = []; sg.cycStop = false; sg.cycL = 0; sg.msg = { t: '', ts: 0 };
if (!Object.values(sg.syms).some(Boolean)) SYMBOLS.forEach(s => sg.syms[s.symbol] = true);
let sgPending = false, lastTrade = null;
const sgR = n => Math.round(n * 100) / 100;
let saveT = null;
function sgSave() {
  clearTimeout(saveT);
  saveT = setTimeout(() => {
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      const cfg = {}; Object.keys(sgDef).forEach(k => cfg[k] = sg[k]);
      fs.writeFileSync(STATE_FILE, JSON.stringify({ cfg, pl: sg.pl, h: sg.h, acct: accType }));
    } catch (e) { log('Falha ao salvar estado:', e.message); }
  }, 300);
}
function sgToast(m) { sg.msg = { t: m, ts: Date.now() }; log('[sinais]', m); }

/* ---------- motor de sinais (idêntico ao do site) ---------- */
const sgP={max:{min:.9,n:3,mp:1.5,tiers:['green']},aggressive:{min:.5,n:8,mp:.7,tiers:['white','blue','green','gold']},balanced:{min:.7,n:5,mp:.9,tiers:['green','gold']},safe:{min:.85,n:3,mp:1.5,tiers:['green']}};
const sgTC={white:'#e5e7eb',blue:'#3b82f6',gold:'#f5b301',green:'#22c55e'};
/* ===== Motor de dígitos (portado do Turbo Signals) — dígitos: mais recente primeiro ===== */
const sgW={white:.2,blue:.4,green:.6,gold:.8,red:1},sgGI={red:'green',gold:'gold',green:'blue',blue:'white',white:'white'},sgPI={white:.4,blue:.6,gold:.8,green:1};
const sgRT={O:{1:1.19,2:1.35,3:1.56,4:1.85,5:2.26,6:2.92,7:4.13,8:7.04},U:{1:7.04,2:4.13,3:2.92,4:2.26,5:1.85,6:1.56,7:1.35,8:1.19}};
// [tipo, pagamento, extra, [limiar,nível] agressivo, equilibrado, seguro]
const sgPat=[
 ['par',1.85,{},[8,'green'],[12,'red'],[16,'red']],
 ['rng',1.85,{u:5,o:4},[10,'green'],[14,'red'],[18,'red']],['rng',1.85,{u:7,o:3},[6,'green'],[8,'red'],[10,'red']],['rng',1.85,{u:8,o:2},[4,'green'],[6,'red'],[8,'red']],
 ['rng',1.35,{u:7,o:2},[5,'blue'],[7,'green'],[9,'red']],['rng',1.19,{u:8,o:1},[4,'white'],[6,'blue'],[8,'green']],
 ['int',1.85,{},[6,'green'],[8,'red'],[10,'red']],['fpar',1.85,{lb:100},[52,'green'],[54,'red'],[56,'red']],
 ['same',1.06,{},[2,'green'],[3,'red'],[4,'red']],['rise',1.85,{},[5,'green'],[6,'red'],[7,'red']],['gap',1.85,{g:6},[4,'green'],[5,'red'],[6,'red']],
 ['rf',1.85,{u:5,o:4,lb:100},[52,'green'],[55,'red'],[58,'red']],['rf',1.35,{u:7,o:2,lb:100},[52,'blue'],[55,'green'],[58,'red']],['rf',1.19,{u:8,o:1,lb:100},[52,'white'],[55,'blue'],[58,'green']],
 ['rep',1.06,{lb:10},[5,'green'],[6,'red'],[7,'red']],['clu',1.85,{n:20},[12,'green'],[14,'red'],[16,'red']],['osc',1.85,{},[6,'green'],[8,'red'],[8,'red']]];
sgPat[sgPat.length-1][4]=[7,'red'];
function sgEval(d,pt,th,tier){
 const [k,pay,x]=pt,w=sgW[tier],n=d.length,mn=Math.min,R=(rec,conf,pred)=>({rec,conf:Number(mn(conf,1).toFixed(2)),pred,pay,tier});
 const ev=v=>v%2===0;let u,h,a,b;
 switch(k){
 case'par':if(n<th)return;u=d.slice(0,th);if(u.every(ev))return R('DIGITODD',.5+th/20+.2*w);if(u.every(v=>!ev(v)))return R('DIGITEVEN',.5+th/20+.2*w);return;
 case'rng':{if(n<th)return;u=d.slice(0,th);const lo=u.every(v=>v<5),hi=u.every(v=>v>4);if(!lo&&!hi)return;const rec=lo?'DIGITOVER':'DIGITUNDER',p=lo?x.o:x.u;
  return Object.assign(R(rec,.5+th/20+.2*w,p),{pay:(rec==='DIGITOVER'?sgRT.O:sgRT.U)[p]||pay})}
 case'int':{if(n<th)return;u=d.slice(0,th);a=u.every((v,i)=>(i%2===0)===ev(v));b=u.every((v,i)=>(i%2===0)!==ev(v));if(!a&&!b)return;return R(a?'DIGITODD':'DIGITEVEN',.5+th/12+.2*w)}
 case'fpar':{if(n<30)return;u=d.slice(0,mn(n,x.lb));const e=u.filter(ev).length,o=u.length-e;if(e/u.length*100>th)return R('DIGITODD',e/u.length+.2*w);if(o/u.length*100>th)return R('DIGITEVEN',o/u.length+.2*w);return}
 case'same':{if(n<th)return;u=d.slice(0,th);if(!u.every(v=>v===u[0]))return;return R('DIGITDIFF',.5+.1*th+.2*w,u[0])}
 case'rise':{if(n<th+1)return;u=d.slice(0,th+1);a=u.every((v,i)=>i===u.length-1||v>u[i+1]);b=u.every((v,i)=>i===u.length-1||v<u[i+1]);if(!a&&!b)return;return R(a?'DIGITEVEN':'DIGITODD',.5+th/8+.2*w)}
 case'gap':{if(n<th)return;u=d.slice(0,th);let c=0,s=0;for(let i=0;i<u.length-1;i++){const g=Math.abs(u[i]-u[i+1]);s+=g;if(g>=x.g)c++}if(c<th-1)return;return R('DIGITEVEN',.5+(s/(u.length-1))/10+.2*w)}
 case'rf':{if(n<30)return;u=d.slice(0,mn(n,x.lb));a=u.filter(v=>v<5).length/u.length*100;b=u.filter(v=>v>4).length/u.length*100;
  if(a>th)return Object.assign(R('DIGITOVER',.5+(a-50)/100+.2*w,x.o),{pay});if(b>th)return Object.assign(R('DIGITUNDER',.5+(b-50)/100+.2*w,x.u),{pay});return}
 case'rep':{if(n<x.lb)return;u=d.slice(0,x.lb);const c={};u.forEach(v=>c[v]=(c[v]||0)+1);let p=null,g=0;for(const[v,q]of Object.entries(c))if(q>=th&&q>g){p=+v;g=q}if(p===null)return;return R('DIGITDIFF',.5+g/x.lb*.3+.2*w,p)}
 case'clu':{if(n<x.n)return;u=d.slice(0,x.n);const e=u.filter(ev).length,o=x.n-e;if(e>=th)return R('DIGITODD',.5+(e/x.n*100-50)/100+.2*w);if(o>=th)return R('DIGITEVEN',.5+(o/x.n*100-50)/100+.2*w);return}
 case'osc':{if(n<th)return;u=d.slice(0,th);let m=0;for(let i=0;i<u.length-1;i++)if((u[i]>=5)!==(u[i+1]>=5))m++;if(m<th-1)return;const hi=u[0]>=5;return R(hi?'DIGITUNDER':'DIGITOVER',.5+m/th*.3+.2*w,hi?5:4)}}}
function sgLabel(s){return{DIGITEVEN:'Par',DIGITODD:'Ímpar',DIGITOVER:'Acima '+s.barrier,DIGITUNDER:'Abaixo '+s.barrier,DIGITDIFF:'Diferente de '+s.barrier,CALL:'Sobe',PUT:'Desce'}[s.type]}
function sgDigitSignals(sym,name){
 const d=(sg.d[sym]||[]).slice(0,100);if(d.length<10)return[];const out=[];
 sgPat.forEach(pt=>[3,4,5].forEach(ix=>{const[th,tier]=pt[ix],r=sgEval(d,pt,th,tier);if(r)out.push(r)}));
 out.sort((a,b)=>b.conf*sgPI[sgGI[b.tier]]-a.conf*sgPI[sgGI[a.tier]]);
 return out.slice(0,3).map(r=>({sym,name,type:r.rec,barrier:r.pred,conf:r.conf,acc:.75+.1*r.conf,payout:r.pay,tier:sgGI[r.tier],bought:false}))}
function sgFilter(all){
 const P=sgP[sg.prof],by={};
 all.filter(s=>P.tiers.includes(s.tier)&&s.payout>=P.mp&&s.conf>=P.min).forEach(s=>(by[s.sym]=by[s.sym]||[]).push(s));
 const ks=Object.keys(by);ks.forEach(k=>{by[k].sort((a,b)=>b.conf*b.acc*b.payout-a.conf*a.acc*a.payout);const seen={};by[k]=by[k].filter(s=>{const q=s.type+s.barrier;return seen[q]?false:(seen[q]=1)})});
 const out=[];for(let i=0;out.length<P.n;i++){let any=false;for(const k of ks){if(by[k][i]){out.push(by[k][i]);any=true;if(out.length>=P.n)break}}if(!any)break}
 return out}
/* Ticks: heurística própria (o motor de indicadores do site não foi portado) */
function sgTickSignals(sym,name){const p=sg.p[sym]||[];if(p.length<20)return[];const m=[];for(let i=1;i<p.length;i++)m.push(p[i]>p[i-1]?1:0);
 const sc=f=>{const a=m.slice(-20).filter(f).length/20,b=m.slice(-6).filter(f).length/6;return Math.min(.97,Math.max(.3,.5+((.6*a+.4*b)-.5)*2.2))};
 return[['CALL',1],['PUT',0]].map(([t,v])=>{const c=sc(x=>x===v);return{sym,name,type:t,barrier:null,conf:c,acc:.7,payout:1.85,tier:c>=.85?'green':c>=.72?'gold':c>=.6?'blue':'white',bought:false}})}

/* ---------- ciclo de sinais, loss virtual e operações ---------- */
function sgRound() {
  sg.cycStop = false; sg.cycL = 0;
  sgSub(); const all = [];
  SYMBOLS.forEach(S => { if (sg.syms[S.symbol]) { if (sg.type !== 'TICK') all.push(...sgDigitSignals(S.symbol, S.name)); if (sg.type !== 'DIGIT') all.push(...sgTickSignals(S.symbol, S.name)); } });
  sg.sigs = sgFilter(all).map(s => Object.assign(s, { label: sgLabel(s), id: ++sg.uid }));
  sg.gen = Date.now();
  setTimeout(() => { if (Date.now() - sg.gen >= 17900) sg.sigs = []; }, 18000);
  if (sg.auto) sgNext();
}
function sgLoop() {
  const now = Date.now(), key = Math.floor(now / 20000);
  if (key !== sg.key) { sg.key = key; sgRound(); }
}
function sgVCheck() { if (sg.vOn && !sg.vReal && sg.vc >= sg.vN) { sg.vReal = true; sgToast('Loss virtual ' + sg.vc + '/' + sg.vN + ' · próxima entrada REAL liberada'); } }
function sgVirt(s) {
  s.bought = true; s.v = true; s.st = 'v';
  const pp = sg.p[s.sym] || []; sg.vp.push({ sig: s, sym: s.sym, p0: pp.length ? pp[pp.length - 1] : null });
  return true;
}
function sgVRes(sym, digit, price) {
  if (!sg.vp.length) return;
  const rest = []; let any = false;
  sg.vp.forEach(v => {
    if (v.sym !== sym) { rest.push(v); return; }
    const s = v.sig, t = s.type, b = Number(s.barrier); let w;
    if (t === 'DIGITEVEN') w = digit % 2 === 0; else if (t === 'DIGITODD') w = digit % 2 === 1;
    else if (t === 'DIGITOVER') w = digit > b; else if (t === 'DIGITUNDER') w = digit < b; else if (t === 'DIGITDIFF') w = digit !== b;
    else if (t === 'CALL') w = v.p0 != null && price > v.p0; else if (t === 'PUT') w = v.p0 != null && price < v.p0; else w = false;
    s.st = w ? 'vwin' : 'vloss'; any = true;
    if (!sg.vReal) { if (w) sg.vc = 0; else { sg.vc++; sgVCheck(); } }
  });
  sg.vp = rest;
  if (any && sg.auto) setTimeout(sgNext, 150);
}
function sgTick(sym, digit, price) {
  if (!sg.syms[sym]) return;
  (sg.d[sym] = sg.d[sym] || []).unshift(digit); if (sg.d[sym].length > 500) sg.d[sym].pop();
  (sg.p[sym] = sg.p[sym] || []).push(price); if (sg.p[sym].length > 40) sg.p[sym].shift();
  sgVRes(sym, digit, price);
}
function sgBuy(s) {
  if (!s || s.bought) return false;
  if (sg.vOn && !sg.vReal) return sgVirt(s);
  if (!ws || ws.readyState !== 1 || !authorized) { sgToast('Sem conexão com a Deriv'); return false; }
  if (sg.pl <= -sg.stop) { sg.auto = false; sgSave(); sgToast('Stop Loss atingido'); return false; }
  if (sgPending) { s.bought = true; s.st = 'q'; sg.q.push(s); sgToast('Na fila: entra quando a operação atual terminar'); return true; }
  return sgGo(s);
}
function sgGo(s) {
  const stake = Math.max(.35, sgR(sg.stake)); s.bought = true;
  if (stake > balance) { s.st = 'err'; sgToast('Saldo insuficiente'); return false; }
  sgPending = true; lastTrade = { stake, sym: s.sym, sig: s }; s.st = 'buy';
  const req = { proposal: 1, amount: stake, basis: 'stake', contract_type: s.type, currency, duration: 1, duration_unit: 't', underlying_symbol: s.sym, passthrough: { sg: 1 } };
  if (s.barrier != null) req.barrier = String(s.barrier);
  try { ws.send(JSON.stringify(req)); } catch (e) { sgFail(s, 'Falha ao enviar a ordem'); return false; }
  clearTimeout(sg.to); sg.to = setTimeout(() => { if (sgPending && lastTrade && lastTrade.sig === s) sgFail(s, 'Sem resposta da Deriv'); }, 25000);
  return true;
}
function sgFail(s, msg) { sgPending = false; clearTimeout(sg.to); if (s) s.st = 'err'; sgToast(msg); sgDrain(); }
function sgDrain() { setTimeout(() => { if (sgPending) return; let s; while ((s = sg.q.shift())) { if (sg.sigs.includes(s)) { sgGo(s); return; } } if (sg.auto) sgNext(); }, 300); }
function sgManual(id) { sgBuy(sg.sigs.find(x => x.id === id)); }
function sgNext() {
  if (sg.cycStop) return;
  if (sg.vOn && !sg.vReal && sg.vp.length) return;
  const s = sg.sigs.find(x => !x.bought); if (s && sg.auto && !sgPending) sgBuy(s);
}
function sgOnResult(won, profit, c) {
  sgPending = false; clearTimeout(sg.to); profit = Number(profit) || 0; sg.pl = sgR(sg.pl + profit);
  const L = lastTrade || {}, s = L.sig;
  const paid = (c && Number(c.buy_price) > 0) ? Number(c.buy_price) : L.stake || sg.stake;
  if (s) { s.st = won ? 'win' : 'loss'; s.res = profit; }
  sg.h.unshift({ t: new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: process.env.TZ_PAINEL || 'America/Sao_Paulo' }), n: s ? s.name + ' · ' + s.label : (L.sym || ''), w: won, p: profit, k: paid });
  sg.h = sg.h.slice(0, 30);
  if (won) { sg.stake = sg.base; sg.vc = 0; sg.vReal = false; sg.cycStop = true; }
  else { if (sg.autoMul) sg.stake = sgR(paid * sg.mart); sg.cycL++; if (sg.gOn && sg.cycL > sg.gN) sg.cycStop = true; }
  sgToast((won ? 'WIN +' : 'LOSS ') + profit.toFixed(2));
  if (sg.pl <= -sg.stop) { sg.auto = false; sgToast('Stop Loss atingido · Auto-Trade desligado'); }
  sgSave(); sgDrain();
}

/* ---------- ações vindas do painel ---------- */
function sgIn(k, v) {
  v = parseFloat(String(v).replace(',', '.')); if (!isFinite(v)) return;
  sg[k] = k === 'base' ? Math.max(.35, sgR(v)) : k === 'mart' ? Math.max(1, sgR(v)) : k === 'vN' ? Math.max(1, Math.min(10, Math.round(v))) : k === 'gN' ? Math.max(0, Math.min(10, Math.round(v))) : Math.max(1, v);
  if (k === 'base') sg.stake = sg.base; if (k === 'vN') sgVCheck(); sgSave();
}
const NUM_KEYS = { base: 1, mart: 1, stop: 1, vN: 1, gN: 1 }, TOG_KEYS = { autoMul: 1, auto: 1, vOn: 1, gOn: 1 };
function doAction(b) {
  switch (b.a) {
    case 'set':
      if (b.k === 'type' && ['DIGIT', 'TICK', 'BOTH'].includes(b.v)) { sg.type = b.v; sg.sigs = []; sgSave(); }
      else if (b.k === 'prof' && sgP[b.v]) { sg.prof = b.v; sg.sigs = []; sgSave(); }
      break;
    case 'tog':
      if (!TOG_KEYS[b.k]) break;
      sg[b.k] = !sg[b.k]; if (b.k === 'vOn') { sg.vc = 0; sg.vReal = false; } sgSave();
      if (b.k === 'auto' && sg.auto) { if (sg.pl <= -sg.stop) { sg.auto = false; sgToast('Stop Loss atingido · limpe a sessão para religar'); } else sgNext(); }
      break;
    case 'in': if (NUM_KEYS[b.k]) sgIn(b.k, b.v); break;
    case 'step': if (NUM_KEYS[b.k] && isFinite(Number(b.d))) sgIn(b.k, (b.k === 'base' ? sg.base : sg[b.k]) + Number(b.d)); break;
    case 'reset': sg.stake = sg.base; sg.vc = 0; sg.vReal = false; sgSave(); break;
    case 'mul': sg.stake = sgR(sg.stake * sg.mart); sgSave(); break;
    case 'zero': sg.pl = 0; sg.h = []; sgSave(); sgToast('Sessão limpa: lucro e operações zerados'); break;
    case 'sym': if (SYMBOLS.some(s => s.symbol === b.s)) { sg.syms[b.s] = !sg.syms[b.s]; sgSave(); sgSub(); } break;
    case 'all': { const all = SYMBOLS.every(s => sg.syms[s.symbol]); SYMBOLS.forEach(s => sg.syms[s.symbol] = !all); sgSave(); sgSub(); break; }
    case 'manual': sgManual(Number(b.id)); break;
    case 'acct': {
      if (b.v !== 'demo' && b.v !== 'real') break;
      if (b.v === accType) break;
      if (b.v === 'real' && b.ok !== true) break;                       // real exige confirmação explícita do painel
      if (sgPending) { sgToast('Aguarde a operação atual terminar para trocar de conta'); break; }
      accType = b.v; sg.auto = false; sg.stake = sg.base; sg.pl = 0; sg.vc = 0; sg.vReal = false; sg.cycStop = false; sg.cycL = 0;
      sg.sigs = []; sg.q = []; sg.vp = []; sg.d = {}; sg.p = {};
      acct = null; authorized = false; balance = 0; connErr = ''; reconnectTries = 0;
      sgSave(); sgToast('Trocando para a conta ' + accType.toUpperCase() + ' · Auto-Trade desligado'); connect(); break;
    }
  }
}
function snapshot() {
  const now = Date.now(), cfg = {}; Object.keys(sgDef).forEach(k => cfg[k] = sg[k]);
  return {
    now, cfg, remain: 20 - Math.floor((now % 20000) / 1000), pl: sg.pl, h: sg.h, symbols: SYMBOLS,
    vc: sg.vc, vReal: sg.vReal, cycStop: sg.cycStop, cycL: sg.cycL, pending: sgPending,
    msg: sg.msg, sigs: sg.sigs.map(s => ({ id: s.id, name: s.name, label: s.label, payout: s.payout, tier: s.tier, st: s.st || '', res: s.res, bought: !!s.bought, v: !!s.v })),
    conn: { ok: authorized, err: connErr, balance, currency, acc: acct ? { type: acct.account_type, id: acct.account_id } : null, want: accType }
  };
}

/* ---------- conexão com a Deriv (REST -> OTP -> WebSocket) ---------- */
let ws = null, authorized = false, balance = 0, currency = 'USD', acct = null, connectId = 0, reconnectTries = 0, reconnectTimer = null, connErr = '';
function scheduleReconnect() { clearTimeout(reconnectTimer); const d = Math.min(30000, 3000 * (++reconnectTries)); reconnectTimer = setTimeout(connect, d); }
async function connect() {
  if (!TOKEN || !APP_ID) { connErr = 'Defina DERIV_TOKEN e DERIV_APP_ID'; return; }
  const my = ++connectId; clearTimeout(reconnectTimer);
  if (ws) { ws.__manual = true; try { ws.close(); } catch (e) {} }
  authorized = false;
  const headers = { Authorization: 'Bearer ' + TOKEN, 'Deriv-App-ID': APP_ID };
  try {
    const r = await fetch(API_BASE + '/trading/v1/options/accounts', { headers });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error((j.errors && j.errors[0] && j.errors[0].message) || ('Erro ' + r.status + ' ao listar contas'));
    const list = Array.isArray(j.data) ? j.data : (j.data ? [j.data] : []);
    const acc = list.find(a => a.account_type === accType && a.status !== 'inactive');
    if (!acc) throw new Error('Nenhuma conta ' + accType + ' ativa encontrada para este token');
    const o = await fetch(`${API_BASE}/trading/v1/options/accounts/${acc.account_id}/otp`, { method: 'POST', headers });
    const oj = await o.json().catch(() => ({}));
    if (!o.ok || !oj.data || !oj.data.url) throw new Error((oj.errors && oj.errors[0] && oj.errors[0].message) || ('Erro ' + o.status + ' ao obter OTP'));
    if (my !== connectId) return;
    openSocket(oj.data.url, acc);
  } catch (e) {
    if (my !== connectId) return;
    connErr = e.message; log('Falha ao conectar:', e.message); scheduleReconnect();
  }
}
function openSocket(url, acc) {
  const sock = ws = new WebSocket(url);
  sock.on('open', () => {
    authorized = true; reconnectTries = 0; connErr = ''; acct = acc; currency = acc.currency || 'USD'; balance = Number(acc.balance) || 0;
    sock.__subs = new Set(); sock.__hist = new Set(); sg.d = {}; sg.p = {};
    sock.__ping = setInterval(() => { if (sock.readyState === 1) sock.send(JSON.stringify({ ping: 1 })); }, 20000);
    log('Conectado na Deriv · conta', acc.account_type, acc.account_id);
    sock.send(JSON.stringify({ balance: 1, subscribe: 1 }));
    sgSub();
  });
  sock.on('message', d => { if (sock === ws) onMsg(sock, d.toString()); });
  sock.on('error', e => log('Erro no WebSocket:', e.message));
  sock.on('close', () => {
    clearInterval(sock.__ping);
    if (sock.__manual || ws !== sock) return;
    authorized = false; log('Conexão com a Deriv caiu; reconectando');
    if (sgPending) sgFail(lastTrade && lastTrade.sig, 'Conexão caiu durante a operação');
    scheduleReconnect();
  });
}
function sgSub() {
  if (!ws || ws.readyState !== 1 || !authorized) return;
  SYMBOLS.forEach(s => {
    if (!sg.syms[s.symbol]) return;
    if (!ws.__subs.has(s.symbol)) { ws.__subs.add(s.symbol); ws.send(JSON.stringify({ ticks: s.symbol, subscribe: 1 })); }
    if (!ws.__hist.has(s.symbol)) { ws.__hist.add(s.symbol); ws.send(JSON.stringify({ ticks_history: s.symbol, count: 300, end: 'latest', style: 'ticks', passthrough: { sg: 1 } })); }
  });
}
function onMsg(sock, raw) {
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  if (m.error) {
    if ((m.msg_type === 'proposal' || m.msg_type === 'buy') && sgPending) sgFail(lastTrade && lastTrade.sig, m.error.message || 'Erro na ordem');
    else log('Deriv:', m.msg_type || '', m.error.message || ''); return;
  }
  switch (m.msg_type) {
    case 'tick': {
      const t = m.tick; if (!t) break;
      const sym = t.symbol || t.underlying_symbol, q = Number(t.quote), pip = t.pip_size !== undefined ? t.pip_size : 2;
      sgTick(sym, Number(q.toFixed(pip).slice(-1)), q); break;
    }
    case 'balance': if (m.balance && m.balance.balance !== undefined) { balance = Number(m.balance.balance); if (m.balance.currency) currency = m.balance.currency; } break;
    case 'history': {
      if (!(m.echo_req && m.echo_req.passthrough && m.echo_req.passthrough.sg) || !m.history) break;
      const sym = m.echo_req.ticks_history, pip = m.pip_size || 2, pr = m.history.prices.map(Number);
      sg.d[sym] = (sg.d[sym] || []).concat(pr.map(q => Number(q.toFixed(pip).slice(-1))).reverse()).slice(0, 500);
      sg.p[sym] = pr.slice(-40).concat(sg.p[sym] || []).slice(-40); break;
    }
    case 'proposal':
      if (sgPending && m.proposal) sock.send(JSON.stringify({ buy: m.proposal.id, price: m.proposal.ask_price, subscribe: 1 })); break;
    case 'proposal_open_contract': {
      const c = m.proposal_open_contract;
      if (c && (c.status === 'sold' || c.is_sold) && sgPending) sgOnResult(Number(c.profit) > 0, c.profit, c); break;
    }
  }
}

/* ---------- servidor HTTP (painel + API com senha) ---------- */
const sha = x => crypto.createHash('sha256').update(String(x)).digest();
const sessions = new Set(), fails = new Map();
const ipOf = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
function readBody(req) { return new Promise((res, rej) => { let b = ''; req.on('data', d => { b += d; if (b.length > 10000) { rej(new Error('big')); req.destroy(); } }); req.on('end', () => { try { res(b ? JSON.parse(b) : {}); } catch (e) { res({}); } }); }); }
const send = (res, code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(obj)); };
const authed = req => sessions.has(String(req.headers['x-token'] || ''));
const PANEL = fs.readFileSync(path.join(__dirname, 'painel.html'));
http.createServer(async (req, res) => {
  try {
    const url = (req.url || '/').split('?')[0];
    if (req.method === 'GET' && (url === '/' || url === '/index.html')) { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }); return res.end(PANEL); }
    if (req.method === 'GET' && url === '/health') { res.writeHead(200); return res.end('ok'); }
    if (req.method === 'POST' && url === '/api/login') {
      const ip = ipOf(req), f = fails.get(ip) || { n: 0, until: 0 };
      if (f.until > Date.now()) return send(res, 429, { error: 'Muitas tentativas. Aguarde alguns minutos.' });
      const b = await readBody(req);
      if (crypto.timingSafeEqual(sha(b.senha || ''), sha(SENHA))) {
        fails.delete(ip); const t = crypto.randomBytes(24).toString('hex'); sessions.add(t);
        if (sessions.size > 20) sessions.delete(sessions.values().next().value);
        return send(res, 200, { token: t });
      }
      f.n++; if (f.n >= 5) { f.until = Date.now() + 10 * 60 * 1000; f.n = 0; } fails.set(ip, f);
      return send(res, 401, { error: 'Senha incorreta' });
    }
    if (url === '/api/state' && req.method === 'GET') { if (!authed(req)) return send(res, 401, { error: 'login' }); return send(res, 200, snapshot()); }
    if (url === '/api/do' && req.method === 'POST') {
      if (!authed(req)) return send(res, 401, { error: 'login' });
      doAction(await readBody(req)); return send(res, 200, snapshot());
    }
    res.writeHead(404); res.end('not found');
  } catch (e) { log('Erro HTTP:', e.message); try { send(res, 500, { error: 'erro' }); } catch (x) {} }
}).listen(PORT, () => log('Painel ouvindo na porta', PORT, '· conta', accType));

setInterval(sgLoop, 1000);
connect();
process.on('unhandledRejection', e => log('unhandledRejection', e && e.message));
process.on('uncaughtException', e => log('uncaughtException', e && e.message));
