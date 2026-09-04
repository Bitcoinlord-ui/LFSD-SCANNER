import express from 'express';
import { WebSocket } from 'ws';

const app=express(); app.use(express.json()); app.use(express.static('public'));
const PORT=process.env.PORT||3000;
const APP_ID=process.env.DERIV_APP_ID||'1089';
const SYMBOL=process.env.DERIV_SYMBOL||'R_100';
const state={connection:'CONNECTING',symbol:SYMBOL,price:null,candles5:[],candles15:[],activeZone:null,setupStatus:'WAITING',signal:null,lastUpdate:null,history:[],liveTrading:false};
let deriv=null, token=null;
const tf=(ms,t)=>Math.floor(t/ms)*ms;
const candle=(ms,p,t)=>({time:tf(ms,t),open:p,high:p,low:p,close:p});
function addTick(p,t){state.price=p;state.lastUpdate=new Date().toISOString(); for(const [arr,ms] of [[state.candles5,300000],[state.candles15,900000]]){let c=arr.at(-1),k=tf(ms,t);if(!c||c.time!==k){arr.push(candle(ms,p,t));if(arr.length>300)arr.shift()}else{c.high=Math.max(c.high,p);c.low=Math.min(c.low,p);c.close=p}} analyze();}
function analyze(){const a=state.candles15,b=state.candles5;if(a.length<12||b.length<12)return;const z=a.at(-2),prev=a.at(-3);if(!state.activeZone&&z){const range=z.high-z.low; if(range>0){const body=Math.abs(z.close-z.open);if(body<range*.45){state.activeZone={zoneId:`${z.time}`,type:z.close>=z.open?'demand':'supply',low:z.low,high:z.high,formationTime:z.time,status:'ACTIVE'};state.setupStatus='ZONE IDENTIFIED'}}}
const zone=state.activeZone;if(!zone)return; if(state.price<zone.low||state.price>zone.high){if((zone.type==='demand'&&state.price<zone.low)||(zone.type==='supply'&&state.price>zone.high)){zone.status='INVALIDATED';state.history.push({...zone});state.activeZone=null;state.setupStatus='RESET';state.signal=null;return}}
if(state.price>=zone.low&&state.price<=zone.high)state.setupStatus='ZONE ENTRY';const r=b.at(-2),q=b.at(-3);const bullish=r.close>r.open&&r.close>q.high;const bearish=r.close<r.open&&r.close<q.low;if((zone.type==='demand'&&bullish)||(zone.type==='supply'&&bearish)){state.setupStatus='5M BOS/CHOCH';const entry=state.price;const sl=zone.type==='demand'?zone.low-(zone.high-zone.low)*.1:zone.high+(zone.high-zone.low)*.1;const risk=Math.abs(entry-sl);const target=zone.type==='demand'?entry+risk*2:entry-risk*2;state.signal={side:zone.type==='demand'?'BUY':'SELL',entry,SL:sl,TP:target,RR:2,timestamp:new Date().toISOString(),lot:0.02};state.setupStatus='QUALIFIED SIGNAL'}}
function connect(){state.connection='CONNECTING';deriv=new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`);deriv.on('open',()=>{state.connection='LIVE';deriv.send(JSON.stringify({ticks:SYMBOL,subscribe:1}));deriv.send(JSON.stringify({ticks_history:SYMBOL,adjust_start_time:1,count:300,end:'latest',style:'ticks'}));});deriv.on('message',raw=>{try{const m=JSON.parse(raw);if(m.tick)addTick(Number(m.tick.quote),Number(m.tick.epoch)*1000);if(m.history?.times){for(let i=0;i<m.history.times.length;i++)addTick(Number(m.history.prices[i]),Number(m.history.times[i])*1000)}}catch{state.connection='DATA_ERROR'}});deriv.on('close',()=>{state.connection='RECONNECTING';setTimeout(connect,3000)});deriv.on('error',()=>{state.connection='RECONNECTING'});}
app.get('/api/state',(req,res)=>res.json(state));
app.post('/api/live',(req,res)=>{state.liveTrading=Boolean(req.body?.enabled);res.json({liveTrading:state.liveTrading})});
app.post('/api/token',(req,res)=>{token=typeof req.body?.token==='string'?req.body.token:null;res.json({ok:Boolean(token),storedInMemory:true})});
app.post('/api/confirm',(req,res)=>{if(!state.signal)return res.status(400).json({error:'No qualified signal'}); if(!state.liveTrading)return res.status(400).json({error:'Live trading is disabled'}); if(!token)return res.status(400).json({error:'Deriv token not connected'}); res.status(403).json({error:'Real-money execution requires a fresh explicit confirmation immediately before each order; this endpoint intentionally does not place unattended orders.'})});
app.get('/health',(req,res)=>res.json({ok:true,connection:state.connection,uptime:process.uptime()}));
app.listen(PORT,()=>{console.log(`LFSD scanner listening on ${PORT}`);connect()});
