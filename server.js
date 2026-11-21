'use strict';
const http=require('node:http');
const fs=require('node:fs');
const path=require('node:path');
const {randomUUID}=require('node:crypto');
const {WebSocketServer,WebSocket}=require('ws');
const PORT=Number(process.env.PORT||3000),indexPath=path.join(__dirname,'index.html');
const rooms=new Map(),joinRates=new Map(),MAX_PEERS=4;
const server=http.createServer((req,res)=>{
 const pathname=new URL(req.url,'http://localhost').pathname;
 if(pathname==='/health'){res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'});return res.end(JSON.stringify({ok:true,maxPeers:MAX_PEERS}))}
 if((pathname==='/'||pathname==='/index.html')&&req.method==='GET'){res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});return fs.createReadStream(indexPath).pipe(res)}
 res.writeHead(404,{'content-type':'text/plain; charset=utf-8'});res.end('Not found')
});
const wss=new WebSocketServer({server,path:'/signal',maxPayload:128*1024});
function send(ws,value){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(value))}
function getRoom(code){return rooms.get(code)}
function roomFor(ws){return ws.roomCode?getRoom(ws.roomCode):null}
function rateLimited(ip){const now=Date.now(),times=(joinRates.get(ip)||[]).filter(t=>now-t<60000);times.push(now);joinRates.set(ip,times);return times.length>18}
function roster(room){return room.peers.map(p=>({id:p.id,name:p.displayName}))}
wss.on('connection',(ws,req)=>{
 ws.roomCode=null;ws.id=randomUUID();ws.displayName='เพื่อน';
 const ip=String(req.headers['x-forwarded-for']||req.socket.remoteAddress||'').split(',')[0].trim();
 ws.on('message',raw=>{
  let msg;try{msg=JSON.parse(raw.toString())}catch{return send(ws,{type:'error',message:'bad-message'})}
  if(!msg||typeof msg.type!=='string')return;
  if(msg.type==='create'||msg.type==='join'){
   if(ws.roomCode)return;
   if(rateLimited(ip))return send(ws,{type:'error',message:'slow-down'});
   const code=String(msg.room||'');if(!/^\d{6}$/.test(code))return send(ws,{type:'error',message:'bad-room'});
   let room=getRoom(code);
   if(msg.type==='create'){if(room)return send(ws,{type:'code-taken'});room={peers:[]};rooms.set(code,room)}
   else if(!room)return send(ws,{type:'not-found'});
   if(room.peers.length>=MAX_PEERS)return send(ws,{type:'room-full'});
   const existing=room.peers.slice();ws.roomCode=code;ws.displayName=String(msg.name||'เพื่อน').trim().slice(0,18)||'เพื่อน';room.peers.push(ws);
   send(ws,{type:existing.length?'joined':'waiting',room:code,id:ws.id,peers:existing.map(p=>({id:p.id,name:p.displayName}))});
   existing.forEach(peer=>send(peer,{type:'peer-joined',peer:{id:ws.id,name:ws.displayName}}));
   return
  }
  const room=roomFor(ws);if(!room)return;
  if(msg.type==='signal'&&msg.data&&['offer','answer','candidate'].includes(msg.data.type)){
   const target=room.peers.find(peer=>peer.id===msg.to);if(target&&target!==ws)send(target,{type:'signal',from:ws.id,data:msg.data})
  }
 });
 ws.on('close',()=>{const room=roomFor(ws);if(!room)return;room.peers=room.peers.filter(p=>p!==ws);room.peers.forEach(peer=>send(peer,{type:'peer-left',id:ws.id}));if(!room.peers.length)rooms.delete(ws.roomCode)})
});
server.listen(PORT,'0.0.0.0',()=>console.log('Together Frame listening on '+PORT));
