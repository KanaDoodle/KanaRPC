'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number = value => Number(value || 0);
const fmt = value => number(value).toLocaleString('zh-CN');
const ms = value => `${number(value).toFixed(1)} ms`;
const methods = {'/showcase/fast':'快请求','/showcase/slow':'慢请求','/showcase/stream':'流式请求','/showcase/drain':'在途 drain 请求'};
const stages = {open:'解析 OPEN',admitted:'取得请求名额',queued:'进入方法调度',handler_started:'handler 开始',handler_done:'handler 返回',execution_skipped:'未执行 handler',data_sent:'发送 DATA',cancel_received:'观察到 CANCEL',terminal_ready:'准备终态',terminal_sent:'发送 END',ack_received:'观察到 ACK',late_ack:'忽略迟到 ACK',receipt_acknowledged:'终态回执确认',receipt_canceled:'取消结束回执等待',receipt_deadline:'请求期限结束等待',receipt_lost:'终态未获回执',retired:'服务端请求资源释放',peer_fin:'观察到客户端 FIN',exchange_incomplete:'交换结束未确认',queue_expired:'排队上下文结束',rejected:'拒绝接纳',budget_rejected:'本机字节预算拒绝'};
const reasons = {record_parsed:'记录已解析；不等于已接纳',request_capacity_reserved:'在途名额已取得',method_scheduler:'按方法的有界公平调度',execution_slot_acquired:'已获得执行槽位',handler_returned:'执行返回；不等于终态送达',dispatch_context_finished:'执行前上下文已结束；没有调用 handler',credit_consumed:'消费客户端提供的流信用',first_cancel_control:'收到第一次有效 CANCEL；不证明它最终胜出',handler_result_classified:'处理器结果已分类',receipt_expected:'此 END 需要协议回执',request_deadline_no_receipt:'请求 deadline 已到，不再建立回执等待',protocol_receipt_observed:'客户端协议层 ACK；不是业务提交',settlement_already_frozen:'不改写已经累计的故障',delivery_receipt_confirmed:'服务端已经确认 ACK',explicit_cancel_won:'显式取消先结束等待',request_deadline:'请求自身 deadline 已到',terminal_not_acknowledged:'等待结束，未确认 ACK，累计故障保留',server_request_released:'服务端请求记账释放；不代表应用已消费',native_send_direction_finished:'客户端完成原生发送方向',client_fin_not_observed:'已收到 ACK，但客户端 FIN 未确认',queue_context_finished:'排队超时或请求取消；不是字节配额拒绝',capacity:'在途/会话容量不足',policy:'治理队列容量不足',draining:'服务端已经停止接纳',invalid:'OPEN 字段或调用类型无效',not_found:'未注册的方法',local_send_reservation:'发送字节预约失败'};
let current = null, cursor = 0, loading = false, pending = false, dirty = false, version = 0;
let selected = '', prefer = '', session = 0, previous = null, historyPoints = [], apiError = '', networkError = '';
const requests = new Map();
const controls = [['rate','rate-value',' RPS'],['slow-percent','slow-percent-value','%'],['slow-ms','slow-ms-value',' ms'],['capacity','capacity-value','']];
function outputs(){for(const [id,out,suffix] of controls) $(out).textContent = $(id).value + suffix;}
for(const [id] of controls) $(id).addEventListener('input',()=>{dirty=true;outputs();});
outputs();
function record(events,d){
 for(const e of events || []){
  if(number(e.sequence)<=cursor)continue;
  cursor = Math.max(cursor,number(e.sequence));
  if(!e.request) continue;
  const key=`${e.channel}:${e.request}`;
  const row=requests.get(key)||{key,method:e.method||'',events:[],code:'',receipt:'',cancel:false,phase:'open'};
  if(e.method) row.method=e.method;
  const origins=[...(d.history||[]).map(h=>({session:h.session,start:h.trace_start})),{session:d.session,start:d.trace_start}];
  row.session=origins.filter(o=>number(e.sequence)>number(o.start)).at(-1)?.session||0;
  row.phase=e.kind;
  if(e.kind==='handler_done'||e.kind==='rejected') row.code=e.code||'';
  if(e.kind==='cancel_received') row.cancel=true;
  if(e.kind==='terminal_sent') row.receipt=e.reason==='receipt_expected'?'等待回执':'无回执义务';
  if(e.kind==='receipt_acknowledged') row.receipt='已回执';
  if(e.kind==='receipt_canceled') row.receipt='取消结束';
  if(e.kind==='receipt_deadline') row.receipt='请求期限结束';
  if(e.kind==='receipt_lost') row.receipt='未回执';
  if(e.kind==='exchange_incomplete') row.receipt='缺少 FIN';
  row.events.push(e);if(row.events.length>96)row.events.shift();
  requests.delete(key);requests.set(key,row);
 }
 while(requests.size>240)requests.delete(requests.keys().next().value);
}
function keyOf(ref){return ref?.channel&&ref?.request?`${ref.channel}:${ref.request}`:'';}
function choose(d){
 if(!selected&&d.drain_error&&keyOf(d.fault_ref))prefer='fault';
 if(prefer==='stream'&&d.stream){const key=keyOf(d.stream.ref);if(requests.has(key))selected=key;}
 if(prefer==='fault'){const key=keyOf(d.fault_ref);if(requests.has(key))selected=key;}
 if(prefer==='drain'){const row=Array.from(requests.values()).filter(r=>r.method==='/showcase/drain').at(-1);if(row)selected=row.key;}
 if(!requests.has(selected))selected=Array.from(requests.keys()).at(-1)||'';
}
function color(kind){if(['receipt_lost','exchange_incomplete','rejected','budget_rejected'].includes(kind))return 'bad';if(['cancel_received','receipt_canceled','receipt_deadline','queue_expired'].includes(kind))return 'warn';if(kind==='receipt_acknowledged')return 'good';return '';}
function renderRequests(){
 const list=$('requests'),scroll=list.scrollTop;
 list.innerHTML=Array.from(requests.values()).reverse().slice(0,150).map(r=>{
  let status=r.receipt||(r.phase==='rejected'?'拒绝接纳':(r.cancel?'取消已观察':stages[r.phase]||r.phase));
  const cls=['未回执','缺少 FIN','拒绝接纳'].includes(status)?'bad':r.cancel&&r.receipt!=='已回执'?'warn':r.receipt==='已回执'?'good':'';
  return `<button class="request-row ${selected===r.key?'selected':''}" data-key="${esc(r.key)}"><span class="request-id">${r.session?`S${r.session} / `:``}${esc(r.key)}</span><span>${esc(methods[r.method]||r.method||'方法未观察到')}</span><span class="request-outcome ${cls}">${esc(status)}</span></button>`;
 }).join('')||'<p class="empty">先制造过载或开始一条流，再选中请求查看。</p>';
 list.scrollTop=scroll;
 const panel=$('timeline'),old=panel.scrollTop,changed=panel.dataset.key!==selected;
 panel.dataset.key=selected;
 const row=requests.get(selected);
 if(!row){panel.innerHTML='<p class="empty">选择左侧一条请求。缺少事件不代表它没有发生。</p>';return;}
 const first=new Date(row.events[0].at).getTime();
 panel.innerHTML=`<h4>${esc(methods[row.method]||row.method)} <span class="request-id">${esc(row.key)}</span></h4><div class="timeline-note">会话 ${row.session||"更早"}；本地通道 : wire 请求 ID。最多 96 个事件。${row.code?` handler 状态：${esc(row.code)}。`:''}</div>`+row.events.map(e=>`<div class="event ${color(e.kind)}"><time>+${(new Date(e.at).getTime()-first).toFixed(0)} ms</time><strong>${esc(stages[e.kind]||e.kind)}${e.kind==='data_sent'?` #${number(e.value)}`:''}</strong><small>${esc(reasons[e.reason]||e.reason)}${e.code&&e.code!=='ok'?` [${esc(e.code)}]`:''}</small></div>`).join('');
 panel.scrollTop=changed?0:old;
}
$('requests').addEventListener('click',event=>{const button=event.target.closest('[data-key]');if(button){selected=button.dataset.key;prefer='';renderRequests();}});
function chart(d){
 if(previous&&session===d.session&&+new Date(d.now)===+previous.at)return;
 const fast=d.metrics['/showcase/fast']||{},slow=d.metrics['/showcase/slow']||{},calls=number(fast.calls)+number(slow.calls);
 if(session!==d.session){session=d.session;historyPoints=[];previous=null;}
 let rate=0;if(previous){const dt=(new Date(d.now)-previous.at)/1000;if(dt>0)rate=Math.max(0,(calls-previous.calls)/dt);}
 previous={at:new Date(d.now),calls};
 historyPoints.push({fast:number(fast.p95_ms),slow:number(slow.p95_ms)});if(historyPoints.length>90)historyPoints.shift();
 const max=Math.max(20,...historyPoints.map(p=>Math.max(p.fast,p.slow)));
 for(const kind of ['fast','slow']){const points=historyPoints.map((p,i)=>`${i?'L':'M'}${(i*640/Math.max(89,historyPoints.length-1)).toFixed(1)},${(125-p[kind]/max*110).toFixed(1)}`).join(' ');$(kind+'-line').setAttribute('d',points);}
 $('chart-scale').textContent=`纵轴上限 ${max.toFixed(0)} ms`;$('fast-p95').textContent=ms(fast.p95_ms);$('slow-p95').textContent=ms(slow.p95_ms);$('recent-rate').textContent=`${rate.toFixed(0)} 次完成 / 秒`;
 $('results').textContent=`${fmt(number(fast.success)+number(slow.success))} / ${fmt(number(fast.failed)+number(slow.failed))}`;
}
function render(d){
 current=d;record(d.trace.events,d);choose(d);
 $('connection-dot').classList.add('online');$('connection').textContent='页面已连接';
 const status={running:'正在接纳',starting:'启动中',closing:'关闭旧会话',draining:'停止接纳，收尾中',drained:'资源已退出',drain_failed:'Drain 报告失败',closed:'已关闭'};
 $('session-label').textContent=`会话 ${d.session||'准备中'} / ${d.mode==='fixed'?'固定上限 + 方法公平':'自适应 + 客户端反馈'}`;
 $('server-state').textContent=status[d.state]||'准备中';$('server-state').className='status'+(d.state==='drain_failed'?' failed':'');$('message').textContent=d.message;
 $('error').hidden=!(apiError||networkError);$('error').textContent=networkError||apiError;
 const s=d.stats;$('admitted').textContent=fmt(s.Admitted);$('queued').textContent=fmt(s.Queued);$('executing').textContent=fmt(s.Executing);$('finishing').textContent=fmt(Math.max(0,s.InFlight-s.Queued-s.Executing));$('completed').textContent=fmt(s.Completed);
 chart(d);$('rejected').textContent=fmt(s.Rejected);$('feedback-limited').textContent=fmt(d.feedback.Limited);$('cancel-observed').textContent=fmt(s.Outcomes.CancelRequests);$('generator-dropped').textContent=fmt(d.generator_dropped);
 $('historical-faults').textContent=fmt(d.total_lost+d.total_incomplete);$('historical-faults').className=d.total_lost+d.total_incomplete?'nonzero':'';
 const msRows=(d.governance.Methods||[]).filter(m=>['/showcase/fast','/showcase/slow'].includes(m.Method));
 $('methods').innerHTML=msRows.map(m=>`<div class="method ${m.Method.endsWith('/slow')?'slow':''}"><strong>${esc(methods[m.Method])}</strong><div class="method-numbers"><b>${m.Limit}</b>并发上限　执行 ${m.Running} / 排队 ${m.Queued}<br>handler p90 ${ms(m.P90/1e6)}　${esc(m.Mode)}</div></div>`).join('');
 const latest=(d.governance.Decisions||[]).slice(-3).reverse();
 $('decisions').innerHTML=latest.map(x=>`<span class="decision">${esc(methods[x.Method]||x.Method)} <strong>${x.Previous} → ${x.Next}</strong> ${esc(({latency_or_overload:'延迟或过载，降低并发',demand_growth:'有未满足需求，尝试增加',target_unmet_fallback:'持续未达目标，固定回退',operator_reset:'操作员复位'})[x.Reason]||x.Reason)}</span>`).join('')||'尚未产生并发调整。固定模式不会自动调整上限。';
 const running=d.state==='running',blocked=pending||d.busy;
 for(const id of ['overload','recover','stop-load','apply-load','start-stream','drain','fault-drain'])$(id).disabled=!running||blocked;
 $('new-session').disabled=blocked;
 const stream=d.stream;
 $('start-stream').disabled=!running||blocked||!!stream&&['active','cancel_requested'].includes(stream.state);
 $('cancel-stream').disabled=!stream||stream.state!=='active'||pending;
 $('stream-state').textContent=stream?({active:'正在接收',cancel_requested:'已请求取消',canceled:'客户端已取消',completed:'自然结束',failed:'请求失败'}[stream.state]||stream.state):'尚未开始';$('chunk-count').textContent=`${fmt(stream?.count)} 个片段`;
 if(stream){const box=$('chunks'),top=box.scrollTop,bottom=box.scrollHeight-box.scrollTop-box.clientHeight<20;box.innerHTML=(stream.chunks||[]).map(c=>`<div>${esc(c.text)}</div>`).join('')||'等待服务端片段…';box.scrollTop=bottom?box.scrollHeight:top;
  const row=requests.get(keyOf(stream.ref));const has=kind=>row?.events.some(e=>e.kind===kind);
  $('stream-observation').textContent=`CANCEL：${has('cancel_received')?'已观察':'未观察'} / handler 返回：${has('handler_done')?'已观察':'未观察'} / 服务端释放：${has('retired')?'已观察':'未观察'}。${stream.error||'未观察到的事件可能尚未发生或已丢弃。'}`;
 }else{$('chunks').textContent='开始后，片段将从真实 QUIC 流陆续到达。';$('stream-observation').textContent='取消信号、handler 退出与资源释放分别记录。';}
 const lastError=Object.values(d.metrics).map(m=>m.last_error).filter(Boolean).at(-1);$('last-rpc-error').hidden=!lastError;$('last-rpc-error').textContent=lastError?'最近 RPC 错误：'+lastError:'';
 const report=d.report;
 $('drain-result').className='drain-result'+(d.drain_error?' failed':'');
 if(report){$('drain-result').textContent=`返回时资源静止：${report.Quiescent?'是':'否'}；历史 / 本次新增未回执：${report.PriorFaults.LostTerminals} / ${report.AddedFaults.LostTerminals}。${d.drain_error||'本会话 drain 未报告错误。'}`;}else{$('drain-result').textContent=d.state==='draining'?'正在等待实际执行、ACK 与 FIN；失败会原样保留。':'在途请求会继续收尾。长流未结束时，可能触发截止时间。';}
 $('history-list').innerHTML=(d.history||[]).slice().reverse().map(h=>`<p>会话 ${h.session} / ${esc(h.mode)}：RPC 失败 ${Object.values(h.metrics||{}).reduce((n,m)=>n+number(m.failed),0)}，未回执 ${h.stats.LostTerminals}。${esc(h.error||'未报告 drain 错误')}</p>`).join('')||'尚无旧会话。';
 const tr=d.trace;$('trace-health').textContent=`容量 ${tr.capacity} / 覆盖 ${fmt(tr.overwritten)} / 争用丢弃 ${fmt(tr.dropped)} / 采样跳过 ${fmt(tr.sampled_out)}${tr.gap?' / 本次存在缺口':''}`;
 $('resource-summary').textContent=`在途 ${s.InFlight}　原生流 ${s.NativeStreams}　服务端收/发 ${(d.memory.Receive.Used/1024).toFixed(1)}/${(d.memory.Send.Used/1024).toFixed(1)} KiB　客户端接收 ${(d.client_memory.Receive.Used/1024).toFixed(1)} KiB`;
 $('endpoint').textContent=`QUIC ${d.endpoint}`;
 if(!dirty&&!controls.some(([id])=>document.activeElement===$(id))){$('rate').value=d.config.rate;$('slow-percent').value=d.config.slow_percent;$('slow-ms').value=d.config.slow_ms;$('capacity').value=d.config.capacity;outputs();}
 renderRequests();
}
async function refresh(){
 if(loading)return;loading=true;const started=version;
 try{const response=await fetch(`/api/state?after=${cursor}`,{cache:'no-store'});if(!response.ok)throw Error(`HTTP ${response.status}`);const d=await response.json();if(started===version){networkError='';render(d);}}
 catch(error){networkError=`连接中断，保留上次数据：${error.message}`;$('connection-dot').classList.remove('online');$('connection').textContent='连接已中断';$('error').hidden=false;$('error').textContent=networkError;}
 finally{loading=false;}
}
async function act(path,body,selection=''){
 if(pending)return;pending=true;version++;apiError='';if(selection)prefer=selection;if(current)render(current);
 try{const response=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await response.json();if(!response.ok)throw Error(d.error||`HTTP ${response.status}`);dirty=false;}
 catch(error){apiError=error.message;}
 finally{pending=false;if(current)render(current);await refresh();}
}
$('load-form').addEventListener('submit',event=>{event.preventDefault();act('/api/load',{rate:+$('rate').value,slow_percent:+$('slow-percent').value,slow_ms:+$('slow-ms').value,capacity:+$('capacity').value});});
$('overload').onclick=()=>act('/api/preset',{name:'overload'});
$('recover').onclick=()=>act('/api/preset',{name:'recover'});
$('stop-load').onclick=()=>act('/api/preset',{name:'stop'});
$('new-session').onclick=()=>act('/api/session',{mode:$('mode').value});
$('start-stream').onclick=()=>act('/api/stream',{action:'start'},'stream');
$('cancel-stream').onclick=()=>act('/api/stream',{action:'cancel'},'stream');
$('drain').onclick=()=>act('/api/drain',{omit_ack:false},'drain');
$('fault-drain').onclick=()=>act('/api/drain',{omit_ack:true},'fault');
refresh();setInterval(refresh,500);
