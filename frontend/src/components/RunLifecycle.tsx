import type {Run} from '../types/domain';import {RUN_STATUS} from '../constants/enums';import {date} from '../utils/format';
const live=(r:Run)=>r.status==='queued'||r.status==='running';
export function retryBadge(r:Run){return r.retry_of?<small className="retry-link">重试自 {r.retry_of.slice(0,8)}</small>:null}
export function heartbeatNote(r:Run){
 if(r.status==='running')return <small className="run-note">第{r.attempt}次尝试 · 执行者 {r.owner||'未登记'} · 心跳 {r.heartbeat_at?date(r.heartbeat_at):'—'}{r.lease_expires_at?' · 租约至 '+date(r.lease_expires_at):''}</small>;
 if(r.status==='queued')return <small className="run-note">第{r.attempt?r.attempt+1:1}次尝试排队中；执行者失联超过租约后由系统自动接管。</small>;
 if(r.status==='canceled')return <small className="run-note">{r.canceled_by||'—'} 于 {date(r.canceled_at)} 取消；本次运行已终止，不会出现频谱；需要时可另起重试。</small>;
 if(r.status==='failed')return <small className="run-note">第{r.attempt}次尝试失败于 {date(r.finished_at)}；错误保留在原运行，重试会新建一条运行。</small>;
 return <small className="run-note">第{r.attempt}次尝试完成于 {date(r.finished_at)}；结果已固化，复核与归档照常进行。</small>;
}
export function RunActions({run,disabled,busy,cancel,retry}:{run:Run;disabled:boolean;busy:boolean;cancel:(id:string)=>void;retry:(id:string)=>void}){
 return <div className="run-actions">
  {live(run)&&<button className="secondary" disabled={disabled||busy} onClick={()=>cancel(run.id)}>{run.status==='running'?'取消计算':'取消排队'}</button>}
  {(run.status==='failed'||run.status==='canceled')&&<button className="secondary" disabled={disabled||busy} onClick={()=>retry(run.id)}>{run.status==='failed'?'重试为新运行':'重新排队计算'}</button>}
  <span className={'status '+run.status}>{RUN_STATUS[run.status]||run.status}</span>
 </div>;
}
