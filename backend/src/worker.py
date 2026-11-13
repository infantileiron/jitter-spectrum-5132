from pathlib import Path
import os,socket,threading,uuid,traceback
import redis
from sqlalchemy import select,update
from src.config.settings import REDIS_URL,RUN_LEASE_SECONDS,RUN_HEARTBEAT_SECONDS,RUN_RECOVERY_SECONDS
from src.config.database import Session
from src.models import SpectralRun,SpectralStudy,SamplingWindow,SignalDataset
from src.services.windows import points_for
from src.services.analysis import analyze
from src.utils.time import now,stamp,lease_deadline
QUEUE="jitter-spectrum:jobs"
CANCEL="jitter-spectrum:cancel:"
client=redis.Redis.from_url(REDIS_URL);heartbeat=Path("/artifacts/worker-heartbeat")
WORKER=os.getenv("WORKER_ID",socket.gethostname()+":"+str(os.getpid())+":"+uuid.uuid4().hex[:8])
def owner_token():return WORKER+":"+uuid.uuid4().hex[:8]
def canceled_in_redis(id):
 try:return client.exists(CANCEL+id)==1
 except redis.RedisError:return False
def own_update(id,owner,attempt,values):
 """All writes are guarded by status+owner+attempt. Once a cancel or takeover
 commits, the previous worker becomes a no-op writer automatically."""
 with Session() as db:
  changed=db.execute(update(SpectralRun).where(SpectralRun.id==id,SpectralRun.status=="running",SpectralRun.owner==owner,SpectralRun.attempt==attempt).values(**values)).rowcount
  db.commit()
 return bool(changed)
def renew(id,owner,attempt):
 return own_update(id,owner,attempt,{"heartbeat_at":stamp(),"lease_expires_at":lease_deadline(RUN_LEASE_SECONDS)})
def finish(id,owner,attempt,status,result=None,error=""):
 with Session() as db:
  values={"status":status,"result":result or {},"error":error,"finished_at":now(),"lease_expires_at":""}
  changed=db.execute(update(SpectralRun).where(SpectralRun.id==id,SpectralRun.status=="running",SpectralRun.owner==owner,SpectralRun.attempt==attempt).values(**values)).rowcount
  db.commit()
 return bool(changed)
def keep_alive(id,owner,attempt,stop):
 """Lease renewal loop; also converts cancellation/takeover into an early stop."""
 while not stop.wait(RUN_HEARTBEAT_SECONDS):
  if canceled_in_redis(id) or not renew(id,owner,attempt):
   stop.set();return
class Lease:
 def __init__(self,id,owner,attempt):
  self.stop=threading.Event();self.thread=threading.Thread(target=keep_alive,args=(id,owner,attempt,self.stop),daemon=True);self.thread.start()
 def release(self):
  self.stop.set();self.thread.join(timeout=RUN_HEARTBEAT_SECONDS+2)
 def lost(self):return self.stop.is_set()
def claim(id):
 """Optimistic claim: a queued run is promoted to running with a fresh owner token
 and attempt number. Two workers racing on the same queue entry cannot both win."""
 with Session() as db:
  run=db.get(SpectralRun,id)
  if not run or run.status!="queued":return None
  attempt=run.attempt+1;token=owner_token()
  changed=db.execute(update(SpectralRun).where(SpectralRun.id==id,SpectralRun.status=="queued",SpectralRun.attempt==run.attempt).values(status="running",owner=token,attempt=attempt,started_at=now(),canceled_at="",canceled_by="",finished_at="",error="",result={},heartbeat_at=stamp(),lease_expires_at=lease_deadline(RUN_LEASE_SECONDS))).rowcount
  db.commit()
  return (id,token,attempt) if changed else None
def execute(id):
 claimed=claim(id)
 if not claimed:return
 id,owner,attempt=claimed
 if canceled_in_redis(id):
  finish(id,owner,attempt,"canceled");return
 lease=Lease(id,owner,attempt)
 try:
  with Session() as db:
   run=db.get(SpectralRun,id);study=db.get(SpectralStudy,run.study_id)
   window=db.get(SamplingWindow,study.window_id) if study else None;dataset=db.get(SignalDataset,window.dataset_id) if window else None
   if not study or not window or not dataset:raise ValueError("研究或窗口数据不存在")
   points=points_for(dataset,window)
  result=analyze(points,study)
  if lease.lost():return   # canceled or taken over mid-compute: never publish
  finish(id,owner,attempt,"completed",result=result)
 except Exception as e:
  traceback.print_exc()
  if not lease.lost():finish(id,owner,attempt,"failed",error=str(e))
 finally:lease.release()
def recover():
 from src.services.runs import recover as service_recover
 with Session() as db:return service_recover(db,RUN_RECOVERY_SECONDS)
def requeue_pending():
 """At start-up queue anything still queued on disk, including work taken over
 while Redis was briefly unreachable. Duplicate entries are harmless because
 claim() is conditional on queued status + attempt."""
 with Session() as db:
  for id in db.scalars(select(SpectralRun.id).where(SpectralRun.status=="queued")):client.lpush(QUEUE,id)
if __name__=="__main__":
 try:requeue_pending()
 except redis.RedisError:traceback.print_exc()
 while True:
  heartbeat.touch()
  try:recover()
  except Exception:traceback.print_exc()
  try:job=client.brpop(QUEUE,timeout=1)
  except redis.RedisError:job=None
  if job:execute(job[1].decode())
