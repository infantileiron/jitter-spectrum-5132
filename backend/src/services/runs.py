import uuid,redis
from datetime import datetime,timezone,timedelta
from fastapi import HTTPException
from sqlalchemy import select,update
from src.models import SpectralRun,SpectralStudy
from src.config.settings import REDIS_URL
from src.utils.time import now
from src.utils.serialization import serialize
QUEUE="jitter-spectrum:jobs"
CANCEL="jitter-spectrum:cancel:"
def _redis():return redis.Redis.from_url(REDIS_URL)
def enqueue(id):
 _redis().lpush(QUEUE,id)
def mark_canceled_in_redis(id):
 """Redis only shortcuts a still-looping worker's cancellation check.
 Acceptance of a result is decided atomically in PostgreSQL, never in Redis."""
 try:_redis().set(CANCEL+id,"1",ex=3600)
 except redis.RedisError:pass
def _new_run(study_id,retry_of=""):
 return SpectralRun(id=str(uuid.uuid4()),study_id=study_id,status="queued",result={},error="",created_at=now(),attempt=0,owner="",lease_expires_at="",heartbeat_at="",started_at="",finished_at="",canceled_at="",canceled_by="",retry_of=retry_of)
def _queue_or_fail(db,item):
 try:enqueue(item.id)
 except redis.RedisError:
  item.status="failed";item.error="后台队列不可用，请重试";item.finished_at=now();db.commit();raise HTTPException(503,item.error)
 return serialize(item)
def create(db,study_id):
 if not db.get(SpectralStudy,study_id):raise HTTPException(404,"频谱研究不存在")
 item=_new_run(study_id);db.add(item);db.commit()
 return _queue_or_fail(db,item)
def retry(db,id,user):
 origin=db.get(SpectralRun,id)
 if not origin:raise HTTPException(404,"运行不存在")
 if origin.status not in ("failed","canceled"):raise HTTPException(409,"只有失败或已取消的运行可以重试；已完成结果请直接复核")
 item=_new_run(origin.study_id,retry_of=origin.id);db.add(item);db.commit()
 return _queue_or_fail(db,item)
def cancel(db,id,user):
 item=db.get(SpectralRun,id)
 if not item:raise HTTPException(404,"运行不存在")
 if item.status not in ("queued","running"):raise HTTPException(409,"只有排队中或计算中的运行可以取消")
 item.status="canceled";item.canceled_at=now();item.canceled_by=user["email"];item.finished_at=item.canceled_at;item.owner="";item.lease_expires_at="";db.commit()
 mark_canceled_in_redis(id)
 return serialize(item)
def require_completed(db,id):
 item=db.get(SpectralRun,id)
 if not item:raise HTTPException(404,"运行不存在")
 if item.status!="completed":raise HTTPException(409,"计算尚未完成")
 return item
def recover(db,seconds):
 """Requeue runs whose worker stopped heartbeating. Each takeover is an optimistic
 conditional update on the exact owner + attempt snapshot, so several recovery
 passes (backend loop, worker loop) can race safely: exactly one wins.
 The old owner token becomes invalid the moment the row is requeued; a late result
 can no longer be accepted."""
 cutoff=datetime.now(timezone.utc)-timedelta(seconds=seconds)
 taken=[]
 running=db.scalars(select(SpectralRun).where(SpectralRun.status=="running")).all()
 for item in running:
  try:beat=datetime.fromisoformat(item.heartbeat_at)
  except ValueError:beat=None
  if beat is not None and beat>=cutoff:continue
  updated=db.execute(update(SpectralRun).where(SpectralRun.id==item.id,SpectralRun.status=="running",SpectralRun.owner==item.owner,SpectralRun.attempt==item.attempt).values(status="queued",owner="",lease_expires_at="",heartbeat_at="",started_at="",finished_at="")).rowcount
  if updated:taken.append({"id":item.id,"previous_owner":item.owner})
 db.commit()
 for record in taken:
  try:enqueue(record["id"])
  except redis.RedisError:pass   # stays queued; the next recovery pass requeues it
 return taken
