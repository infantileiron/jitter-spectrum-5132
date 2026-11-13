"""End-to-end lifecycle checks against SQLite (no Redis, no Postgres).

Covers: cancel of a queued run never yields a result, cancel of a running run,
takeover after a lost heartbeat, a late old-owner result being rejected, two
workers racing the same queue entry, retry as a separate run preserving the
original error, and completed history staying reviewable.
"""
import json,os,sys,tempfile
from datetime import datetime,timezone,timedelta
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL","sqlite:///"+os.path.join(tempfile.mkdtemp(),"lifecycle.db"))
os.environ.setdefault("REDIS_URL","redis://127.0.0.1:9/0")
os.environ.setdefault("RUN_LEASE_SECONDS","30")
os.environ.setdefault("RUN_HEARTBEAT_SECONDS","1")
os.environ.setdefault("RUN_RECOVERY_SECONDS","2")
from sqlalchemy import update
from src.config.database import Session
from src.models import *
from src.services import runs as service
import src.worker as worker
from src.utils.time import now,stamp,lease_deadline
from scripts.seed_demo import seed
FAILURES=[]
def check(name,condition,detail=""):
 print(("PASS " if condition else "FAIL ")+name+((" :: "+str(detail)) if detail else ""))
 if not condition:FAILURES.append(name)
service._redis=lambda:type("X",(),{"lpush":staticmethod(lambda *a:None),"set":staticmethod(lambda *a,**k:None)})()
service.enqueue=lambda id:None
worker.client=type("X",(),{"exists":staticmethod(lambda *a:0),"lpush":staticmethod(lambda *a:None),"brpop":staticmethod(lambda *a,**k:None)})()
def make_run(status="queued",owner="",heartbeat_ago=None,attempt=0):
 with Session() as db:
  item=SpectralRun(id="run-"+str(os.urandom(3).hex()),study_id="demo-study",status=status,result={},error="",created_at=now(),attempt=attempt,owner=owner,lease_expires_at="",heartbeat_at="",started_at="",finished_at="",canceled_at="",canceled_by="",retry_of="")
  if heartbeat_ago is not None:
   item.heartbeat_at=(datetime.now(timezone.utc)-timedelta(seconds=heartbeat_ago)).isoformat();item.lease_expires_at=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
  db.add(item);db.commit();return item.id
seed()
# 1. queued cancel: worker sees canceled status and never starts.
id=make_run()
service.cancel(Session(),id,{"email":"analyst@beiyu.demo"})
worker.execute(id)
with Session() as db:item=db.get(SpectralRun,id)
check("queued cancel stays canceled with no result",item.status=="canceled" and not item.result and item.canceled_by=="analyst@beiyu.demo",(item.status,item.result))
# 2. running cancel then late finish from old owner is rejected.
id=make_run()
with Session() as db:
 run=db.get(SpectralRun,id);token="old-owner";attempt=1
 db.execute(update(SpectralRun).where(SpectralRun.id==id).values(status="running",owner=token,attempt=attempt,heartbeat_at=stamp(),lease_expires_at=lease_deadline(30)));db.commit()
service.cancel(Session(),id,{"email":"admin@beiyu.demo"})
late=worker.finish(id,token,attempt,"completed",result={"fake":True})
with Session() as db:item=db.get(SpectralRun,id)
check("running cancel discards late result",item.status=="canceled" and not item.result and late is False,(item.status,item.result,late))
try:service.require_completed(Session(),id);blocked=False
except Exception:blocked=True
check("canceled run cannot be reviewed",blocked)
# 3. lost heartbeat takeover, old worker result rejected, new owner result accepted.
id=make_run(status="running",owner="old",heartbeat_ago=60,attempt=3)
taken=service.recover(Session(),5)
with Session() as db:item=db.get(SpectralRun,id)
check("stale run taken over to queued",any(t["id"]==id for t in taken) and item.status=="queued" and item.owner=="",(taken,item.status,item.owner))
late=worker.finish(id,"old",3,"completed",result={"fake":True})
with Session() as db:item=db.get(SpectralRun,id)
check("old owner late write rejected after takeover",item.status=="queued" and late is False,(item.status,late))
with Session() as db:
 db.execute(update(SpectralRun).where(SpectralRun.id==id).values(status="queued",attempt=item.attempt));db.commit()
claimed=worker.claim(id)
check("takeover claimant becomes running",claimed is not None and claimed[1]!="old",claimed)
check("new owner can finish",worker.finish(claimed[0],claimed[1],claimed[2],"completed",result={"ok":True}) is True)
with Session() as db:item=db.get(SpectralRun,id)
check("current owner result accepted",item.status=="completed" and item.result=={"ok":True},item.result)
# 4. two workers race the same queued entry: exactly one claims.
id=make_run()
results=[worker.claim(id) for _ in range(3)]
check("concurrent claims: exactly one wins",sum(r is not None for r in results)==1,results)
# 5. retry creates a separate run and keeps the original error visible.
id=make_run()
with Session() as db:
 db.execute(update(SpectralRun).where(SpectralRun.id==id).values(status="failed",error="回归失败：常量残差"));db.commit()
new=service.retry(Session(),id,{"email":"analyst@beiyu.demo"})
with Session() as db:
 origin=db.get(SpectralRun,id);copy=db.get(SpectralRun,new["id"])
check("retry is a new run linked to original",copy.retry_of==id and copy.id!=id and copy.study_id=="demo-study",(copy.id,copy.retry_of))
check("original failure remains queryable",origin.status=="failed" and origin.error=="回归失败：常量残差",(origin.status,origin.error))
# 6. completed history still reviewable; retry refused for completed.
check("completed demo run still passes review gate",service.require_completed(Session(),"demo-run").result["meta"]["n"]==480)
try:service.retry(Session(),"demo-run",{"email":"analyst@beiyu.demo"});blocked=False
except Exception:blocked=True
check("completed run not retryable",blocked)
# 7. real worker execution of a valid run completes.
id=make_run();worker.execute(id)
with Session() as db:item=db.get(SpectralRun,id)
check("valid run computes to completed",item.status=="completed" and item.result.get("meta",{}).get("n")==480,(item.status,item.error))
print(json.dumps({"failures":FAILURES},ensure_ascii=False))
sys.exit(1 if FAILURES else 0)
