import os
DATABASE_URL=os.getenv("DATABASE_URL","postgresql+psycopg://spectrum:spectrum-demo@db/spectrum")
REDIS_URL=os.getenv("REDIS_URL","redis://redis:6379/0")
JWT_SECRET=os.getenv("JWT_SECRET","jitter-spectrum-development-secret-change")
SEED_DEMO=os.getenv("SEED_DEMO","true").lower()=="true"
RUN_LEASE_SECONDS=int(os.getenv("RUN_LEASE_SECONDS","30"))
RUN_HEARTBEAT_SECONDS=int(os.getenv("RUN_HEARTBEAT_SECONDS","5"))
RUN_RECOVERY_SECONDS=int(os.getenv("RUN_RECOVERY_SECONDS","10"))
