from sqlalchemy import String,Double,Integer,JSON,ForeignKey,Text
from sqlalchemy.orm import Mapped,mapped_column
from src.config.database import Base
class SpectralRun(Base):
 __tablename__="spectral_runs"
 id:Mapped[str]=mapped_column(String,primary_key=True)
 study_id:Mapped[str]=mapped_column(ForeignKey("spectral_studies.id"))
 status:Mapped[str]=mapped_column(String)
 result:Mapped[dict]=mapped_column(JSON,default=dict)
 error:Mapped[str]=mapped_column(Text,default="")
 created_at:Mapped[str]=mapped_column(String)
 attempt:Mapped[int]=mapped_column(Integer,default=0)
 owner:Mapped[str]=mapped_column(String,default="")
 lease_expires_at:Mapped[str]=mapped_column(String,default="")
 heartbeat_at:Mapped[str]=mapped_column(String,default="")
 started_at:Mapped[str]=mapped_column(String,default="")
 finished_at:Mapped[str]=mapped_column(String,default="")
 canceled_at:Mapped[str]=mapped_column(String,default="")
 canceled_by:Mapped[str]=mapped_column(String,default="")
 retry_of:Mapped[str]=mapped_column(String,default="")
