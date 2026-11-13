from datetime import datetime,timezone,timedelta
def now(): return datetime.now(timezone.utc).isoformat()
def stamp(): return datetime.now(timezone.utc).isoformat(timespec="microseconds")
def lease_deadline(seconds): return (datetime.now(timezone.utc)+timedelta(seconds=seconds)).isoformat(timespec="microseconds")
