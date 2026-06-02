
from datetime import datetime, timezone

REQUIRED=["event_id","user_id","track_id","source_peer","timestamp","duration_ms","completed","device_type","geo_country","event_source"]

def is_valid_listening_event(event):
    for k in REQUIRED:
        if k not in event or event.get(k) is None:
            return False
    try:
        ts=event["timestamp"].replace("Z","+00:00")
        dt=datetime.fromisoformat(ts)
        if dt > datetime.now(timezone.utc):
            return False
    except Exception:
        return False
    if event.get("duration_ms",0) < 5000 and event.get("completed") is False:
        return False
    return True
