
from typing import Any, Dict, List

MAX_DURATION_MS = 36_000_000

def normalize_artist_name(name):
    if name is None:
        return None
    return str(name).strip().title()

def validate_track_schema(track: Dict[str, Any]) -> List[str]:
    errors=[]
    required=["id","artist_id","title","duration_ms","genre"]
    for field in required:
        if field not in track or track.get(field) in (None,""):
            errors.append(f"missing_{field}")
    duration=track.get("duration_ms")
    if duration is not None:
        try:
            d=int(duration)
            if d <=0:
                errors.append("invalid_duration_negative")
            if d > MAX_DURATION_MS:
                errors.append("invalid_duration_too_long")
        except Exception:
            errors.append("invalid_duration_type")
    return errors

def deduplicate_artists(artists):
    seen=set()
    result=[]
    for artist in artists:
        key=(normalize_artist_name(artist.get("name")), artist.get("label"))
        if key not in seen:
            seen.add(key)
            new=dict(artist)
            if artist.get("name") is not None:
                new["name"]=normalize_artist_name(artist.get("name"))
            result.append(new)
    return result
