from datetime import datetime, timezone
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")


def utc_to_eastern(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(EASTERN)


def parse_iso_z_to_eastern(iso_str: str) -> datetime:
    utc_dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
    return utc_to_eastern(utc_dt)
