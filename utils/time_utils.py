from astral.sun import sun
from astral import LocationInfo
import pytz
from datetime import timedelta, datetime
from time import sleep


def wait_for_next_hour():
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    sleep((next_hour - now).seconds)


def is_daylight(ts_utc, lat, lng, local_tz_str):
    if ts_utc.tzinfo is None or ts_utc.tzinfo.utcoffset(ts_utc) is None:
        raise ValueError("ts_utc must be timezone-aware and in UTC")

    local_tz = pytz.timezone(local_tz_str)
    local_date = ts_utc.astimezone(local_tz).date()
    loc = LocationInfo(timezone=local_tz_str, latitude=lat, longitude=lng)

    s = sun(loc.observer, date=local_date, tzinfo=local_tz)  # Correct usage
    sunrise_utc = s["sunrise"].astimezone(pytz.UTC)
    sunset_utc = s["sunset"].astimezone(pytz.UTC)

    return 1 if sunrise_utc <= ts_utc <= sunset_utc else 0
