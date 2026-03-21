from astral import LocationInfo
from astral.sun import sun
from functools import lru_cache
import pytz


@lru_cache(maxsize=4096)
def _get_daylight_bounds(date_key, lat, lng, tz_str):
    location = LocationInfo(latitude=lat, longitude=lng)
    tz = pytz.timezone(tz_str)
    s = sun(location.observer, date=date_key, tzinfo=tz)
    return s["sunrise"], s["sunset"]


def is_daylight(timestamp, lat, lng, tz_str):
    """
    Determine if given timestamp is during daylight hours.
    
    Args:
        timestamp: datetime object (aware or naive)
        lat: latitude
        lng: longitude
        tz_str: timezone string (e.g., "Europe/Prague")
    
    Returns:
        1 if daylight, 0 if night
    """
    tz = pytz.timezone(tz_str)
    
    # Ensure timestamp is timezone-aware
    if timestamp.tzinfo is None:
        timestamp = tz.localize(timestamp)
    else:
        timestamp = timestamp.astimezone(tz)
    
    sunrise, sunset = _get_daylight_bounds(timestamp.date(), lat, lng, tz_str)
    
    return 1 if sunrise <= timestamp <= sunset else 0
