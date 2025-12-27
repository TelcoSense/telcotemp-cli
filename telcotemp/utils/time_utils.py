from astral import LocationInfo
from astral.sun import sun
import pytz


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
    location = LocationInfo(latitude=lat, longitude=lng)
    tz = pytz.timezone(tz_str)
    
    # Ensure timestamp is timezone-aware
    if timestamp.tzinfo is None:
        timestamp = tz.localize(timestamp)
    else:
        timestamp = timestamp.astimezone(tz)
    
    # Get sunrise and sunset for the day
    s = sun(location.observer, date=timestamp.date(), tzinfo=tz)
    sunrise = s["sunrise"]
    sunset = s["sunset"]
    
    return 1 if sunrise <= timestamp <= sunset else 0
