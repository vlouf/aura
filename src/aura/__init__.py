"""
AURA - Australian Unified Radar Archive Python Interface

A library for efficiently accessing ODIM HDF5 radar data from the NCI AURA archive.

Example usage:
    import aura

    # Get all volumes for a day
    volumes = aura.get_vol(2, date(2025, 10, 16))
    for vol in volumes:
        data = vol.read()  # Lazy load - only extracts when needed
        process(data)

    # Get the nearest volume to a specific time
    vol = aura.get_vol(2, datetime(2025, 10, 16, 12, 30), nearest=True)
    data = vol.read()

    # Get volumes for a date range
    volumes = aura.get_vol(2, date(2025, 10, 1), date(2025, 10, 7))

    # Get radar information
    radar = aura.get_radar(2)
    print(radar.name, radar.location, radar.lat, radar.lon)

    # Check data availability
    dates = aura.available_dates(2, year=2025)
"""

from aura.core import get_vol, read_vol
from aura.volume import LazyVolume, VolumeList
from aura.radar import get_radar, list_radars, RadarInfo
from aura.availability import available_dates, available_years

__version__ = "0.1.0"

__all__ = [
    # Main API
    "get_vol",
    "read_vol",
    # Volume classes
    "LazyVolume",
    "VolumeList",
    # Radar metadata
    "get_radar",
    "list_radars",
    "RadarInfo",
    # Availability
    "available_dates",
    "available_years",
    # Version
    "__version__",
]
