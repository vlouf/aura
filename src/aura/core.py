"""
Core API functions for accessing AURA radar data.

This module provides the main entry points for the AURA library:
- get_vol(): Get lazy volume references
- read_vol(): Directly read a volume (convenience wrapper)
"""

from __future__ import annotations

import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Union, overload

from aura.config import zip_path, vol_path, VOLUME_PATTERN
from aura.volume import LazyVolume, VolumeList


def _ensure_utc(dt: datetime) -> datetime:
    """
    Ensure a datetime is in UTC.
    
    If the datetime is naive (no timezone), assume it's already UTC.
    If it has a timezone, convert to UTC.
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt
    else:
        # Convert to UTC and make naive for comparison
        return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _list_volumes_in_zip(zip_file: Path) -> List[LazyVolume]:
    """
    List all volume files in a zip archive without extracting.
    
    Parameters
    ----------
    zip_file : Path
        Path to the zip file.
        
    Returns
    -------
    List[LazyVolume]
        List of LazyVolume objects for each volume in the zip.
    """
    if not zip_file.exists():
        return []
    
    volumes = []
    with zipfile.ZipFile(zip_file, "r") as zf:
        for name in zf.namelist():
            # Only process .h5 files matching the expected pattern
            if VOLUME_PATTERN.match(name):
                try:
                    vol = LazyVolume.from_filename(zip_file, name)
                    volumes.append(vol)
                except ValueError:
                    # Skip files that don't match the pattern
                    continue
    
    # Sort by timestamp
    volumes.sort(key=lambda v: v.timestamp)
    return volumes


def _get_volumes_for_date(radar_id: int | str, day: date) -> VolumeList:
    """Get all volumes for a specific date."""
    rid = int(radar_id)
    zp = zip_path(rid, day.year, day.month, day.day)
    volumes = _list_volumes_in_zip(zp)
    return VolumeList(volumes)


def _get_volumes_for_range(
    radar_id: int | str,
    start: date,
    end: date,
) -> VolumeList:
    """Get all volumes for a date range (inclusive)."""
    rid = int(radar_id)
    all_volumes = []
    
    # Iterate through each day in the range
    current = start
    while current <= end:
        day_volumes = _get_volumes_for_date(rid, current)
        all_volumes.extend(day_volumes)
        current += timedelta(days=1)
    
    return VolumeList(all_volumes)


# Type overloads for better IDE support
@overload
def get_vol(
    radar_id: int | str,
    time: datetime,
    *,
    nearest: bool = True,
) -> LazyVolume: ...


@overload
def get_vol(
    radar_id: int | str,
    time: datetime,
    *,
    nearest: bool = False,
    tolerance_seconds: float = 60.0,
) -> LazyVolume: ...


@overload
def get_vol(
    radar_id: int | str,
    day: date,
) -> VolumeList: ...


@overload
def get_vol(
    radar_id: int | str,
    start: date,
    end: date,
) -> VolumeList: ...


def get_vol(
    radar_id: int | str,
    time_or_date: Union[datetime, date],
    end_date: Optional[date] = None,
    *,
    nearest: bool = False,
    tolerance_seconds: float = 60.0,
) -> Union[LazyVolume, VolumeList]:
    """
    Get radar volume(s) from the AURA archive.
    
    This function has multiple calling signatures:
    
    1. **Single datetime with nearest=True**: Returns the volume closest to the
       specified time.
    2. **Single datetime with nearest=False**: Returns the volume at exactly
       that time (within tolerance), or raises an error.
    3. **Single date**: Returns all volumes for that day.
    4. **Date range**: Returns all volumes between start and end dates (inclusive).
    
    All returned volumes are "lazy" - they don't load any data until you call
    `.read()` on them. This makes it efficient to work with large collections.
    
    Parameters
    ----------
    radar_id : int or str
        Radar identifier (e.g., 2 or "2" or "002").
    time_or_date : datetime or date
        - If datetime: specific time to find a volume for.
        - If date: day to get all volumes for (or start of date range).
    end_date : date, optional
        End date for a date range query (inclusive).
    nearest : bool, default False
        If True and time_or_date is a datetime, return the nearest volume
        even if it's not exactly at that time.
    tolerance_seconds : float, default 60.0
        When nearest=False, maximum allowed difference in seconds between
        the requested time and the volume timestamp.
        
    Returns
    -------
    LazyVolume or VolumeList
        - LazyVolume: When querying for a specific datetime.
        - VolumeList: When querying for a date or date range.
        
    Raises
    ------
    FileNotFoundError
        If no data exists for the specified radar/date.
    ValueError
        If nearest=False and no volume exists within the tolerance.
        
    Examples
    --------
    >>> import aura
    >>> from datetime import date, datetime
    >>> 
    >>> # Get all volumes for a day
    >>> volumes = aura.get_vol(2, date(2025, 10, 16))
    >>> print(f"Found {len(volumes)} volumes")
    >>> for vol in volumes:
    ...     data = vol.read()  # Lazy - only loads when called
    ...     process(data)
    >>> 
    >>> # Get the nearest volume to a specific time
    >>> vol = aura.get_vol(2, datetime(2025, 10, 16, 12, 30), nearest=True)
    >>> data = vol.read()
    >>> 
    >>> # Get volumes for a date range
    >>> volumes = aura.get_vol(2, date(2025, 10, 1), date(2025, 10, 7))
    >>> 
    >>> # Works with timezone-aware datetimes (converted to UTC)
    >>> from datetime import timezone
    >>> aest = timezone(timedelta(hours=10))
    >>> local_time = datetime(2025, 10, 16, 22, 30, tzinfo=aest)
    >>> vol = aura.get_vol(2, local_time, nearest=True)
    """
    rid = int(radar_id)
    
    # Helper to check if something is a pure date (not datetime/Timestamp)
    # We check exact type because datetime is a subclass of date,
    # and pd.Timestamp is a subclass of datetime
    def _is_date_only(obj) -> bool:
        return type(obj) is date
    
    # Case 1: Date range (start_date, end_date)
    if end_date is not None:
        if not _is_date_only(time_or_date):
            raise TypeError(
                "When end_date is provided, first argument must be a date (not datetime). "
                "Use datetime.date() or pass a datetime.date object."
            )
        return _get_volumes_for_range(rid, time_or_date, end_date)
    
    # Case 2: Single date (all volumes for that day)
    # Only matches exact date type, not datetime or pd.Timestamp
    if _is_date_only(time_or_date):
        volumes = _get_volumes_for_date(rid, time_or_date)
        if not volumes:
            zp = zip_path(rid, time_or_date.year, time_or_date.month, time_or_date.day)
            raise FileNotFoundError(
                f"No data found for radar {rid} on {time_or_date}. "
                f"Expected zip file: {zp}"
            )
        return volumes
    
    # Case 3: Specific datetime (includes datetime, pd.Timestamp, etc.)
    if isinstance(time_or_date, datetime):
        dt = _ensure_utc(time_or_date)
        day = dt.date()
        
        # Check if this looks like a "date-only" timestamp (midnight with no explicit time)
        # This handles cases like pd.Timestamp("2026-01-09") which creates midnight
        is_midnight = dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0
        
        # If it's exactly midnight and nearest=False (default), treat as "get all for day"
        # This makes pd.Timestamp("2026-01-09") work intuitively
        if is_midnight and not nearest:
            volumes = _get_volumes_for_date(rid, day)
            if not volumes:
                zp = zip_path(rid, day.year, day.month, day.day)
                raise FileNotFoundError(
                    f"No data found for radar {rid} on {day}. "
                    f"Expected zip file: {zp}"
                )
            return volumes
        
        # Get all volumes for that day
        volumes = _get_volumes_for_date(rid, day)
        
        if not volumes:
            zp = zip_path(rid, day.year, day.month, day.day)
            raise FileNotFoundError(
                f"No data found for radar {rid} on {day}. "
                f"Expected zip file: {zp}"
            )
        
        if nearest:
            # Return the nearest volume
            return volumes.nearest(dt)
        else:
            # Try to find exact match within tolerance
            vol = volumes.at(dt, tolerance_seconds=tolerance_seconds)
            if vol is None:
                nearest_vol = volumes.nearest(dt)
                diff = abs((nearest_vol.timestamp - dt).total_seconds())
                raise ValueError(
                    f"No volume found within {tolerance_seconds}s of {dt.isoformat()}. "
                    f"Nearest volume is at {nearest_vol.timestamp.isoformat()} "
                    f"({diff:.0f}s away). Use nearest=True to get the nearest volume."
                )
            return vol
    
    raise TypeError(
        f"time_or_date must be a date or datetime, got {type(time_or_date).__name__}"
    )


def read_vol(
    radar_id: int | str,
    time: datetime,
    *,
    nearest: bool = True,
    **read_kwargs,
):
    """
    Read a single radar volume directly.
    
    This is a convenience function that combines get_vol() and .read().
    For processing multiple volumes, use get_vol() to get lazy references
    and call .read() only when needed.
    
    Parameters
    ----------
    radar_id : int or str
        Radar identifier.
    time : datetime
        Time of the volume to read.
    nearest : bool, default True
        If True, read the nearest volume to the specified time.
        If False, raise an error if no exact match exists.
    **read_kwargs
        Additional arguments passed to pyodim.read_odim().
        
    Returns
    -------
    xr.Dataset or List[xr.Dataset]
        The radar data.
        
    Examples
    --------
    >>> data = aura.read_vol(2, datetime(2025, 10, 16, 12, 30))
    >>> print(data)
    """
    vol = get_vol(radar_id, time, nearest=nearest)
    return vol.read(**read_kwargs)
