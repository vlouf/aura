"""
Data availability checking.

This module provides functions to check what radar data is available
in the AURA archive without loading the actual data.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from aura.config import radar_path, year_path, vol_path, ZIP_PATTERN


def available_years(radar_id: int | str) -> List[int]:
    """
    Get a list of years with available data for a radar.
    
    Parameters
    ----------
    radar_id : int or str
        Radar identifier.
        
    Returns
    -------
    List[int]
        Sorted list of years with data available.
        
    Examples
    --------
    >>> years = aura.available_years(2)
    >>> print(years)
    [1993, 1994, 1995, ..., 2024, 2025]
    """
    base = radar_path(radar_id)
    
    if not base.exists():
        return []
    
    years = []
    for item in base.iterdir():
        if item.is_dir():
            try:
                year = int(item.name)
                # Check that vol/ directory exists
                if (item / "vol").exists():
                    years.append(year)
            except ValueError:
                continue
    
    return sorted(years)


def available_dates(
    radar_id: int | str,
    year: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[date]:
    """
    Get a list of dates with available data for a radar.
    
    Parameters
    ----------
    radar_id : int or str
        Radar identifier.
    year : int, optional
        Limit to a specific year (faster than searching all years).
    start_date : date, optional
        Only include dates on or after this date.
    end_date : date, optional
        Only include dates on or before this date.
        
    Returns
    -------
    List[date]
        Sorted list of dates with data available.
        
    Examples
    --------
    >>> # All dates in 2025
    >>> dates = aura.available_dates(2, year=2025)
    >>> 
    >>> # Date range
    >>> dates = aura.available_dates(2, 
    ...     start_date=date(2025, 1, 1),
    ...     end_date=date(2025, 1, 31))
    """
    dates = []
    
    # Determine which years to search
    if year is not None:
        years = [year]
    elif start_date is not None and end_date is not None:
        years = list(range(start_date.year, end_date.year + 1))
    elif start_date is not None:
        # From start_date to present
        from datetime import datetime
        years = list(range(start_date.year, datetime.now().year + 1))
    elif end_date is not None:
        # All years up to end_date
        all_years = available_years(radar_id)
        years = [y for y in all_years if y <= end_date.year]
    else:
        years = available_years(radar_id)
    
    # Search each year's vol/ directory
    for yr in years:
        vol_dir = vol_path(radar_id, yr)
        if not vol_dir.exists():
            continue
        
        for zip_file in vol_dir.glob("*.pvol.zip"):
            match = ZIP_PATTERN.match(zip_file.name)
            if match:
                date_str = match.group(2)  # YYYYMMDD
                try:
                    d = date(
                        int(date_str[:4]),
                        int(date_str[4:6]),
                        int(date_str[6:8])
                    )
                    
                    # Apply date filters
                    if start_date and d < start_date:
                        continue
                    if end_date and d > end_date:
                        continue
                    
                    dates.append(d)
                except ValueError:
                    continue
    
    return sorted(dates)


def has_data(radar_id: int | str, day: date) -> bool:
    """
    Check if data exists for a specific radar and date.
    
    This is faster than available_dates() for checking a single date.
    
    Parameters
    ----------
    radar_id : int or str
        Radar identifier.
    day : date
        The date to check.
        
    Returns
    -------
    bool
        True if data exists for this date.
        
    Examples
    --------
    >>> if aura.has_data(2, date(2025, 10, 16)):
    ...     volumes = aura.get_vol(2, date(2025, 10, 16))
    """
    from aura.config import zip_path
    return zip_path(radar_id, day.year, day.month, day.day).exists()


def data_summary(radar_id: int | str) -> dict:
    """
    Get a summary of available data for a radar.
    
    Parameters
    ----------
    radar_id : int or str
        Radar identifier.
        
    Returns
    -------
    dict
        Summary with keys:
        - radar_id: int
        - years: List[int] - available years
        - first_date: date or None - earliest available date
        - last_date: date or None - most recent available date
        - total_days: int - total number of days with data
        
    Examples
    --------
    >>> summary = aura.data_summary(2)
    >>> print(f"Data from {summary['first_date']} to {summary['last_date']}")
    >>> print(f"Total: {summary['total_days']} days")
    """
    rid = int(radar_id)
    years = available_years(rid)
    
    if not years:
        return {
            "radar_id": rid,
            "years": [],
            "first_date": None,
            "last_date": None,
            "total_days": 0,
        }
    
    # Get first and last dates efficiently
    first_year = years[0]
    last_year = years[-1]
    
    first_dates = available_dates(rid, year=first_year)
    first_date = first_dates[0] if first_dates else None
    
    last_dates = available_dates(rid, year=last_year)
    last_date = last_dates[-1] if last_dates else None
    
    # Count total days (this could be slow for radars with many years)
    total_days = sum(len(available_dates(rid, year=y)) for y in years)
    
    return {
        "radar_id": rid,
        "years": years,
        "first_date": first_date,
        "last_date": last_date,
        "total_days": total_days,
    }
