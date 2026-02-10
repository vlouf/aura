"""
Radar metadata and site information.

This module provides access to radar site information from the AURA archive's
radar_site_list.csv file.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

from aura.config import radar_site_list_path


@dataclass
class RadarInfo:
    """
    Information about a radar site.
    
    Note that radar sites can have multiple configurations over time
    (e.g., upgrades from C-band to S-band, or addition of dual-polarization).
    Each configuration is represented as a separate RadarInfo instance.
    
    Attributes
    ----------
    id : int
        Numeric radar identifier (1-3 digits).
    id_long : str
        Long-form ID with configuration suffix (e.g., "002_3").
    wigos : str
        WMO Integrated Global Observing System identifier.
    short_name : str
        Short name/code for the radar.
    location : str
        Human-readable location name.
    radar_type : str
        Radar model/type (e.g., "Meteor1500S", "DWSR81C").
    start_date : date or None
        Start date for this configuration (postchange_start).
    end_date : date or None
        End date for this configuration (prechange_end), None if current.
    lat : float
        Latitude in decimal degrees.
    lon : float
        Longitude in decimal degrees.
    ground_altitude : float
        Ground elevation in meters.
    site_altitude : float
        Antenna height in meters above sea level.
    status : str
        Current status (e.g., "OK").
    band : str
        Radar band ("C" or "S").
    doppler : bool
        Whether the radar has Doppler capability.
    dual_pol : bool
        Whether the radar has dual-polarization capability.
    beamwidth : float
        Antenna beamwidth in degrees.
    state : str
        Australian state/territory.
    notes : str
        Additional notes.
    """
    
    id: int
    id_long: str
    wigos: str
    short_name: str
    location: str
    radar_type: str
    start_date: Optional[date]
    end_date: Optional[date]
    lat: float
    lon: float
    ground_altitude: float
    site_altitude: float
    status: str
    band: str
    doppler: bool
    dual_pol: bool
    beamwidth: float
    state: str
    notes: str
    
    @property
    def name(self) -> str:
        """Alias for location."""
        return self.location
    
    @property
    def is_current(self) -> bool:
        """Whether this configuration is currently active."""
        return self.end_date is None
    
    @property
    def is_sband(self) -> bool:
        """Whether this is an S-band radar."""
        return self.band.upper() == "S"
    
    @property
    def is_cband(self) -> bool:
        """Whether this is a C-band radar."""
        return self.band.upper() == "C"
    
    def active_at(self, dt: date | datetime) -> bool:
        """
        Check if this radar configuration was active at a given date.
        
        Parameters
        ----------
        dt : date or datetime
            The date to check.
            
        Returns
        -------
        bool
            True if this configuration was active at the given date.
        """
        if isinstance(dt, datetime):
            dt = dt.date()
        
        if self.start_date and dt < self.start_date:
            return False
        if self.end_date and dt > self.end_date:
            return False
        return True
    
    def __repr__(self) -> str:
        return (
            f"RadarInfo(id={self.id}, name='{self.location}', "
            f"type='{self.radar_type}', band='{self.band}')"
        )


def _parse_date(date_str: str) -> Optional[date]:
    """Parse a date string from the CSV (DD/MM/YYYY format) or return None."""
    if not date_str or date_str == "-":
        return None
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        return None


def _parse_bool(value: str) -> bool:
    """Parse a boolean field (Yes/empty)."""
    return value.lower() == "yes" if value else False


def _parse_float(value: str, default: float = 0.0) -> float:
    """Parse a float field with default."""
    try:
        return float(value) if value else default
    except ValueError:
        return default


@lru_cache(maxsize=1)
def _load_radar_site_list() -> List[RadarInfo]:
    """
    Load and parse the radar site list CSV.
    
    Results are cached after first load.
    """
    csv_path = radar_site_list_path()
    
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Radar site list not found at {csv_path}. "
            "Make sure you have access to the rq0 project on NCI."
        )
    
    radars = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                radar = RadarInfo(
                    id=int(row["id"]),
                    id_long=row["id_long"],
                    wigos=row.get("WIGOS", ""),
                    short_name=row["short_name"],
                    location=row["location"],
                    radar_type=row["radar_type"],
                    start_date=_parse_date(row.get("postchange_start", "")),
                    end_date=_parse_date(row.get("prechange_end", "")),
                    lat=_parse_float(row["site_lat"]),
                    lon=_parse_float(row["site_lon"]),
                    ground_altitude=_parse_float(row.get("ge_ground_altitude", "")),
                    site_altitude=_parse_float(row["site_alt"]),
                    status=row.get("status", ""),
                    band=row.get("band", ""),
                    doppler=_parse_bool(row.get("doppler", "")),
                    dual_pol=_parse_bool(row.get("dp", "")),
                    beamwidth=_parse_float(row.get("beamwidth", ""), default=1.0),
                    state=row.get("state", ""),
                    notes=row.get("notes", ""),
                )
                radars.append(radar)
            except (KeyError, ValueError) as e:
                # Skip malformed rows but log for debugging
                import warnings
                warnings.warn(f"Skipping malformed row in radar site list: {e}")
                continue
    
    return radars


def list_radars(
    current_only: bool = False,
    band: Optional[str] = None,
    state: Optional[str] = None,
    dual_pol_only: bool = False,
) -> List[RadarInfo]:
    """
    List all radars in the archive.
    
    Parameters
    ----------
    current_only : bool
        If True, only return currently active configurations.
    band : str, optional
        Filter by radar band ("C" or "S").
    state : str, optional
        Filter by Australian state (e.g., "VIC", "NSW").
    dual_pol_only : bool
        If True, only return dual-polarization radars.
        
    Returns
    -------
    List[RadarInfo]
        List of radar information objects.
        
    Examples
    --------
    >>> # Get all current S-band radars
    >>> sband = aura.list_radars(current_only=True, band="S")
    >>> 
    >>> # Get all dual-pol radars in Victoria
    >>> vic_dp = aura.list_radars(state="VIC", dual_pol_only=True)
    """
    radars = _load_radar_site_list()
    
    if current_only:
        radars = [r for r in radars if r.is_current]
    if band:
        radars = [r for r in radars if r.band.upper() == band.upper()]
    if state:
        radars = [r for r in radars if r.state.upper() == state.upper()]
    if dual_pol_only:
        radars = [r for r in radars if r.dual_pol]
    
    return radars


def get_radar(
    radar_id: int | str,
    at_date: Optional[date | datetime] = None,
) -> RadarInfo:
    """
    Get information about a specific radar.
    
    If a radar has multiple configurations (e.g., upgrades), this returns
    the configuration that was active at the specified date, or the most
    recent configuration if no date is specified.
    
    Parameters
    ----------
    radar_id : int or str
        Radar identifier (e.g., 2 or "2" or "002").
    at_date : date or datetime, optional
        Return the configuration active at this date.
        If None, returns the most recent (current) configuration.
        
    Returns
    -------
    RadarInfo
        Information about the radar.
        
    Raises
    ------
    ValueError
        If no radar with the given ID exists.
        
    Examples
    --------
    >>> radar = aura.get_radar(2)
    >>> print(f"{radar.location}: {radar.radar_type} ({radar.band}-band)")
    Melbourne: Meteor1500S (S-band)
    >>> 
    >>> # Get historical configuration
    >>> old_radar = aura.get_radar(2, at_date=date(2010, 1, 1))
    >>> print(old_radar.radar_type)
    Meteor1500S
    """
    rid = int(radar_id)
    radars = _load_radar_site_list()
    
    # Filter to this radar ID
    matches = [r for r in radars if r.id == rid]
    
    if not matches:
        raise ValueError(f"No radar found with ID {rid}")
    
    if at_date is not None:
        # Find configuration active at the given date
        active = [r for r in matches if r.active_at(at_date)]
        if active:
            return active[0]  # Should be only one
        # Fall back to nearest configuration
        import warnings
        warnings.warn(
            f"No active configuration found for radar {rid} at {at_date}. "
            "Returning most recent configuration."
        )
    
    # Return the current (or most recent) configuration
    current = [r for r in matches if r.is_current]
    if current:
        return current[0]
    
    # If no current config, return the one with the latest start date
    return max(matches, key=lambda r: r.start_date or date.min)


def get_radar_ids() -> List[int]:
    """
    Get a list of all unique radar IDs in the archive.
    
    Returns
    -------
    List[int]
        Sorted list of radar IDs.
    """
    radars = _load_radar_site_list()
    return sorted(set(r.id for r in radars))
