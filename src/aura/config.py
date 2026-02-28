"""
Configuration and constants for AURA.

This module defines the base paths and patterns for the AURA archive on NCI Gadi.
"""

from pathlib import Path
from typing import Optional
import os

# Base paths for the AURA archive
# Operational radars (rq0 project)
RQ0_BASE = Path("/g/data/rq0/level_1/odim_pvol")

# Research radars (hj10 project) - for future use
HJ10_BASE = Path("/g/data/hj10")

# Default base path (can be overridden for testing or alternative mounts)
_base_path: Optional[Path] = None


def get_base_path() -> Path:
    """Get the current base path for the AURA archive."""
    if _base_path is not None:
        return _base_path
    # Allow override via environment variable
    env_path = os.environ.get("AURA_BASE_PATH")
    if env_path:
        return Path(env_path)
    return RQ0_BASE


def set_base_path(path: Path | str | None) -> None:
    """
    Set a custom base path for the AURA archive.

    Useful for testing or when the archive is mounted at a different location.
    Pass None to reset to default.

    Parameters
    ----------
    path : Path, str, or None
        The base path to use, or None to reset to default.
    """
    global _base_path
    if path is None:
        _base_path = None
    else:
        _base_path = Path(path)


# File naming patterns
# Zip file: {radar_id}_{YYYYMMDD}.pvol.zip
# Volume file inside zip: {radar_id}_{YYYYMMDD}_{HHMMSS}.pvol.h5

# Regex patterns for parsing
import re

# Pattern for zip filenames: captures radar_id and date
ZIP_PATTERN = re.compile(r"^(\d+)_(\d{8})\.pvol\.zip$")

# Pattern for volume filenames inside zip: captures radar_id, date, and time
VOLUME_PATTERN = re.compile(r"^(\d+)_(\d{8})_(\d{6})\.pvol\.h5$")

# Date/time formats
DATE_FORMAT = "%Y%m%d"
TIME_FORMAT = "%H%M%S"
DATETIME_FORMAT = f"{DATE_FORMAT}_{TIME_FORMAT}"


def radar_path(radar_id: int | str) -> Path:
    """Get the base path for a specific radar."""
    # Radar IDs in the path don't have leading zeros
    rid = str(int(radar_id))
    return get_base_path() / rid


def year_path(radar_id: int | str, year: int) -> Path:
    """Get the path for a specific radar and year."""
    return radar_path(radar_id) / str(year)


def vol_path(radar_id: int | str, year: int) -> Path:
    """Get the path to the vol/ directory for a specific radar and year."""
    return year_path(radar_id, year) / "vol"


def zip_path(radar_id: int | str, year: int, month: int, day: int) -> Path:
    """Get the path to a specific daily zip file."""
    rid = int(radar_id)
    date_str = f"{year:04d}{month:02d}{day:02d}"
    return vol_path(radar_id, year) / f"{rid}_{date_str}.pvol.zip"


def radar_site_list_path() -> Path:
    """Get the path to the radar site list CSV."""
    return get_base_path() / "radar_site_list.csv"
