"""
Lazy volume loading classes.

This module provides LazyVolume and VolumeList classes that allow efficient
access to radar volumes without extracting them from zip archives until needed.
"""

from __future__ import annotations

import io
import warnings
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Callable, List, Optional, TypeVar, overload

import h5py
import xarray as xr

from aura.config import VOLUME_PATTERN, DATETIME_FORMAT

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class LazyVolume:
    """
    A lazy reference to a radar volume file inside a zip archive.

    This class holds metadata about a volume file but does not extract or read
    the data until explicitly requested. This allows efficient iteration over
    large collections of volumes.

    The LazyVolume is immutable and picklable, making it safe to use with
    multiprocessing, dask.bag, and other parallel processing frameworks.

    Attributes
    ----------
    zip_path : Path
        Path to the zip archive containing this volume.
    filename : str
        Name of the volume file inside the zip (e.g., "2_20251016_123456.pvol.h5").
    radar_id : int
        Radar identifier.
    timestamp : datetime
        UTC timestamp of the volume scan.

    Examples
    --------
    >>> vol = LazyVolume(...)
    >>> # Data is NOT loaded yet
    >>> data = vol.read()  # NOW the data is extracted and loaded
    >>>
    >>> # Works with parallel processing
    >>> from multiprocessing import Pool
    >>> with Pool(4) as p:
    ...     results = p.map(lambda v: process(v.read()), volumes)
    """

    zip_path: Path
    filename: str
    radar_id: int
    timestamp: datetime

    @classmethod
    def from_filename(cls, zip_path: Path, filename: str) -> "LazyVolume":
        """
        Create a LazyVolume from a zip path and filename.

        Parameters
        ----------
        zip_path : Path
            Path to the zip archive.
        filename : str
            Name of the file inside the zip.

        Returns
        -------
        LazyVolume
            A new LazyVolume instance.

        Raises
        ------
        ValueError
            If the filename doesn't match the expected pattern.
        """
        match = VOLUME_PATTERN.match(filename)
        if not match:
            raise ValueError(f"Invalid volume filename: {filename}")

        radar_id = int(match.group(1))
        date_str = match.group(2)
        time_str = match.group(3)
        timestamp = datetime.strptime(f"{date_str}_{time_str}", DATETIME_FORMAT)

        return cls(
            zip_path=zip_path,
            filename=filename,
            radar_id=radar_id,
            timestamp=timestamp,
        )

    def read(self, sweeps: int | list[int] | None = None, **kwargs) -> List[xr.Dataset]:
        """
        Read the volume data using pyodim.

        Reads directly from the zip into an in-memory buffer and hands it to
        ``pyodim.read_odim``; no temp file ever touches disk.

        Parameters
        ----------
        sweeps : int or list of int, optional
            Sweep index (or indices) to read, in elevation order. All sweeps if omitted.
        **kwargs
            Additional options passed to ``pyodim.read_odim`` /
            ``pyodim.read_sweep``, e.g. ``include_fields``, ``exclude_fields``,
            ``mask_undetect``, ``georef``.

        Returns
        -------
        List[xr.Dataset]
            Radar sweeps ordered by elevation, fully materialised (pyodim >= 0.7
            is eager by default).
        """
        try:
            from pyodim import read_odim
        except ImportError:
            raise ImportError("pyodim>=0.7 is required to read ODIM HDF5 files. Install it with: pip install pyodim")

        if "nslice" in kwargs:  # pyodim < 0.7 name
            warnings.warn("`nslice` is deprecated, use `sweeps`.", DeprecationWarning, stacklevel=2)
            sweeps = kwargs.pop("nslice")
        if kwargs.pop("lazy", False):
            raise ValueError("Volume.read() reads from an in-memory buffer and is always eager; `lazy` is not supported.")

        # Read zip entry into memory
        with zipfile.ZipFile(self.zip_path, "r") as zf:
            with zf.open(self.filename) as f:
                data = f.read()

        # h5py (and therefore pyodim) accepts a file-like object
        return read_odim(io.BytesIO(data), sweeps=sweeps, **kwargs)

    def read_h5py(self) -> h5py.File:
        """
        Read the raw HDF5 file using h5py (without pyodim processing).

        This provides direct access to the HDF5 structure for advanced users
        who need to access metadata or non-standard fields.

        Returns
        -------
        h5py.File
            The opened HDF5 file. Note: The file is backed by in-memory data,
            so it remains valid after this method returns.

        Warning
        -------
        The returned h5py.File should be closed when done to free memory.
        Consider using a context manager pattern.
        """
        with zipfile.ZipFile(self.zip_path, "r") as zf:
            with zf.open(self.filename) as f:
                data = f.read()

        # Use h5py's core driver to read from memory
        # This keeps everything in RAM without temp files
        return h5py.File(io.BytesIO(data), "r")

    def plot(self, sweep: int = 0, moments: list[str] | None = None, **kwargs):
        """
        Plot radar moments of one sweep of the volume.

        Only the requested sweep (and moments, if given) is read.

        Parameters
        ----------
        sweep : int
            Sweep index, in elevation order (default 0, the lowest).
        moments : list of str, optional
            Moments to plot. Defaults to the first AURA default moment set available.
        **kwargs
            Additional options passed to ``aura.plots.plot``, e.g. ``xlims``,
            ``ylims``, ``nrings``, ``ncols``.

        Returns
        -------
        matplotlib.figure.Figure
            The figure, to display or save.
        """
        try:
            from aura.plots import plot
        except ImportError:
            raise ImportError("matplotlib and cmweather are required for plotting. Install with: pip install aura[plot]")

        read_kwargs = {"include_fields": list(moments)} if moments is not None else {}
        radar = self.read(sweeps=sweep, **read_kwargs)
        return plot(radar[0], moments=moments, **kwargs)

    def extract_to(self, directory: Path | str) -> Path:
        """
        Extract the volume file to a directory on disk.

        Useful when you need to pass the file to external tools that
        require a filesystem path.

        Parameters
        ----------
        directory : Path or str
            Directory to extract the file to.

        Returns
        -------
        Path
            Path to the extracted file.
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        output_path = directory / self.filename

        with zipfile.ZipFile(self.zip_path, "r") as zf:
            zf.extract(self.filename, directory)

        return output_path

    def __repr__(self) -> str:
        return f"LazyVolume(radar={self.radar_id}, " f"time={self.timestamp.isoformat()}, " f"file={self.filename})"

    def __str__(self) -> str:
        return f"{self.filename} @ {self.timestamp.isoformat()}"

    # Make it work nicely with Path-like APIs
    @property
    def name(self) -> str:
        """Filename (for Path-like compatibility)."""
        return self.filename

    @property
    def stem(self) -> str:
        """Filename without extension."""
        return Path(self.filename).stem

    @property
    def suffix(self) -> str:
        """File extension."""
        return Path(self.filename).suffix


class VolumeList(Sequence[LazyVolume]):
    """
    A collection of LazyVolume objects with convenience methods.

    VolumeList behaves like a regular list but provides additional methods
    for filtering, searching, and bulk operations on radar volumes.

    Supports all standard sequence operations: indexing, slicing, iteration,
    len(), and membership testing.

    Attributes
    ----------
    volumes : List[LazyVolume]
        The underlying list of lazy volumes.

    Examples
    --------
    >>> volumes = aura.get_vol(2, date(2025, 10, 16))
    >>> len(volumes)
    144
    >>>
    >>> # Iterate (lazy - no data loaded)
    >>> for vol in volumes:
    ...     data = vol.read()  # Load happens here
    ...     process(data)
    >>>
    >>> # Filter by time range
    >>> morning = volumes.filter(
    ...     start=datetime(2025, 10, 16, 6, 0),
    ...     end=datetime(2025, 10, 16, 12, 0)
    ... )
    >>>
    >>> # Find nearest to a specific time
    >>> vol = volumes.nearest(datetime(2025, 10, 16, 12, 30, 0))
    """

    __slots__ = ("_volumes",)

    def __init__(self, volumes: List[LazyVolume] | None = None):
        """
        Create a new VolumeList.

        Parameters
        ----------
        volumes : List[LazyVolume], optional
            Initial list of volumes. If None, creates an empty list.
        """
        self._volumes: List[LazyVolume] = list(volumes) if volumes else []

    @overload
    def __getitem__(self, index: int) -> LazyVolume: ...

    @overload
    def __getitem__(self, index: slice) -> "VolumeList": ...

    def __getitem__(self, index: int | slice) -> LazyVolume | "VolumeList":
        if isinstance(index, slice):
            return VolumeList(self._volumes[index])
        return self._volumes[index]

    def __len__(self) -> int:
        return len(self._volumes)

    def __iter__(self) -> Iterator[LazyVolume]:
        return iter(self._volumes)

    def __repr__(self) -> str:
        if not self._volumes:
            return "VolumeList(empty)"

        first = self._volumes[0]
        last = self._volumes[-1]
        return (
            f"VolumeList({len(self._volumes)} volumes, "
            f"radar={first.radar_id}, "
            f"from={first.timestamp.isoformat()}, "
            f"to={last.timestamp.isoformat()})"
        )

    def __bool__(self) -> bool:
        return bool(self._volumes)

    def __add__(self, other: "VolumeList") -> "VolumeList":
        """Concatenate two VolumeLists."""
        return VolumeList(self._volumes + other._volumes)

    @cached_property
    def timestamps(self) -> List[datetime]:
        """List of all timestamps in the collection."""
        return [v.timestamp for v in self._volumes]

    @property
    def first(self) -> LazyVolume | None:
        """First volume in the list, or None if empty."""
        return self._volumes[0] if self._volumes else None

    @property
    def last(self) -> LazyVolume | None:
        """Last volume in the list, or None if empty."""
        return self._volumes[-1] if self._volumes else None

    def filter(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        predicate: Callable[[LazyVolume], bool] | None = None,
    ) -> "VolumeList":
        """
        Filter volumes by time range and/or custom predicate.

        Parameters
        ----------
        start : datetime, optional
            Include only volumes at or after this time.
        end : datetime, optional
            Include only volumes at or before this time.
        predicate : Callable[[LazyVolume], bool], optional
            Custom filter function.

        Returns
        -------
        VolumeList
            A new VolumeList with only the matching volumes.
        """
        result = self._volumes

        if start is not None:
            result = [v for v in result if v.timestamp >= start]
        if end is not None:
            result = [v for v in result if v.timestamp <= end]
        if predicate is not None:
            result = [v for v in result if predicate(v)]

        return VolumeList(result)

    def nearest(self, target: datetime) -> LazyVolume:
        """
        Find the volume nearest to the target time.

        Parameters
        ----------
        target : datetime
            The target timestamp to search for.

        Returns
        -------
        LazyVolume
            The volume with the timestamp closest to target.

        Raises
        ------
        ValueError
            If the list is empty.
        """
        if not self._volumes:
            raise ValueError("Cannot find nearest in empty VolumeList")

        # Binary search would be faster for sorted lists, but this is simple
        # and volumes are typically sorted anyway
        return min(self._volumes, key=lambda v: abs((v.timestamp - target).total_seconds()))

    def at(self, target: datetime, tolerance_seconds: float = 60.0) -> LazyVolume | None:
        """
        Find a volume at exactly the target time (within tolerance).

        Parameters
        ----------
        target : datetime
            The exact timestamp to search for.
        tolerance_seconds : float
            Maximum allowed difference in seconds (default 60).

        Returns
        -------
        LazyVolume or None
            The matching volume, or None if no volume is within tolerance.
        """
        if not self._volumes:
            return None

        nearest = self.nearest(target)
        if abs((nearest.timestamp - target).total_seconds()) <= tolerance_seconds:
            return nearest
        return None

    def iter_read(self, **kwargs) -> Iterator[xr.Dataset]:
        """
        Iterate over volumes, reading each one.

        This is a convenience generator that yields the read data for each
        volume. Useful for streaming processing without loading all data
        into memory at once.

        Parameters
        ----------
        **kwargs
            Arguments passed to LazyVolume.read().

        Yields
        ------
        xr.Dataset
            The radar data for each volume.
        """
        for vol in self._volumes:
            yield vol.read(**kwargs)

    def iter_read_with_progress(self, **kwargs) -> Iterator[xr.Dataset]:
        """
        Like iter_read() but with a progress bar.

        Requires tqdm to be installed.
        """
        try:
            from tqdm import tqdm
        except ImportError:
            raise ImportError("tqdm is required for progress bars. Install with: pip install tqdm")

        for vol in tqdm(self._volumes, desc="Reading volumes"):
            yield vol.read(**kwargs)

    def to_list(self) -> List[LazyVolume]:
        """Return a regular list of the volumes."""
        return list(self._volumes)

    def sort(self, reverse: bool = False) -> "VolumeList":
        """
        Return a new VolumeList sorted by timestamp.

        Parameters
        ----------
        reverse : bool
            If True, sort in descending order (newest first).

        Returns
        -------
        VolumeList
            A new sorted VolumeList.
        """
        return VolumeList(sorted(self._volumes, key=lambda v: v.timestamp, reverse=reverse))
