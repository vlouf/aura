# AURA - Australian Unified Radar Archive Python Interface

A Python library for efficiently accessing ODIM HDF5 radar data from the NCI AURA archive on Gadi.

## Features

- **Lazy loading**: Access thousands of radar volumes without loading them into memory until needed
- **Simple API**: Get volumes by radar ID, date, datetime, or date range
- **Time-aware**: Supports timezone-aware datetimes (automatically converts to UTC)
- **Efficient**: Lists zip contents without extraction; only extracts individual files when read
- **Parallel-ready**: `LazyVolume` objects are picklable for use with multiprocessing, dask.bag, etc.
- **Radar metadata**: Look up radar information (location, type, capabilities) from the archive

## Installation

```bash
# From source
cd aura
pip install -e .

# Or with dev dependencies
pip install -e ".[dev]"
```

### Requirements

- Python >= 3.9
- Access to NCI Gadi and the `rq0` project
- pyodim (for reading ODIM HDF5 files)

## Quick Start

```python
import aura
from datetime import date, datetime

# Get all volumes for a day (lazy - no data loaded yet)
volumes = aura.get_vol(2, date(2025, 10, 16))
print(f"Found {len(volumes)} volumes")

# Iterate and process (data loaded one at a time)
for vol in volumes:
    data = vol.read()  # <-- Data loaded HERE
    process(data)

# Get the nearest volume to a specific time
vol = aura.get_vol(2, datetime(2025, 10, 16, 12, 30), nearest=True)
data = vol.read()

# Get volumes for a date range
volumes = aura.get_vol(2, date(2025, 10, 1), date(2025, 10, 7))

# Direct read (convenience function)
data = aura.read_vol(2, datetime(2025, 10, 16, 12, 30))
```

## API Reference

### Main Functions

#### `aura.get_vol(radar_id, time_or_date, [end_date], *, nearest=False)`

Get radar volume(s) from the archive.

**Signatures:**
- `get_vol(radar_id, datetime, nearest=True)` → `LazyVolume` (nearest to time)
- `get_vol(radar_id, datetime, nearest=False)` → `LazyVolume` (exact match or error)
- `get_vol(radar_id, date)` → `VolumeList` (all volumes for day)
- `get_vol(radar_id, start_date, end_date)` → `VolumeList` (date range)

```python
# Single datetime - find nearest
vol = aura.get_vol(2, datetime(2025, 10, 16, 12, 30), nearest=True)

# Single date - all volumes for that day
volumes = aura.get_vol(2, date(2025, 10, 16))

# Date range
volumes = aura.get_vol(2, date(2025, 10, 1), date(2025, 10, 7))

# With timezone (automatically converted to UTC)
from datetime import timezone, timedelta
aest = timezone(timedelta(hours=10))
local_time = datetime(2025, 10, 16, 22, 30, tzinfo=aest)
vol = aura.get_vol(2, local_time, nearest=True)
```

#### `aura.read_vol(radar_id, datetime, *, nearest=True)`

Convenience function to directly read a volume (combines `get_vol` + `.read()`).

```python
data = aura.read_vol(2, datetime(2025, 10, 16, 12, 30))
```

### Classes

#### `LazyVolume`

A lazy reference to a radar volume file inside a zip archive.

**Attributes:**
- `zip_path`: Path to the zip archive
- `filename`: Name of the file inside the zip
- `radar_id`: Radar identifier
- `timestamp`: UTC timestamp of the scan

**Methods:**
- `read(**kwargs)` → `xr.Dataset`: Read the volume using pyodim
- `read_h5py()` → `h5py.File`: Read raw HDF5 (for advanced use)
- `extract_to(directory)` → `Path`: Extract file to disk

```python
vol = aura.get_vol(2, datetime(2025, 10, 16, 12, 30), nearest=True)

# Lazy - nothing loaded yet
print(vol.timestamp)  # 2025-10-16 12:28:45

# Read with pyodim
data = vol.read()

# Read specific sweep
sweep0 = vol.read(nslice=0)

# Extract to disk for external tools
path = vol.extract_to("/scratch/myproject/tmp")
```

#### `VolumeList`

A collection of `LazyVolume` objects with convenience methods.

**Methods:**
- `filter(start=None, end=None, predicate=None)` → `VolumeList`
- `nearest(datetime)` → `LazyVolume`
- `at(datetime, tolerance_seconds=60)` → `LazyVolume | None`
- `iter_read(**kwargs)` → `Iterator[xr.Dataset]`
- `iter_read_with_progress(**kwargs)` → `Iterator[xr.Dataset]` (with tqdm)

```python
volumes = aura.get_vol(2, date(2025, 10, 16))

# Filter by time range
morning = volumes.filter(
    start=datetime(2025, 10, 16, 6, 0),
    end=datetime(2025, 10, 16, 12, 0)
)

# Find nearest to a time
vol = volumes.nearest(datetime(2025, 10, 16, 12, 30))

# Iterate with progress bar
for data in volumes.iter_read_with_progress():
    process(data)
```

### Radar Metadata

#### `aura.get_radar(radar_id, [at_date])`

Get information about a radar.

```python
radar = aura.get_radar(2)
print(f"{radar.location}: {radar.radar_type}")
print(f"Location: {radar.lat}, {radar.lon}")
print(f"Band: {radar.band}, Dual-pol: {radar.dual_pol}")

# Historical configuration
old_config = aura.get_radar(2, at_date=date(2010, 1, 1))
```

#### `aura.list_radars(**filters)`

List all radars with optional filtering.

```python
# All current S-band dual-pol radars
radars = aura.list_radars(current_only=True, band="S", dual_pol_only=True)

# All radars in Victoria
vic_radars = aura.list_radars(state="VIC")
```

### Data Availability

#### `aura.available_years(radar_id)`

Get years with available data.

```python
years = aura.available_years(2)  # [1993, 1994, ..., 2025]
```

#### `aura.available_dates(radar_id, year=None, start_date=None, end_date=None)`

Get dates with available data.

```python
# All dates in 2025
dates = aura.available_dates(2, year=2025)

# Date range
dates = aura.available_dates(2, 
    start_date=date(2025, 1, 1),
    end_date=date(2025, 1, 31)
)
```

## Working with Parallel Processing

`LazyVolume` objects are lightweight and picklable, making them perfect for parallel processing:

```python
from multiprocessing import Pool

volumes = aura.get_vol(2, date(2025, 10, 16))

def process_volume(vol):
    data = vol.read()
    # ... your processing ...
    return result

# With multiprocessing
with Pool(4) as p:
    results = p.map(process_volume, volumes)

# With dask.bag
import dask.bag as db
bag = db.from_sequence(volumes)
results = bag.map(process_volume).compute()
```

## Configuration

### Custom Base Path

For testing or alternative mount points:

```python
from aura.config import set_base_path

# Use a different path
set_base_path("/scratch/myproject/radar_mirror")

# Reset to default
set_base_path(None)
```

Or via environment variable:

```bash
export AURA_BASE_PATH=/scratch/myproject/radar_mirror
```

## Archive Structure

The AURA archive on NCI Gadi follows this structure:

```
/g/data/rq0/level_1/odim_pvol/
├── radar_site_list.csv
├── 2/                          # Radar ID
│   ├── 2025/                   # Year
│   │   ├── vol/
│   │   │   ├── 2_20251001.pvol.zip
│   │   │   ├── 2_20251002.pvol.zip
│   │   │   └── ...
│   │   ├── img/                # Daily summary images
│   │   └── list/               # Statistics spreadsheets
│   └── 2024/
└── 3/
    └── ...
```

Each daily zip contains individual volume files:
```
2_20251016.pvol.zip
├── 2_20251016_000123.pvol.h5
├── 2_20251016_001023.pvol.h5
├── 2_20251016_001923.pvol.h5
└── ...
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please feel free to submit issues and pull requests.
