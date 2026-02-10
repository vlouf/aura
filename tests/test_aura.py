"""
Tests for the AURA library.

These tests use a mock archive structure to test the library without
requiring access to the actual NCI Gadi filesystem.
"""

import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

# Import the library
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aura.config import set_base_path, get_base_path
from aura.volume import LazyVolume, VolumeList
from aura.core import get_vol


@pytest.fixture
def mock_archive(tmp_path):
    """Create a mock AURA archive structure for testing."""
    # Create radar directory structure
    radar_id = 2
    year = 2025
    
    vol_dir = tmp_path / str(radar_id) / str(year) / "vol"
    vol_dir.mkdir(parents=True)
    
    # Create a mock zip file with fake h5 files
    zip_file = vol_dir / f"{radar_id}_20251016.pvol.zip"
    
    # Create volume filenames for the day (every 10 minutes)
    with zipfile.ZipFile(zip_file, "w") as zf:
        for hour in range(24):
            for minute in [0, 10, 20, 30, 40, 50]:
                filename = f"{radar_id}_20251016_{hour:02d}{minute:02d}00.pvol.h5"
                # Write minimal content (won't be valid HDF5, but enough for listing)
                zf.writestr(filename, b"mock h5 content")
    
    # Create a second day
    zip_file2 = vol_dir / f"{radar_id}_20251017.pvol.zip"
    with zipfile.ZipFile(zip_file2, "w") as zf:
        for hour in range(12):  # Only half day
            for minute in [0, 30]:
                filename = f"{radar_id}_20251017_{hour:02d}{minute:02d}00.pvol.h5"
                zf.writestr(filename, b"mock h5 content")
    
    # Create radar site list CSV
    csv_content = """id,id_long,WIGOS,short_name,location,radar_type,postchange_start,prechange_end,site_lat,site_lon,ge_ground_altitude,site_alt,status,band,doppler,dp,eth_dhz_threshold,beamwidth,state,notes
2,002_1,0-20000-0-94865,Melb,Melbourne,DWSR81C,7/09/1993,30/07/2007,-37.8553,144.7554,20,42,OK,C,,,18,1,VIC,
2,002_2,0-20000-0-94865,Melb,Melbourne,Meteor1500S,31/07/2007,25/08/2017,-37.8553,144.7554,20,45.093,OK,S,Yes,,0,1,VIC,
2,002_3,0-20000-0-94865,Melb,Melbourne,Meteor1500S,11/10/2017,-,-37.8553,144.7554,20,45.093,OK,S,Yes,Yes,0,1,VIC,
"""
    (tmp_path / "radar_site_list.csv").write_text(csv_content)
    
    # Set the mock path
    old_path = get_base_path()
    set_base_path(tmp_path)
    
    yield tmp_path
    
    # Restore
    set_base_path(old_path if old_path != tmp_path else None)


class TestLazyVolume:
    """Tests for LazyVolume class."""
    
    def test_from_filename(self, tmp_path):
        """Test creating LazyVolume from filename."""
        zip_path = tmp_path / "test.zip"
        filename = "2_20251016_123456.pvol.h5"
        
        vol = LazyVolume.from_filename(zip_path, filename)
        
        assert vol.radar_id == 2
        assert vol.timestamp == datetime(2025, 10, 16, 12, 34, 56)
        assert vol.filename == filename
        assert vol.zip_path == zip_path
    
    def test_from_filename_invalid(self, tmp_path):
        """Test that invalid filenames raise ValueError."""
        zip_path = tmp_path / "test.zip"
        
        with pytest.raises(ValueError):
            LazyVolume.from_filename(zip_path, "invalid_filename.h5")
    
    def test_repr(self, tmp_path):
        """Test string representation."""
        zip_path = tmp_path / "test.zip"
        vol = LazyVolume.from_filename(zip_path, "2_20251016_123456.pvol.h5")
        
        repr_str = repr(vol)
        assert "radar=2" in repr_str
        assert "2025-10-16" in repr_str


class TestVolumeList:
    """Tests for VolumeList class."""
    
    @pytest.fixture
    def sample_volumes(self, tmp_path):
        """Create sample volumes for testing."""
        zip_path = tmp_path / "test.zip"
        volumes = []
        for hour in range(6):
            filename = f"2_20251016_{hour:02d}0000.pvol.h5"
            volumes.append(LazyVolume.from_filename(zip_path, filename))
        return VolumeList(volumes)
    
    def test_len(self, sample_volumes):
        """Test length."""
        assert len(sample_volumes) == 6
    
    def test_iteration(self, sample_volumes):
        """Test iteration."""
        timestamps = [v.timestamp for v in sample_volumes]
        assert len(timestamps) == 6
    
    def test_indexing(self, sample_volumes):
        """Test integer indexing."""
        vol = sample_volumes[0]
        assert vol.timestamp.hour == 0
        
        vol = sample_volumes[-1]
        assert vol.timestamp.hour == 5
    
    def test_slicing(self, sample_volumes):
        """Test slice indexing."""
        subset = sample_volumes[1:4]
        assert isinstance(subset, VolumeList)
        assert len(subset) == 3
    
    def test_filter_by_time(self, sample_volumes):
        """Test filtering by time range."""
        start = datetime(2025, 10, 16, 2, 0)
        end = datetime(2025, 10, 16, 4, 0)
        
        filtered = sample_volumes.filter(start=start, end=end)
        
        assert len(filtered) == 3  # 02:00, 03:00, 04:00
        assert all(start <= v.timestamp <= end for v in filtered)
    
    def test_nearest(self, sample_volumes):
        """Test finding nearest volume."""
        target = datetime(2025, 10, 16, 2, 30)
        nearest = sample_volumes.nearest(target)
        
        # Should be either 02:00 or 03:00 (both 30 min away)
        assert nearest.timestamp.hour in [2, 3]
    
    def test_at_exact(self, sample_volumes):
        """Test finding volume at exact time."""
        target = datetime(2025, 10, 16, 3, 0, 0)
        vol = sample_volumes.at(target, tolerance_seconds=1)
        
        assert vol is not None
        assert vol.timestamp == target
    
    def test_at_not_found(self, sample_volumes):
        """Test at() returns None when no match."""
        target = datetime(2025, 10, 16, 3, 30, 0)  # No volume at 3:30
        vol = sample_volumes.at(target, tolerance_seconds=60)
        
        assert vol is None


class TestGetVol:
    """Tests for get_vol function."""
    
    def test_get_vol_date(self, mock_archive):
        """Test getting all volumes for a date."""
        volumes = get_vol(2, date(2025, 10, 16))
        
        assert isinstance(volumes, VolumeList)
        assert len(volumes) == 144  # 24 hours * 6 per hour
    
    def test_get_vol_datetime_nearest(self, mock_archive):
        """Test getting nearest volume to a datetime."""
        dt = datetime(2025, 10, 16, 12, 35)  # Between 12:30 and 12:40
        vol = get_vol(2, dt, nearest=True)
        
        assert isinstance(vol, LazyVolume)
        # Should be 12:30 or 12:40
        assert vol.timestamp.hour == 12
        assert vol.timestamp.minute in [30, 40]
    
    def test_get_vol_date_range(self, mock_archive):
        """Test getting volumes for a date range."""
        volumes = get_vol(2, date(2025, 10, 16), date(2025, 10, 17))
        
        assert isinstance(volumes, VolumeList)
        # 144 from first day + 24 from second day (12 hours * 2 per hour)
        assert len(volumes) == 144 + 24
    
    def test_get_vol_not_found(self, mock_archive):
        """Test error when no data exists."""
        with pytest.raises(FileNotFoundError):
            get_vol(2, date(2025, 10, 20))  # Date doesn't exist
    
    def test_get_vol_timezone_aware(self, mock_archive):
        """Test with timezone-aware datetime."""
        # AEST is UTC+10, so 22:00 AEST on Oct 16 = 12:00 UTC on Oct 16
        aest = timezone(timedelta(hours=10))
        local_time = datetime(2025, 10, 16, 22, 0, tzinfo=aest)
        
        vol = get_vol(2, local_time, nearest=True)
        
        assert isinstance(vol, LazyVolume)
        assert vol.timestamp.hour == 12  # UTC
    
    def test_get_vol_midnight_datetime_returns_list(self, mock_archive):
        """Test that midnight datetime returns full day (like pd.Timestamp('date'))."""
        # A datetime at exactly midnight should return all volumes for the day
        midnight = datetime(2025, 10, 16, 0, 0, 0)
        volumes = get_vol(2, midnight)
        
        assert isinstance(volumes, VolumeList)
        assert len(volumes) == 144  # All volumes for the day
    
    def test_get_vol_midnight_with_nearest_returns_single(self, mock_archive):
        """Test that midnight datetime with nearest=True returns single volume."""
        midnight = datetime(2025, 10, 16, 0, 0, 0)
        vol = get_vol(2, midnight, nearest=True)
        
        assert isinstance(vol, LazyVolume)
        assert vol.timestamp.hour == 0


class TestRadarMetadata:
    """Tests for radar metadata functions."""
    
    def test_get_radar(self, mock_archive):
        """Test getting radar info."""
        from aura.radar import get_radar
        
        radar = get_radar(2)
        
        assert radar.id == 2
        assert radar.location == "Melbourne"
        assert radar.is_current
    
    def test_get_radar_historical(self, mock_archive):
        """Test getting historical radar config."""
        from aura.radar import get_radar
        
        radar = get_radar(2, at_date=date(2010, 1, 1))
        
        assert radar.radar_type == "Meteor1500S"
        assert not radar.dual_pol  # Dual-pol added in 2017
    
    def test_list_radars(self, mock_archive):
        """Test listing radars."""
        from aura.radar import list_radars
        
        radars = list_radars()
        assert len(radars) == 3  # 3 configs for radar 2
        
        current = list_radars(current_only=True)
        assert len(current) == 1


class TestAvailability:
    """Tests for availability functions."""
    
    def test_available_years(self, mock_archive):
        """Test getting available years."""
        from aura.availability import available_years
        
        years = available_years(2)
        assert 2025 in years
    
    def test_available_dates(self, mock_archive):
        """Test getting available dates."""
        from aura.availability import available_dates
        
        dates = available_dates(2, year=2025)
        assert date(2025, 10, 16) in dates
        assert date(2025, 10, 17) in dates
    
    def test_has_data(self, mock_archive):
        """Test checking if data exists."""
        from aura.availability import has_data
        
        assert has_data(2, date(2025, 10, 16))
        assert not has_data(2, date(2025, 10, 20))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
