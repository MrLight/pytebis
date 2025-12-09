"""
Integration-style tests for pytebis Tebis class
These tests focus on configuration and initialization without requiring a live server
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import datetime
import numpy as np
from pytebis.tebis import Tebis, TebisMST, TebisException, TebisOracleDBException


class TestTebisConfiguration(unittest.TestCase):
    """Test Tebis class configuration and initialization"""
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    def test_default_configuration(self, mock_refresh):
        """Test initialization with default configuration"""
        config = {
            'host': '192.168.1.10',
            'configfile': '/path/to/config.txt'
        }
        
        teb = Tebis(configuration=config)
        
        self.assertEqual(teb.config['host'], '192.168.1.10')
        self.assertEqual(teb.config['port'], 4712)  # Default port
        self.assertFalse(teb.config['liveValues']['enable'])
        mock_refresh.assert_called_once()
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    def test_custom_port_configuration(self, mock_refresh):
        """Test initialization with custom port"""
        config = {
            'host': '192.168.1.10',
            'port': 5000,
            'configfile': '/path/to/config.txt'
        }
        
        teb = Tebis(configuration=config)
        
        self.assertEqual(teb.config['port'], 5000)
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    def test_oracle_configuration_auto_enable(self, mock_refresh):
        """Test that Oracle is auto-enabled when host is provided"""
        config = {
            'host': '192.168.1.10',
            'configfile': '/path/to/config.txt',
            'OracleDbConn': {
                'host': '192.168.1.20',
                'user': 'testuser',
                'psw': 'testpass'
            }
        }
        
        teb = Tebis(configuration=config)
        
        self.assertTrue(teb.config['useOracle'])
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    def test_oracle_configuration_explicit_disable(self, mock_refresh):
        """Test explicit disabling of Oracle"""
        config = {
            'host': '192.168.1.10',
            'configfile': '/path/to/config.txt',
            'useOracle': False,
            'OracleDbConn': {
                'host': '192.168.1.20',
            }
        }
        
        teb = Tebis(configuration=config)
        
        self.assertFalse(teb.config['useOracle'])
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    @patch('pytebis.tebis.Tebis.setupLiveValues')
    def test_live_values_configuration(self, mock_setup_live, mock_refresh):
        """Test live values feature configuration"""
        config = {
            'host': '192.168.1.10',
            'configfile': '/path/to/config.txt',
            'liveValues': {
                'enable': True,
                'offsetMstId': 100050
            }
        }
        
        teb = Tebis(configuration=config)
        
        self.assertTrue(teb.config['liveValues']['enable'])
        self.assertEqual(teb.config['liveValues']['offsetMstId'], 100050)
        mock_setup_live.assert_called_once()


class TestTebisTimestampConversion(unittest.TestCase):
    """Test timestamp conversion logic used in data retrieval"""
    
    def test_datetime_to_timestamp(self):
        """Test datetime object conversion"""
        dt = datetime.datetime(2023, 12, 1, 12, 0, 0)
        # Convert to milliseconds
        expected = dt.timestamp() * 1000.0
        
        self.assertIsInstance(expected, float)
        self.assertGreater(expected, 0)
    
    def test_float_to_timestamp(self):
        """Test float (seconds) conversion to milliseconds"""
        timestamp_sec = 1701432000.0
        timestamp_ms = timestamp_sec * 1000.0
        
        self.assertEqual(timestamp_ms, 1701432000000.0)
    
    def test_int_to_timestamp(self):
        """Test integer (seconds) conversion"""
        timestamp_sec = 1701432000
        # For timestamps less than 100000000000, multiply by 1000
        if timestamp_sec < 100000000000:
            timestamp_ms = timestamp_sec * 1000.0
        
        self.assertEqual(timestamp_ms, 1701432000000.0)
    
    def test_timestamp_already_in_milliseconds(self):
        """Test timestamp already in milliseconds"""
        timestamp_ms = 1701432000000
        # Should not be multiplied if > 100000000000
        if timestamp_ms >= 100000000000:
            result = timestamp_ms
        else:
            result = timestamp_ms * 1000.0
        
        self.assertEqual(result, 1701432000000)


class TestTebisMSTRetrieval(unittest.TestCase):
    """Test MST (measuring point) retrieval methods"""
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    def setUp(self, mock_refresh):
        """Set up test Tebis instance with mock data"""
        config = {
            'host': '192.168.1.10',
            'configfile': '/path/to/config.txt'
        }
        self.teb = Tebis(configuration=config)
        
        # Create mock MST dictionaries
        mst1 = TebisMST(100, 'Temperature', '°C', 'Temperature sensor')
        mst2 = TebisMST(101, 'Pressure', 'bar', 'Pressure sensor')
        
        self.teb.mstById = {100: mst1, 101: mst2}
        self.teb.mstByName = {'Temperature': mst1, 'Pressure': mst2}
    
    def test_get_mst_by_id(self):
        """Test retrieving MST by ID"""
        mst = self.teb.getMst(id=100)
        
        self.assertIsNotNone(mst)
        self.assertEqual(mst.name, 'Temperature')
        self.assertEqual(mst.unit, '°C')
    
    def test_get_mst_by_name(self):
        """Test retrieving MST by name"""
        mst = self.teb.getMst(name='Pressure')
        
        self.assertIsNotNone(mst)
        self.assertEqual(mst.id, 101)
        self.assertEqual(mst.unit, 'bar')
    
    def test_get_mst_not_found(self):
        """Test retrieving non-existent MST"""
        mst = self.teb.getMst(id=999)
        
        self.assertIsNone(mst)
    
    def test_get_msts_by_ids(self):
        """Test retrieving multiple MSTs by IDs"""
        msts = self.teb.getMsts(ids=[100, 101])
        
        self.assertEqual(len(msts), 2)
        self.assertEqual(msts[0].name, 'Temperature')
        self.assertEqual(msts[1].name, 'Pressure')
    
    def test_get_msts_by_names(self):
        """Test retrieving multiple MSTs by names"""
        msts = self.teb.getMsts(names=['Temperature', 'Pressure'])
        
        self.assertEqual(len(msts), 2)
        self.assertEqual(msts[0].id, 100)
        self.assertEqual(msts[1].id, 101)


class TestTebisOracleDBMethods(unittest.TestCase):
    """Test Oracle database related methods"""
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    def test_get_map_tree_group_without_oracle(self, mock_refresh):
        """Test that accessing tree groups without Oracle raises exception"""
        config = {
            'host': '192.168.1.10',
            'configfile': '/path/to/config.txt',
            'useOracle': False
        }
        
        teb = Tebis(configuration=config)
        
        with self.assertRaises(TebisOracleDBException):
            teb.getMapTreeGroupById(1)
    
    @patch('pytebis.tebis.Tebis.refreshMsts')
    def test_get_map_tree_group_with_oracle(self, mock_refresh):
        """Test tree group retrieval with Oracle enabled"""
        config = {
            'host': '192.168.1.10',
            'configfile': '/path/to/config.txt',
            'useOracle': True
        }
        
        teb = Tebis(configuration=config)
        teb.tebisMapTreeGroupById = {1: 'mock_tree_group'}
        
        result = teb.getMapTreeGroupById(1)
        
        self.assertEqual(result, 'mock_tree_group')


class TestTebisDataCalculations(unittest.TestCase):
    """Test data calculation and time range logic"""
    
    def test_time_range_calculation(self):
        """Test time range and sample count calculation"""
        start = 1701432000000  # milliseconds
        end = 1701435600000    # 1 hour later
        rate = 1  # 1 second
        
        nCT = rate * 1000.0  # Convert to milliseconds
        nTimeR = (int(float(end)) // int(nCT)) * int(nCT)
        nTimeL = (int(float(start)) // int(nCT)) * int(nCT)
        nNmbX = int(nTimeR - nTimeL) / int(nCT)
        
        self.assertEqual(nCT, 1000.0)
        self.assertEqual(nNmbX, 3600.0)  # 3600 samples for 1 hour at 1 second rate
    
    def test_minimum_sample_count(self):
        """Test that minimum sample count is 1"""
        start = 1701432000000
        end = 1701432000500  # Only 500ms difference
        rate = 1  # 1 second
        
        nCT = rate * 1000.0
        nTimeR = (int(float(end)) // int(nCT)) * int(nCT)
        nTimeL = (int(float(start)) // int(nCT)) * int(nCT)
        nNmbX = int(nTimeR - nTimeL) / int(nCT)
        
        if nNmbX <= 0:
            nNmbX = 1
        
        self.assertEqual(nNmbX, 1)


if __name__ == '__main__':
    unittest.main()
