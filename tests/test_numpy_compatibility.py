"""
Compatibility tests for different NumPy versions
"""
import unittest
import numpy as np
import sys


class TestNumpyCompatibility(unittest.TestCase):
    """Test NumPy version compatibility"""
    
    def test_numpy_version_info(self):
        """Display NumPy version information"""
        numpy_version = np.__version__
        major_version = int(numpy_version.split('.')[0])
        
        print(f"\n=== NumPy Version Info ===")
        print(f"NumPy Version: {numpy_version}")
        print(f"Major Version: {major_version}")
        print(f"Python Version: {sys.version}")
        
        self.assertIsNotNone(numpy_version)
        self.assertIn(major_version, [1, 2])
    
    def test_structured_array_creation(self):
        """Test structured array creation (used in pytebis)"""
        # This is the pattern used in pytebis for data storage
        dt = np.dtype([('timestamp', np.int64), ('value', np.float32)])
        data = np.array([(1638360000000, 23.5), (1638360001000, 24.1)], dtype=dt)
        
        self.assertEqual(len(data), 2)
        self.assertEqual(data['timestamp'][0], 1638360000000)
        self.assertAlmostEqual(data['value'][0], 23.5, places=1)
    
    def test_numpy_nan_handling(self):
        """Test NaN handling across NumPy versions"""
        arr = np.array([1.0, np.nan, 3.0], dtype=np.float32)
        
        self.assertTrue(np.isnan(arr[1]))
        self.assertEqual(np.sum(~np.isnan(arr)), 2)
    
    def test_dtype_compatibility(self):
        """Test dtype definitions used in pytebis"""
        # Test all dtypes used in the project
        types = [
            ('timestamp', np.int64),
            ('temp', np.float32),
            ('pressure', np.float32),
            ('value', np.float64)
        ]
        
        dt = np.dtype(types)
        self.assertEqual(len(dt.names), 4)
        self.assertIn('timestamp', dt.names)
    
    def test_array_indexing(self):
        """Test array indexing behavior"""
        dt = np.dtype([('id', np.int32), ('value', np.float32)])
        data = np.array([(1, 10.5), (2, 20.3), (3, 30.7)], dtype=dt)
        
        # Test field access
        ids = data['id']
        values = data['value']
        
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids[0], 1)
        self.assertAlmostEqual(values[2], 30.7, places=1)
    
    def test_numpy_version_specific_behavior(self):
        """Test version-specific NumPy behavior"""
        numpy_version = np.__version__
        major_version = int(numpy_version.split('.')[0])
        
        if major_version >= 2:
            # NumPy 2.x specific tests
            print("\n=== Running NumPy 2.x specific tests ===")
            
            # In NumPy 2.0+, np.unicode_ was removed, use np.str_ instead
            arr = np.array([1, 2, 3], dtype=np.int64)
            self.assertEqual(arr.dtype, np.int64)
            
            # Test that np.str_ works (replaces np.unicode_)
            str_dtype = np.dtype([('name', np.str_, 100)])
            self.assertIsNotNone(str_dtype)
            
            # Verify np.unicode_ doesn't exist in NumPy 2.x
            self.assertFalse(hasattr(np, 'unicode_'))
            
        else:
            # NumPy 1.x specific tests
            print("\n=== Running NumPy 1.x specific tests ===")
            
            # Test legacy behavior
            arr = np.array([1, 2, 3], dtype=np.int64)
            self.assertEqual(arr.dtype, np.int64)
            
            # In NumPy 1.x, both np.unicode_ and np.str_ should work
            if hasattr(np, 'unicode_'):
                unicode_dtype = np.dtype([('name', np.unicode_, 100)])
                self.assertIsNotNone(unicode_dtype)
    
    def test_tolist_conversion(self):
        """Test conversion to list (used in JSON serialization)"""
        dt = np.dtype([('timestamp', np.int64), ('value', np.float32)])
        data = np.array([(1000, 1.5), (2000, 2.5)], dtype=dt)
        
        # Convert to dict with lists (as done in getDataSeries_as_Json)
        result = {
            'timestamp': data['timestamp'].tolist(),
            'value': data['value'].tolist()
        }
        
        self.assertIsInstance(result['timestamp'], list)
        self.assertEqual(len(result['timestamp']), 2)
        self.assertEqual(result['timestamp'][0], 1000)


if __name__ == '__main__':
    unittest.main(verbosity=2)
