"""
Specific tests for NumPy 2.0 breaking changes
"""
import unittest
import numpy as np
import sys


class TestNumPy2BreakingChanges(unittest.TestCase):
    """Test NumPy 2.0 specific breaking changes"""
    
    def test_str_dtype_instead_of_unicode(self):
        """Test that np.str_ is used instead of deprecated np.unicode_"""
        numpy_version = np.__version__
        major_version = int(numpy_version.split('.')[0])
        
        if major_version >= 2:
            # In NumPy 2.0+, np.unicode_ was removed
            self.assertFalse(hasattr(np, 'unicode_'),
                           "np.unicode_ should not exist in NumPy 2.0+")
            
            # np.str_ should work
            str_dtype = np.dtype([('name', np.str_, 100)])
            self.assertEqual(str_dtype['name'].kind, 'U')  # Unicode string
            
        else:
            # In NumPy 1.x, both should work
            if hasattr(np, 'unicode_'):
                unicode_dtype = np.dtype([('name', np.unicode_, 100)])
                self.assertEqual(unicode_dtype['name'].kind, 'U')
            
            # np.str_ should also work in NumPy 1.x
            str_dtype = np.dtype([('name', np.str_, 100)])
            self.assertEqual(str_dtype['name'].kind, 'U')
    
    def test_structured_array_with_str_dtype(self):
        """Test structured arrays with string dtype (as used in pytebis)"""
        # This mimics the pattern used in pytebis
        dt = np.dtype([
            ('ID', np.int64),
            ('MSTName', np.str_, 100),
            ('UNIT', np.str_, 10),
            ('MSTDesc', np.str_, 255)
        ])
        
        data = np.array([
            (1, 'Temperature', '°C', 'Temperature sensor'),
            (2, 'Pressure', 'bar', 'Pressure sensor')
        ], dtype=dt)
        
        self.assertEqual(len(data), 2)
        self.assertEqual(data['ID'][0], 1)
        self.assertEqual(data['MSTName'][0], 'Temperature')
        self.assertEqual(data['UNIT'][0], '°C')
    
    def test_mixed_dtype_array(self):
        """Test mixed dtype arrays with strings, ints, and floats"""
        dt = np.dtype([
            ('ID', np.int64),
            ('Name', np.str_, 100),
            ('Value', np.float32)
        ])
        
        data = np.array([
            (100, 'Sensor1', 23.5),
            (101, 'Sensor2', 24.7)
        ], dtype=dt)
        
        self.assertEqual(data['Name'][0], 'Sensor1')
        self.assertAlmostEqual(data['Value'][0], 23.5, places=1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
