"""
Unit tests for pytebis utility functions and helper classes
"""
import unittest
import json
import numpy as np
from datetime import datetime
from pytebis.tebis import (
    TebisMST, TebisRMST, TebisVMST, 
    TebisGroupElement, TebisGroupMember,
    TebisTreeElement, TebisMapTreeGroup,
    selective_merge, getDataSeries_as_Json,
    tebisTreeEncoder, testUnicodeError
)


class TestSelectiveMerge(unittest.TestCase):
    """Test the selective_merge configuration helper function"""
    
    def test_merge_simple_dict(self):
        """Test merging simple dictionaries"""
        base = {'a': 1, 'b': 2}
        delta = {'b': 3, 'c': 4}
        result = selective_merge(base, delta)
        
        self.assertEqual(result['a'], 1)
        self.assertEqual(result['b'], 3)
        self.assertEqual(result['c'], 4)
    
    def test_merge_nested_dict(self):
        """Test merging nested dictionaries"""
        base = {
            'host': 'localhost',
            'db': {'user': 'admin', 'port': 1521}
        }
        delta = {
            'db': {'port': 5432, 'schema': 'public'}
        }
        result = selective_merge(base, delta)
        
        self.assertEqual(result['host'], 'localhost')
        self.assertEqual(result['db']['user'], 'admin')
        self.assertEqual(result['db']['port'], 5432)
        self.assertEqual(result['db']['schema'], 'public')
    
    def test_merge_non_dict_overwrite(self):
        """Test that non-dict values are overwritten"""
        base = {'value': [1, 2, 3]}
        delta = {'value': [4, 5, 6]}
        result = selective_merge(base, delta)
        
        self.assertEqual(result['value'], [4, 5, 6])


class TestTebisMST(unittest.TestCase):
    """Test TebisMST class"""
    
    def test_mst_creation(self):
        """Test basic MST creation"""
        mst = TebisMST(id=100, name='Temperature', unit='°C', desc='Temperature sensor')
        
        self.assertEqual(mst.id, 100)
        self.assertEqual(mst.name, 'Temperature')
        self.assertEqual(mst.unit, '°C')
        self.assertEqual(mst.desc, 'Temperature sensor')
        self.assertIsNone(mst.currentValue)
    
    def test_mst_without_optional_params(self):
        """Test MST creation with minimal parameters"""
        mst = TebisMST(id=200, name='Pressure')
        
        self.assertEqual(mst.id, 200)
        self.assertEqual(mst.name, 'Pressure')
        self.assertIsNone(mst.unit)
        self.assertIsNone(mst.desc)


class TestTebisRMST(unittest.TestCase):
    """Test TebisRMST (Real MST) class"""
    
    def test_rmst_creation_from_tuple(self):
        """Test RMST creation from data tuple"""
        elem = (100, 'Temperature', '°C', 'Temp sensor', 'mode1', 'V', 0, 10, -50, 150)
        rmst = TebisRMST(elem)
        
        self.assertEqual(rmst.id, 100)
        self.assertEqual(rmst.name, 'Temperature')
        self.assertEqual(rmst.unit, '°C')
        self.assertEqual(rmst.desc, 'Temp sensor')
        self.assertEqual(rmst.mode, 'mode1')
        self.assertEqual(rmst.elunit, 'V')
        self.assertEqual(rmst.elFrom, 0)
        self.assertEqual(rmst.elTo, 10)
        self.assertEqual(rmst.phyFrom, -50)
        self.assertEqual(rmst.phyTo, 150)
    
    def test_rmst_empty_creation(self):
        """Test RMST creation without parameters"""
        rmst = TebisRMST()
        self.assertIsInstance(rmst, TebisRMST)


class TestTebisVMST(unittest.TestCase):
    """Test TebisVMST (Virtual MST) class"""
    
    def test_vmst_creation_from_tuple(self):
        """Test VMST creation from data tuple"""
        elem = (200, 'CalcTemp', '°C', 'Calculated temp', 'AVG', 'T1+T2', True)
        vmst = TebisVMST(elem)
        
        self.assertEqual(vmst.id, 200)
        self.assertEqual(vmst.name, 'CalcTemp')
        self.assertEqual(vmst.reduction, 'AVG')
        self.assertEqual(vmst.formula, 'T1+T2')
        self.assertTrue(vmst.recalc)


class TestTebisGroupElement(unittest.TestCase):
    """Test TebisGroupElement class"""
    
    def test_group_element_creation(self):
        """Test group element creation"""
        elem = (1, 'Temperature Group', 'All temperature sensors')
        group = TebisGroupElement(elem)
        
        self.assertEqual(group.id, 1)
        self.assertEqual(group.name, 'Temperature Group')
        self.assertEqual(group.desc, 'All temperature sensors')
        self.assertEqual(group.members, [])


class TestTebisGroupMember(unittest.TestCase):
    """Test TebisGroupMember class"""
    
    def test_group_member_creation(self):
        """Test group member creation"""
        elem = (1, 0, 100, -50, 150, '#FF0000', 2, True, 'line', 1.0)
        member = TebisGroupMember(elem)
        
        self.assertEqual(member.groupId, 1)
        self.assertEqual(member.pos, 0)
        self.assertEqual(member.mstID, 100)
        self.assertEqual(member.grpFrom, -50)
        self.assertEqual(member.grpTo, 150)
        self.assertEqual(member.grpColor, '#FF0000')
        self.assertEqual(member.grpWidth, 2)
        self.assertTrue(member.grpVisiblw)


class TestTebisTreeElement(unittest.TestCase):
    """Test TebisTreeElement class"""
    
    def test_tree_element_creation(self):
        """Test tree element creation"""
        elem = (1, None, 0, 'Root')
        tree = TebisTreeElement(elem)
        
        self.assertEqual(tree.id, 1)
        self.assertIsNone(tree.parent)
        self.assertEqual(tree.order, 0)
        self.assertEqual(tree.name, 'Root')
        self.assertEqual(tree.childs, [])
        self.assertEqual(tree.grps, [])
    
    def test_find_node_by_id(self):
        """Test finding nodes in tree structure"""
        # Create root
        root = TebisTreeElement((1, None, 0, 'Root'))
        
        # Create children
        child1 = TebisTreeElement((2, 1, 0, 'Child1'))
        child2 = TebisTreeElement((3, 1, 1, 'Child2'))
        
        root.childs = [child1, child2]
        
        # Find existing node
        found = root.findNodeByID(2)
        self.assertIsNotNone(found)
        self.assertEqual(found.name, 'Child1')
        
        # Find non-existing node
        not_found = root.findNodeByID(999)
        self.assertIsNone(not_found)


class TestTebisMapTreeGroup(unittest.TestCase):
    """Test TebisMapTreeGroup class"""
    
    def test_map_tree_group_creation(self):
        """Test map tree group creation"""
        elem = (1,)
        map_group = TebisMapTreeGroup(elem)
        
        self.assertEqual(map_group.treeId, 1)
        self.assertEqual(map_group.groups, [])


class TestGetDataSeriesAsJson(unittest.TestCase):
    """Test JSON conversion of numpy data series"""
    
    def test_convert_simple_data_to_json(self):
        """Test converting numpy array to JSON"""
        # Create structured numpy array
        dt = np.dtype([('timestamp', np.int64), ('temp', np.float32), ('pressure', np.float32)])
        data = np.array([(1638360000000, 23.5, 1013.25), 
                        (1638360001000, 23.6, 1013.30)], dtype=dt)
        
        result = getDataSeries_as_Json(data)
        
        # Parse JSON to verify structure
        parsed = json.loads(result)
        self.assertIn('timestamp', parsed)
        self.assertIn('temp', parsed)
        self.assertIn('pressure', parsed)
        self.assertEqual(len(parsed['timestamp']), 2)
    
    def test_convert_with_nan_values(self):
        """Test JSON conversion with NaN values"""
        dt = np.dtype([('value', np.float32)])
        data = np.array([(23.5,), (np.nan,), (24.1,)], dtype=dt)
        
        result = getDataSeries_as_Json(data)
        
        # Should handle NaN gracefully
        self.assertIsInstance(result, str)
        parsed = json.loads(result)
        self.assertEqual(len(parsed['value']), 3)


class TestTebisTreeEncoder(unittest.TestCase):
    """Test custom JSON encoder for Tebis tree structures"""
    
    def test_encode_tree_element(self):
        """Test encoding TebisTreeElement to JSON"""
        tree = TebisTreeElement((1, None, 0, 'Root'))
        
        result = json.dumps(tree, cls=tebisTreeEncoder)
        parsed = json.loads(result)
        
        self.assertEqual(parsed['id'], 1)
        self.assertEqual(parsed['text'], 'Root')
        self.assertIn('nodes', parsed)
    
    def test_encode_group_element(self):
        """Test encoding TebisGroupElement to JSON"""
        group = TebisGroupElement((1, 'TempGroup', 'Temperature sensors'))
        
        result = json.dumps(group, cls=tebisTreeEncoder)
        parsed = json.loads(result)
        
        self.assertEqual(parsed['id'], 1)
        self.assertEqual(parsed['name'], 'TempGroup')
        self.assertEqual(parsed['desc'], 'Temperature sensors')
    
    def test_encode_group_member(self):
        """Test encoding TebisGroupMember to JSON"""
        # Create a group member with an MST
        member = TebisGroupMember((1, 0, 100, -50, 150, '#FF0000', 2, True, 'line', 1.0))
        member.mst = TebisMST(100, 'Temperature', '°C', 'Temp sensor')
        
        result = json.dumps(member, cls=tebisTreeEncoder)
        parsed = json.loads(result)
        
        self.assertEqual(parsed['id'], 1)
        self.assertEqual(parsed['name'], 'Temperature')
        self.assertEqual(parsed['unit'], '°C')


class TestTestUnicodeError(unittest.TestCase):
    """Test Unicode error handling helper function"""
    
    def test_valid_unicode(self):
        """Test with valid unicode strings"""
        elem = ['first', 'second', 'third']
        result = testUnicodeError(elem, 1)
        self.assertEqual(result, 'second')
    
    def test_index_out_of_range(self):
        """Test with index out of range"""
        elem = ['first', 'second']
        # This should raise IndexError, not UnicodeDecodeError
        with self.assertRaises(IndexError):
            testUnicodeError(elem, 5)


if __name__ == '__main__':
    unittest.main()
