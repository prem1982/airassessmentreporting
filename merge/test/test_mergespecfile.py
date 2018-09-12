'''
Created on May 20, 2013

@author: temp_dmenes
'''
import os
import unittest

from airassessmentreporting.merge import *

from abstractmergetestcase import AbstractMergeTestCase

class Test( AbstractMergeTestCase ):
    
    def test_create(self):
        filename = os.path.join( self.out_dir, 'testfile.xlsx' )
        answer = create_spec_file( filename, self.merge_def, True, False)
        self.assertTrue( answer, "Reported that it did not create the file!" )
        
    def test_create_with_break(self):
        filename = os.path.join( self.out_dir, 'testfile.xlsx' )
        try:
            create_spec_file( filename, self.merge_def, True, True)
            self.fail( "Keyboard interrupt not sent" )
        except KeyboardInterrupt:
            pass
        
    def test_dont_overwrite( self ):
        filename = os.path.join( self.out_dir, 'testfile.xlsx' )
        f=open( filename, 'w' )
        f.write("This is a file!")
        f.close()
        answer = create_spec_file( filename, self.merge_def, True, False)
        self.assertFalse( answer, "Reported that it created the file!" )
        f = open( filename, 'r' )
        text = f.read()
        self.assertEqual( text, "This is a file!", "Overwrote the existing file" )
        
    def test_overwrite( self ):
        filename = os.path.join( self.out_dir, 'testfile.xlsx' )
        f=open( filename, 'w' )
        f.write("This is a file!")
        f.close()
        answer = create_spec_file( filename, self.merge_def, False, False)
        self.assertTrue( answer, "Reported that it did not the file!" )
        f = open( filename, 'r' )
        text = f.read()
        self.assertNotEqual( text, "This is a file!", "Did not overwrite the existing file" )
        
    def test_read( self ):
        filename = os.path.join( self.run_context.tests_safe_dir, 'merge_spec.xls' )
        merge_def = MergeDef( self.db_context )
        merge_def.table_name = 'merge_output'
        merge_def.left_input_table = self.data1
        merge_def.right_input_table = self.data2
        
        read_spec_file( filename, merge_def )
        self.assertEqual( len( merge_def ), len( self.merge_def ),
                          "Wrong number of variables in merge def" )
        
        # Do column names match?
        cols = [ x.field_name for x in merge_def ]
        comparison_cols = [ x.field_name for x in self.merge_def ]
        self.assertListEqual( cols, comparison_cols, "Wrong column names found" )
        
        # Do right merge source names match?
        cols = [ x.right_field.field_name for x in merge_def ]
        comparison_cols = [ x.right_field.field_name for x in self.merge_def ]
        self.assertListEqual( cols, comparison_cols, "Wrong right-hand column names found" )
        
        # Do left merge source names match?
        cols = [ x.left_field.field_name for x in merge_def ]
        comparison_cols = [ x.left_field.field_name for x in self.merge_def ]
        self.assertListEqual( cols, comparison_cols, "Wrong left-hand column names found" )
        
        # Do right required keys match?
        cols = [ x.right_field.field_name for x in merge_def.required_merge_keys ]
        comparison_cols = [ x.right_field.field_name for x in self.merge_def.required_merge_keys ]
        self.assertListEqual( cols, comparison_cols, "Wrong right-hand column names found" )
        
        # Do left required keys match?
        cols = [ x.left_field.field_name for x in merge_def.required_merge_keys ]
        comparison_cols = [ x.left_field.field_name for x in self.merge_def.required_merge_keys ]
        self.assertListEqual( cols, comparison_cols, "Wrong left-hand column names found" )
        
        # Do right optional keys match?
        cols = [ x.right_field.field_name for x in merge_def.optional_merge_keys ]
        comparison_cols = [ x.right_field.field_name for x in self.merge_def.optional_merge_keys ]
        self.assertListEqual( cols, comparison_cols, "Wrong right-hand column names found" )
        
        # Do left optional keys match?
        cols = [ x.left_field.field_name for x in merge_def.optional_merge_keys ]
        comparison_cols = [ x.left_field.field_name for x in self.merge_def.optional_merge_keys ]
        self.assertListEqual( cols, comparison_cols, "Wrong left-hand column names found" )
        
        # Do right fuzzy keys match?
        cols = [ x.right_field.field_name for y in merge_def.fuzzy_merge_keys for x in y ]
        comparison_cols = [ x.right_field.field_name for y in self.merge_def.fuzzy_merge_keys for x in y ]
        self.assertListEqual( cols, comparison_cols, "Wrong right-hand column names found" )
        
        # Do left fuzzy keys match?
        cols = [ x.left_field.field_name for y in merge_def.fuzzy_merge_keys for x in y ]
        comparison_cols = [ x.left_field.field_name for y in self.merge_def.fuzzy_merge_keys for x in y ]
        self.assertListEqual( cols, comparison_cols, "Wrong left-hand column names found" )
        
    def test_missing_priority( self ):
        filename = os.path.join( self.run_context.tests_safe_dir, 'merge_spec_with_missing_priorities.xls' )
        merge_def = MergeDef( self.db_context )
        merge_def.table_name = 'merge_output'
        merge_def.left_input_table = self.data1
        merge_def.right_input_table = self.data2
        
        read_spec_file( filename, merge_def )
        self.assertEqual( len( merge_def ), len( self.merge_def ),
                          "Wrong number of variables in merge def" )
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testCreate']
    unittest.main()