'''
Created on Apr 30, 2013

@author: temp_dmenes
'''
import unittest

from airassessmentreporting.airutility import ( get_table_names )
from abstractmergetestcase import AbstractMergeTestCase
from airassessmentreporting.merge import *

class Test( AbstractMergeTestCase ):


    def test1(self):
        self.merge_def.execute()
        self.assertTrue( self.merge_def.table_name in get_table_names( self.db_context ),
                         "Did not find merge output table on server!!" )

    def testLeft(self):
        self.merge_def.join_type = JOIN_TYPE_LEFT
        self.merge_def.table_name = "left_output"
        self.merge_def.execute()
        self.assertTrue( self.merge_def.table_name in get_table_names( self.db_context ),
                         "Did not find merge output table on server!!" )

    def testFull(self):
        self.merge_def.join_type = JOIN_TYPE_FULL
        self.merge_def.table_name = "full_output"
        self.merge_def.execute()
        self.assertTrue( self.merge_def.table_name in get_table_names( self.db_context ),
                         "Did not find merge output table on server!!" )

    def testRequiredPrimaryKey(self):
        ## Tests the situation where the table's primary keys are marked as "required keys"
        self.merge_def.join_type = JOIN_TYPE_FULL
        self.merge_def.table_name = "required_primary_output"
        self.merge_def.add( MergeFieldSpec( self.data1['import_order'], self.data2['import_order'], PRIORITY_LEFT_NONMISSING ) )
        self.merge_def.required_merge_keys.append( self.merge_def[ 'import_order' ] )
        
        self.merge_def.execute()
        self.assertTrue( self.merge_def.table_name in get_table_names( self.db_context ),
                         "Did not find merge output table on server!!" )

    def testNoMoreKeys(self):
        ## Tests the situation where there are no optional or fuzzy keys
        self.merge_def.join_type = JOIN_TYPE_FULL
        self.merge_def.table_name = "no_secondary_fuzzy_output"
        del self.merge_def.optional_merge_keys[:]
        del self.merge_def.fuzzy_merge_keys[:]
        self.merge_def.execute()
        self.assertTrue( self.merge_def.table_name in get_table_names( self.db_context ),
                         "Did not find merge output table on server!!" )


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()