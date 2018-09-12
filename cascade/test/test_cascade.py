'''
Created on May 28, 2013

@author: zschroeder
'''
import unittest
#import os.path

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.cascade import *

_XLS_FILE = 'AggSheet.xls'
_SHEET_NAME = 'cascade_test'
_DATAFILE_ = 'studentg3_n.txt'
_ODBC_CONN = 'unittest'

class TestCascade(unittest.TestCase):
    """ Note: This needs an ODBC connection pointing to your unittest DB to be setup. It is called 'unittest' in this file.
    """
    @classmethod
    def setUpClass(cls):
        cls.runContext = SuiteContext('unittest')
        cls.db_context = cls.runContext.getDBContext( tag='unittest' )
        clear_all( cls.db_context )
        cls.testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"cascade_test" )
        
    def test_Cascade(self):
        #setup the table we will need in the database
        reader = SafeExcelReader(run_context=self.runContext,db_context=self.db_context,scan_all=True,delimiter='|',
                                 filename=os.path.join( self.testDataDir,_DATAFILE_ ),output_table='studentg3_n')
        reader.createTable()
        
        self.assertTrue(table_exists('studentg3_n', self.db_context),'ERROR: output table studentg3_n was not created') 
        
        cascade(excel='Y', agg_file=os.path.join(self.testDataDir, _XLS_FILE),agg_sheet=_SHEET_NAME, 
                inputds='studentg3_n', db_context=self.db_context,odbcconn=_ODBC_CONN)
        
        self.assertTrue(table_exists('cascade_Class_R', self.db_context),'ERROR: output table cascade_Class_R was not created')
        self.assertTrue(table_exists('cascade_district_R', self.db_context),'ERROR: output table cascade_district_R was not created')
        self.assertTrue(table_exists('cascade_school_R', self.db_context),'ERROR: output table cascade_school_R was not created')
        
if __name__ == "__main__":
    unittest.main()
