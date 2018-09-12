'''
Created on May 28, 2013

@author: zschroeder
'''
import unittest

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.erasure import *

_XLS_FILE = 'Bookmaplocations1.xls'
_SHEET_NAME = 'BookMap'
_DATAFILE_1 = 'AIR1.csv'
_DATAFILE_2 = 'AIR2.csv'

class TestErasure(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.runContext = SuiteContext('unittest')
        cls.db_context = cls.runContext.getDBContext( tag='unittest' )
        clear_all( cls.db_context )
        cls.testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"erasure_test" )
        cls.bm_testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"bookmapreader_test" )

        #setup the tables we will need in the database
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,buffer_size=100,scan_all=True,
                                 filename=os.path.join( cls.testDataDir,_DATAFILE_1 ),output_table='AIR1',
                                 range_=(0,0,500,1024))
        reader.createTable()
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,buffer_size=100,scan_all=True,
                                 filename=os.path.join( cls.testDataDir,_DATAFILE_2 ),output_table='AIR2',
                                 range_=(0,0,500,1024))
        reader.createTable()   
        
    def setUp(self):
        self.XLSmaps = BookMapReader(excel='Y',inputfile=os.path.join(self.bm_testDataDir,_XLS_FILE),inputsheet=_SHEET_NAME)
        self.assertIsNotNone(self.XLSmaps, 'Error reading bookmaps from XLS')
    
    def test_DataTables(self):
        self.assertTrue(table_exists('AIR1', self.db_context),'ERROR: output table AIR1 was not created')
        self.assertTrue(table_exists('AIR2', self.db_context),'ERROR: output table AIR2 was not created') 
        
    def test_Erasure(self):
        erasure(self.db_context, inputds1="AIR1",inputds2="AIR2", bookmaps=self.XLSmaps,outputds='erasure_out')
        self.assertTrue(table_exists('erasure_out', self.db_context),'ERROR: output table erasure_out was not created')
        
if __name__ == "__main__":
    unittest.main()