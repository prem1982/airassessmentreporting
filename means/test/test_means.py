'''
Created on May 21, 2013

@author: zschroeder
'''

import unittest
#import os.path

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.means import *

_XLS_FILE = 'SpecSheet.xls'
_MEANS_SHEET = 'Means'
_PERCENTS_SHEET = 'Percent'
_DATA_FILE = 'studentg3_n.txt'

class TestMeans(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.runContext = SuiteContext('unittest')
        cls.db_context = cls.runContext.getDBContext( tag='unittest' )
        clear_all( cls.db_context )
        cls.testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"means_test" )

        #setup the tables we will need in the database
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=os.path.join( cls.testDataDir,_XLS_FILE ),
                                 sheet_name=_MEANS_SHEET,output_table=_MEANS_SHEET)
        reader.createTable()
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=os.path.join( cls.testDataDir,_XLS_FILE ),
                                 sheet_name=_PERCENTS_SHEET,output_table=_PERCENTS_SHEET)
        reader.createTable()
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=os.path.join( cls.testDataDir,_DATA_FILE ),
                                 scan_all=True,delimiter='|',buffer_size=100,output_table='studentg3_n')
        reader.createTable()
        
        
    def setUp(self):
        #drop tables we will create if they already exist
        drop_table_if_exists('Mean_BCRXID', self.db_context)
        drop_table_if_exists('Mean_DCRXID', self.db_context)
        drop_table_if_exists('Mean_RCLASS_ID', self.db_context)
        drop_table_if_exists('Mean_RTEACHER_ID', self.db_context)
        drop_table_if_exists('Mean_SCRXID', self.db_context)
        
        
    def test_dataTables(self):
        self.assertTrue(table_exists(_MEANS_SHEET, self.db_context), 'Failure in construction of Means table')
        self.assertTrue(table_exists(_PERCENTS_SHEET, self.db_context), 'Failure in construction of Percent table')
        self.assertTrue(table_exists('studentg3_n', self.db_context), 'Failure in construction of studentg3_n (data) table')
        
    def test_XLS(self):
        filename = os.path.join( self.testDataDir, _XLS_FILE )
        meansclass = Means(db_context=self.db_context,inputds='studentg3_n',agg_file=filename,
                           agg_sheet=_MEANS_SHEET,percent='Y',percent_file=filename,percent_sheet=_PERCENTS_SHEET,
                           overwrite='N')
        self.assertIsNotNone(meansclass,'Construction of Means object failed')
        meansclass.execute()
        self.assertTrue(table_exists('Mean_BCRXID', self.db_context),'output table Mean_BCRXID was not created')
        self.assertTrue(table_exists('Mean_DCRXID', self.db_context),'output table Mean_DCRXID was not created')
        self.assertTrue(table_exists('Mean_RCLASS_ID', self.db_context),'output table Mean_RCLASS_ID was not created')
        self.assertTrue(table_exists('Mean_RTEACHER_ID', self.db_context),'output table Mean_RTEACHER_ID was not created')
        self.assertTrue(table_exists('Mean_SCRXID', self.db_context),'output table Mean_SCRXID was not created')
        
    def test_DB(self):
        meansclass = Means(excel='N',db_context=self.db_context,inputds='studentg3_n',agg_ds=_MEANS_SHEET,
                           percent='Y',percent_ds=_PERCENTS_SHEET,overwrite='N')
        self.assertIsNotNone(meansclass,'Construction of Means object failed')
        meansclass.execute()
        
        self.assertTrue(table_exists('Mean_BCRXID', self.db_context),'output table Mean_BCRXID was not created')
        self.assertTrue(table_exists('Mean_DCRXID', self.db_context),'output table Mean_DCRXID was not created')
        self.assertTrue(table_exists('Mean_RCLASS_ID', self.db_context),'output table Mean_RCLASS_ID was not created')
        self.assertTrue(table_exists('Mean_RTEACHER_ID', self.db_context),'output table Mean_RTEACHER_ID was not created')
        self.assertTrue(table_exists('Mean_SCRXID', self.db_context),'output table Mean_SCRXID was not created')
        
if __name__ == "__main__":
    unittest.main()