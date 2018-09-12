'''
Created on Jun 13, 2013

@author: zschroeder
'''
import unittest
#import os.path

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.peercompare import *

_SAS_FILE = 'pcdata.txt'
_DATA_FILE = 'studentg10.txt'
_SIMILAR_TABLE = 'similardist'
_DATA_TABLE = "stud10"
_AGG_FILE = "AggregationSheet.xls"
_AGG_TABLE = 'AggregationSheet'

class TestPeerCompare(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.runContext = SuiteContext('unittest')
        cls.db_context = cls.runContext.getDBContext( tag='unittest' )
        clear_all( cls.db_context )
        cls.testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"peercompare_test" )

        #setup the tables we will need in the database
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=os.path.join( cls.testDataDir,_SAS_FILE ),
                                 output_table=_SIMILAR_TABLE, delimiter='|')
        reader.createTable()        
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=os.path.join( cls.testDataDir,_DATA_FILE ),
                                 output_table=_DATA_TABLE)
        reader.createTable()  
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=os.path.join( cls.testDataDir,_AGG_FILE ),
                                 sheet_name='peerCompare',output_table=_AGG_TABLE)
        reader.createTable()  
        
    def test_dataTables(self):
        self.assertTrue(table_exists(_SIMILAR_TABLE, self.db_context), 'Failure in construction of SimilarDist table')
        self.assertTrue(table_exists(_DATA_TABLE, self.db_context), 'Failure in construction of data table')
        self.assertTrue(table_exists(_AGG_TABLE, self.db_context), 'Failure in construction of Aggregate Data table')
        
    def test_XLS(self):
        peer_compare( excel='Y', agg_file=os.path.join( self.testDataDir ,_AGG_FILE ), agg_sheet='PeerCompare', indata=_DATA_TABLE, outdata='peerCompare', 
                      pc_data=_SIMILAR_TABLE, crit_val=1.96, db_context=self.db_context, odbcconn='Scratch' )
        
        self.assertTrue(table_exists('peerCompare', self.db_context),'output table peerCompare was not created')
        
    def test_DB(self):
        peer_compare( excel='N', agg_table=_AGG_TABLE, indata=_DATA_TABLE, outdata='peerCompare', 
                      pc_data=_SIMILAR_TABLE, crit_val=1.96, db_context=self.db_context, odbcconn='Scratch' )
        
        self.assertTrue(table_exists('peerCompare', self.db_context),'output table peerCompare was not created')
        
if __name__ == "__main__":
    unittest.main()