'''
Created on May 24, 2013

@author: zschroeder
'''
import unittest
#import os.path

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.converttopdf import *

_XLS_FILE = 'SpecSheet.xls'
_MEANS_SHEET = 'Means'
_SQL_PDF_OUTPUT = 'sqlpdfoutput.pdf'
_XLS_PDF_OUTPUT_NOSTRETCH = 'xlspdfoutput_1.pdf'
_XLS_PDF_OUTPUT_STRETCH = 'xlspdfoutput_2.pdf'

class TestConvertToPDF(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.runContext = SuiteContext('unittest')
        cls.db_context = cls.runContext.getDBContext( tag='unittest' )
        clear_all( cls.db_context )
        cls.testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"converttopdf_test" )

        # remove the files we will output if they already exist
        if os.path.exists(os.path.join(cls.testDataDir,_SQL_PDF_OUTPUT)):
            os.remove(os.path.join(cls.testDataDir,_SQL_PDF_OUTPUT))
        if os.path.exists(os.path.join(cls.testDataDir,_XLS_PDF_OUTPUT_NOSTRETCH)):
            os.remove(os.path.join(cls.testDataDir,_XLS_PDF_OUTPUT_NOSTRETCH))
        if os.path.exists(os.path.join(cls.testDataDir,_XLS_PDF_OUTPUT_STRETCH)):
            os.remove(os.path.join(cls.testDataDir,_XLS_PDF_OUTPUT_STRETCH))
        
    def test_TableToPdf(self):
        #setup the table we will need in the database
        reader = SafeExcelReader(run_context=self.runContext,db_context=self.db_context,filename=os.path.join( self.testDataDir,_XLS_FILE ),
                                 sheet_name=_MEANS_SHEET,output_table='Means')
        reader.createTable()  
        self.assertTrue(table_exists('Means', self.db_context), 'Failure in construction of Means table')
        
        #now test the function
        colnames = ["*"]
        ConvertSQLtoPDF(colnames, self.db_context,tablename="Means",outputname=os.path.join(self.testDataDir,_SQL_PDF_OUTPUT))
        
        self.assertTrue(os.path.exists(os.path.join(self.testDataDir,_SQL_PDF_OUTPUT)),'Error: PDF Output file {0} does not exist'.format(_SQL_PDF_OUTPUT))
        
    def test_XlsToPdfWithoutStretch(self):
        filepath = os.path.join(self.testDataDir,_XLS_PDF_OUTPUT_NOSTRETCH)
        infilepath = os.path.join(self.testDataDir,_XLS_FILE)
        ConvertXLStoPDF(infilepath, filepath)
        
        self.assertTrue(os.path.exists(filepath),'Error: PDF Output file {0} does not exist'.format(filepath))
       
    def test_XlsToPdfWithStretch(self):
        filepath = os.path.join(self.testDataDir,_XLS_PDF_OUTPUT_STRETCH)
        infilepath = os.path.join(self.testDataDir,_XLS_FILE)
        ConvertXLStoPDF(infilepath, filepath)
        
        self.assertTrue(os.path.exists(filepath),'Error: PDF Output file {0} does not exist'.format(filepath))
         
if __name__ == "__main__":
    unittest.main()