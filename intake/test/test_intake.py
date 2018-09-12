'''
Created on May 21, 2013

@author: zschroeder
'''

import unittest
#import os.path

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.intake import *

_LAYOUT_FILE = r'relevantcourse_layout.xlsx'
_DATA_INFILE = r'TLES_RELEVANT_CRS_LIST.xlsx'
_DATA_FILE_CSV = r'TLES_RELEVANT_CRS_LIST.csv'
_LAYOUT_FILE_FIXED = r'intakeLayoutSimple.xlsx'
_DATA_FILE_FIXED = r'intakeDataSimple.txt'
_SHEET_NAME = 'Sheet1'

class TestMeans(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.runContext = SuiteContext('unittest')
        cls.db_context = cls.runContext.getDBContext( tag='unittest' )
        clear_all( cls.db_context )
        cls.testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"intake_test" )
        
    def setUp(self):
        #copy data from .61 to unittest DB
        clear_all(self.db_context)
        
    def test_XLS(self):
        layoutfilename = os.path.join( self.testDataDir, _LAYOUT_FILE )
        infilefilename = os.path.join( self.testDataDir, _DATA_INFILE )
        
        Intake(Mode='RUN',Type_of_Datafile='EXcEl',Layoutfile=layoutfilename,
               Sheetname=_SHEET_NAME,Infile=infilefilename,Infile_Excel_Sheetname=_SHEET_NAME,
               Outdata='relevCourseOut',Error_Ds='relevCourseError',Getnames='Yes',Overwrite='Yes', 
               db_context=self.db_context)

        self.assertTrue(table_exists('relevCourseOut', self.db_context),'output table relevCourseOut was not created')
        self.assertTrue(table_exists('relevCourseError', self.db_context),'error table relevCourseError was not created')
        
    def test_Delimited(self):
        layoutfilename = os.path.join( self.testDataDir, _LAYOUT_FILE )
        infilefilename = os.path.join( self.testDataDir, _DATA_FILE_CSV )
        
        Intake(Mode='RUN',Type_of_Datafile='deliMIteD',Layoutfile=layoutfilename,
               Sheetname=_SHEET_NAME,Infile=infilefilename,Outdata='relevCourseOut',
               Error_Ds='relevCourseError',Getnames='Yes',Overwrite='Yes',db_context=self.db_context)
        
        self.assertTrue(table_exists('relevCourseOut', self.db_context),'output table relevCourseOut was not created')
        self.assertTrue(table_exists('relevCourseError', self.db_context),'error table relevCourseError was not created')
        
    def test_FixedWidth(self):
        layoutfilename = os.path.join( self.testDataDir, _LAYOUT_FILE_FIXED )
        infilefilename = os.path.join( self.testDataDir, _DATA_FILE_FIXED )
        
        Intake(Mode='RUN',Type_of_Datafile='FixedWidth',Layoutfile=layoutfilename,
               Sheetname=_SHEET_NAME,Infile=infilefilename,Outdata='relevCourseOut',
               Error_Ds='relevCourseError',Getnames='Yes',Overwrite='Yes',db_context=self.db_context)
        
        self.assertTrue(table_exists('relevCourseOut', self.db_context),'output table relevCourseOut was not created')
        self.assertTrue(table_exists('relevCourseError', self.db_context),'error table relevCourseError was not created')
        
if __name__ == "__main__":
    unittest.main()