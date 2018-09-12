'''
Created on May 28, 2013

@author: zschroeder
'''
import unittest
#import os.path

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.erasure import BookMapReader

_XLS_FILE = 'Bookmaplocations1.xls'
_SHEET_NAME = 'BookMap'

class TestBookMapReader(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.runContext = SuiteContext('unittest')
        cls.db_context = cls.runContext.getDBContext( tag='unittest' )
        clear_all( cls.db_context )
        cls.testDataDir = os.path.join( cls.runContext.tests_safe_dir ,"bookmapreader_test" )

        #setup the tables we will need in the database
        reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=os.path.join( cls.testDataDir,_XLS_FILE ),
                                 sheet_name=_SHEET_NAME,output_table='bookmaplocations')
        reader.createTable()
        
        cnt = 0
        for row in reader.getRows():
            tablename = row['Subject'] + '_' + row['Form_Values']
            filename_ = row["location"]
            bm_reader = SafeExcelReader(run_context=cls.runContext,db_context=cls.db_context,filename=filename_,
                                     sheet_name=_SHEET_NAME,output_table=tablename)
            bm_reader.createTable()
            cnt += 1
            #set the 'location' column to be the name of the table we created
            cls.db_context.executeNoResults("""
            UPDATE bookmaplocations
            set location='{0}'
            where subject='{1}' and form_values='{2}'
            """.format(tablename,row['subject'],row['form_values']))
            
            #get a list of table names to check
            cls.tables = []
            for row in cls.db_context.executeBuffered("SELECT [subject] + '_' + [form_values] from bookmaplocations"):
                cls.tables.append(row[0])
           
    def test_DB(self):      
        self.assertTrue(table_exists('bookmaplocations', self.db_context), 'Failure in construction of bookmaplocations table')
        for table in self.tables:
            self.assertTrue(table_exists(table, self.db_context), 'Failure in construction of ' + table + ' table')
    
    def test_BookmapsToDB(self):
        maps = BookMapReader( excel='Y',inputfile=os.path.join(self.testDataDir,_XLS_FILE),inputsheet=_SHEET_NAME,
                              read_to_db=True,db_context = self.db_context, outputTable='bookmaps' )
        self.assertTrue(table_exists('bookmaps', self.db_context), 'Error creating bookmaps table')
        self.assertIsNotNone(maps, 'Error reading bookmaps. None was returned')
    
    def test_BookMapValues(self):
        # test reading from DB 
        DBmaps = BookMapReader(excel='N', inputds='bookmaplocations', db_context=self.db_context)
        self.assertIsNotNone(DBmaps, 'Error reading bookmaps from DB')
        
        # test reading from XLS file
        XLSmaps = BookMapReader( excel='Y',inputfile=os.path.join(self.testDataDir,_XLS_FILE),inputsheet=_SHEET_NAME,
                                 read_to_db=False,db_context = self.db_context )
        
        self.assertIsNotNone(XLSmaps, 'Error reading bookmaps from XLS')
        
        # make sure results match
        maps1dict = {bm.subject + '_' + bm.form_values + '_' + str(len(bm.items)) : bm for bm in DBmaps}
        maps2dict = {bm.subject + '_' + bm.form_values + '_' + str(len(bm.items)) : bm for bm in XLSmaps}
        self.assertTrue(maps1dict.keys() == maps2dict.keys(), 'Error: bookmap values do not match')
                        
if __name__ == "__main__":
    unittest.main()