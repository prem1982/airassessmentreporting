'''
Created on May 17, 2013

@author: temp_dmenes
'''
import unittest
import os

from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.airutility.dbutilities import *
from airassessmentreporting.airutility import SafeExcelReader, TableSpec

_XLS_FILE = r'merge_samples.xls'

class Test(unittest.TestCase):


    def setUp(self):
        self.runContext = SuiteContext('unittest')
        self.db_context = self.runContext.getDBContext( tag='unittest' )
        clear_all( self.db_context )
        self.reader = SafeExcelReader( self.runContext )
        self.reader.db_context = self.db_context
        self.testDataDir = self.runContext.tests_safe_dir
        self.reader.filename = os.path.join( self.testDataDir, _XLS_FILE )
        self.reader.sheetName = "Data1"
        self.reader.outputTable = "Data1"
        self.reader.createTable()

    
    def test_n_obs(self):
        n = n_obs( 'data1', self.db_context )
        self.assertEqual( 300, n, "Got wrong number of observations: {} instead of 300".format(n))
        
    def test_get_tablespec_1(self):
        '''Does a tablespec get returned as is
        '''
        t = self.db_context.getTableSpec("Data1")
        t2 = get_table_spec(t)
        self.assertTrue( t is t2, "Did not return the same tablespec" )
        
    def test_get_tablespec_2(self):
        '''Does it work to specify table name and db_context?
        '''
        t = get_table_spec( 'some_table',self.db_context )
        self.assertTrue( isinstance( t, TableSpec ), "Did not return a TableSpec object" )
        self.assertEquals( t.table_name, '[some_table]', "Did not return the right name" )
        self.assertEquals( t.table_schema, self.db_context.schema, "Did not return the correct schema" )
        
    def test_get_tablespec_3(self):
        '''Can we override the schema?
        '''
        t = get_table_spec( 'some_table',self.db_context, 'another_schema' )
        self.assertTrue( isinstance( t, TableSpec ), "Did not return a TableSpec object" )
        self.assertEquals( t.table_name, '[some_table]', "Did not return the right name" )
        self.assertEquals( t.table_schema, '[another_schema]', "Did not return the correct schema" )
        
    def test_get_tablespec_4(self):
        '''Throw error if db_context conflicts
        '''
        another_context = self.runContext.getDBContext()
        t1 = self.db_context.getTableSpec("Data1")
        try:
            t2 = get_table_spec( t1, another_context )
        except ValueError as e:
            return
        self.fail( "Did not throw expected error on conflicting db_context" )
        
    def test_get_tablespec_5(self):
        '''Throw error if schema conflicts
        '''
        t1 = self.db_context.getTableSpec("Data1")
        try:
            t2 = get_table_spec( t1, self.db_context, 'another_schema' )
        except ValueError as e:
            return
        self.fail( "Did not throw expected error on conflicting schema" )
    
    def test_get_tablespec_6(self):
        '''Correctly normalizing case
        '''
        t = get_table_spec( 'some_TABLE',self.db_context, 'another_SCHEMA' )
        self.assertTrue( isinstance( t, TableSpec ), "Did not return a TableSpec object" )
        self.assertEquals( t.table_name, '[some_table]', "Did not return the right name" )
        self.assertEquals( t.table_schema, '[another_schema]', "Did not return the correct schema" )
        
    def test_get_tablespec_7(self):
        '''Throws error if no db_context specified
        '''
        try:
            t = get_table_spec( 'some_table' )
        except ValueError as e:
            return
        self.fail( "Did not throw expected error on missing db_context" )
        
    def test_get_tablespec_8(self):
        '''No error as long as db context matches
        '''
        t = self.db_context.getTableSpec("Data1")
        t2 = get_table_spec(t)
        self.assertTrue( t is t2, "Did not return the same tablespec" )
        
    def test_get_tablespec_9(self):
        '''No error as long as schema matches
        '''
        t = self.db_context.getTableSpec("Data1", table_schema=self.db_context.schema )
        t2 = get_table_spec(t)
        self.assertTrue( t is t2, "Did not return the same tablespec" )
        
    def test_get_column_names(self):
        cols = get_column_names( 'data1', self.db_context )
        self.assertListEqual(cols,
                ['[barcode_num]','[barcode_char]','[studentid]','[gender]',
                 '[ethnicity]','[studentlnm]','[studentfnm]','[num_1]',
                 '[num_2]','[char_1]','[char_2]','[n1]','[n2]','[import_order]'
                 ], "Did not return correct column names" )
        
    def test_get_table_names1(self):
        tables = get_table_names( self.db_context )
        self.assertListEqual(tables,
                ['[data1]'], "Did not return correct table names" )
    
    def test_get_table_names2(self):
        '''Should not return any tables from another schema
        '''
        tables = get_table_names( self.db_context, table_schema="another_schema" )
        self.assertListEqual(tables,
                [], "Did not return correct table names" )
    
    def test_get_table_names3(self):
        '''Should work with quoted schema name
        '''
        tables = get_table_names( self.db_context, table_schema="[dbo]" )
        self.assertListEqual(tables,
                ['[data1]'], "Did not return correct table names" )
    
    def test_get_table_names4(self):
        '''Should work with unquoted schema name
        '''
        tables = get_table_names( self.db_context, table_schema="dbo" )
        self.assertListEqual(tables,
                ['[data1]'], "Did not return correct table names" )
    
    def test_clear_all(self):
        # Create a table with a foreign key constraint
        query = "CREATE TABLE my_table( key1 bigint, var1 VARCHAR(17), FOREIGN KEY( key1 ) REFERENCES data1( import_order ) )"
        self.db_context.executeNoResults(query)
        tables = get_table_names( self.db_context )
        self.assertTrue('[data1]' in tables, "Did not return correct table names" )
        self.assertTrue('[my_table]' in tables, "Did not return correct table names" )
        self.assertEqual(len( tables ), 2, "Did not return correct table names" )
        clear_all( self.db_context )
        tables = get_table_names( self.db_context )
        self.assertListEqual(tables,
                [], "Did not return correct table names" )
        
    def test_table_exists(self):
        self.assertTrue( table_exists( 'data1', self.db_context ),
                         "Failed to find table that exists" )
        self.assertFalse( table_exists( 'zaxxiz', self.db_context ),
                         "Found table that does not exist" )
        
    def test_drop_table_if_exists1(self):
        self.assertTrue( table_exists( 'data1', self.db_context ),
                "Failed to find table that was supposed to exist" )
        t = self.db_context.getTableSpec('data1')
        drop_table_if_exists( t )
        self.assertFalse( table_exists( t ),
                "Table was supposed to be dropped, but its still there" )

    def test_drop_table_if_exists2(self):
        '''Different schema
        '''
        t = self.db_context.getTableSpec('data1')
        t.table_schema = 'not_my_schema'
        drop_table_if_exists( t )
        self.assertTrue( table_exists( 'data1', self.db_context ),
                "Failed to find table that was supposed to exist" )

    def test_drop_table_if_exists3(self):
        '''Different name
        '''
        t = self.db_context.getTableSpec('data2')
        drop_table_if_exists( t )
        self.assertTrue( table_exists( 'data1', self.db_context ),
                "Failed to find table that was supposed to exist" )
        
    def test_assembly_exists(self):
        '''Assumes that the assembly has been created by running the prep_sqlserver
        script
        '''
        self.assertTrue( assembly_exists( 'ToProperCase', self.db_context ) )
        self.assertFalse( assembly_exists( 'asdfasdf', self.db_context ) )
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()