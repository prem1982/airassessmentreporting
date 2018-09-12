'''
Created on Apr 26, 2013

@author: temp_dmenes
'''
import unittest
from airassessmentreporting.airutility import RunContext


class Test(unittest.TestCase):


    def setUp(self):
        self.context = RunContext( 'unittest' )
        self.db_context = self.context.getDBContext( tag='unittest' )

    def tearDown(self):
        pass
    
    def testCRUD(self):
        ctx = self.db_context
        ctx.executeNoResults( "CREATE TABLE testtable( col1 VARCHAR(255) )" )
        ctx.executeNoResults( "INSERT INTO testtable( col1 ) VALUES ( 'ABCDE' )" )
        results = [ row[0] for row in ctx.execute( "SELECT col1 FROM testtable" ) ]
        n = len( results )
        self.assertEquals( n, 1, "Wrong number of results {0}".format( n ) )
        self.assertEquals( results[0], "ABCDE", "Wrong result {0}".format( results[ 0 ] ) )
        ctx.executeNoResults( "DROP TABLE testtable" )
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
