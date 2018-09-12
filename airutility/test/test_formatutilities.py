'''
Created on May 1, 2013

@author: temp_dmenes
'''

import unittest
from airassessmentreporting.airutility import Joiner

class Test(unittest.TestCase):
    def testJoiner(self):
        cols = Joiner( ['column_1', 'column_2', 'column_3'] )
        out = "SELECT {cols:delimiter=','} FROM {table}".format( cols=cols , table="table_1" )
        self.assertEqual( 'SELECT column_1,column_2,column_3 FROM table_1', out, 'Wrong output \'{}\''.format( out ) )
        
    def testJoinerMultipleSequences(self):
        cols = Joiner( [ 'column_1 VARCHAR(255)', 'column_2 VARCHAR(255)', 'column_3 VARCHAR(255)' ], [], [ 'PRIMARY KEY(column_1)' ] )
        out = "CREATE TABLE {table}({cols:delimiter=','})".format( cols=cols , table="table_1" )
        self.assertEqual( 'CREATE TABLE table_1(column_1 VARCHAR(255),column_2 VARCHAR(255),column_3 VARCHAR(255),PRIMARY KEY(column_1))',
                          out, 'Wrong output \'{}\''.format( out ) )

    def testJoinerItemFormat(self):
        cols = Joiner( ['column_1', 'column_2'] )
        out = """SELECT {cols:delimiter=',',
                        item='col',
                        itemfmt='{alias}.{{col}}'} FROM {table} AS {alias}""".format( cols=cols , table="table_1", alias="A" )
        self.assertEqual( 'SELECT A.column_1,A.column_2 FROM table_1 AS A',
                          out, 'Wrong output \'{}\''.format( out ) )
