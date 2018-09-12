'''
Created on Jul 8, 2013

@author: temp_plakshmanan
'''

import unittest
import lxml.etree as et
from cStringIO import StringIO
import cProfile
import pstats
import os
import tempfile

from airassessmentreporting.airutility import FieldSpec, TableSpec, drop_table_if_exists, get_temp_table, FastTableStream
from airassessmentreporting.airutility.fasttablestream import *
from airassessmentreporting.airutility.fasttablestream_processors import NVarcharProcessor
from airassessmentreporting.testutility import XMLTest, SuiteContext

class TestPreQC( unittest.TestCase ):
    
    def setUp(self):
        
        self.run_context = SuiteContext( 'unittest' )
        self.db_context = self.run_context.getDBContext( 'unittest' )
        self.LOGGER = self.run_context.get_logger()
        
        
    def tearDown(self):
        pass
        
    def runTest(self):
        pass

    