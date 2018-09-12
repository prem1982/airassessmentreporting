'''
Created on May 20, 2013

@author: temp_dmenes
'''

from numbers import Number
import os.path
import shutil
import unittest

from airassessmentreporting.airutility import ( clear_all, SafeExcelReader, dump )
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.merge import *


XLS_FILE = "merge_samples.xls"


class AbstractMergeTestCase( unittest.TestCase ):
    def setUp(self):
        self.run_context = SuiteContext('unittest')
        self.out_dir = os.path.join( self.run_context.tests_dir, 'merge' )
        if os.path.exists( self.out_dir ):
            shutil.rmtree( self.out_dir )
        os.makedirs( self.out_dir )
        self.db_context = self.run_context.getDBContext( 'unittest' )
        self.setUpData()
        self.setUpMerge()
        
    def setUpData(self):
        clear_all( self.db_context )
        reader = SafeExcelReader(
                run_context = self.run_context,
                db_context = self.db_context,
                filename = os.path.join( self.run_context.tests_safe_dir, XLS_FILE ),
                scan_all = True )
        reader.outputTable = reader.sheetName = "Data1"
        reader.createTable()
        reader.outputTable = reader.sheetName = "Data2"
        reader.createTable()
        
        self.data1 = self.db_context.getTableSpec( "Data1" )
        self.data2 = self.db_context.getTableSpec( "Data2" )

    def setUpMerge(self):
        self.merge_def = MergeDef( self.db_context )
        self.merge_def.table_name = 'merge_output'
        self.merge_def.left_input_table = self.data1
        self.merge_def.right_input_table = self.data2
        
        self.merge_def \
            .add( MergeFieldSpec( self.data1['barcode_num'],  self.data2['barcode_num'],  PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['barcode_char'], self.data2['barcode_char'], PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['studentid'],   self.data2['studentid'],     PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['gender'],       self.data2['gender'],       PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['studentlnm'],   self.data2['studentlnm'],   PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['studentfnm'],   self.data2['studentfnm'],   PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['Num_1'],        self.data2['Num_1'],        PRIORITY_LEFT_NONMISSING ) ) \
            .add( MergeFieldSpec( self.data1['Num_2'],        self.data2['Num_2'],        PRIORITY_RIGHT ) ) \
            .add( MergeFieldSpec( self.data1['Char_1'],       self.data2['Char_1'],       PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['Char_2'],       self.data2['Char_2'],       PRIORITY_RIGHT ) ) \
            .add( MergeFieldSpec( self.data1['N1'],           self.data2['N1'],           PRIORITY_LEFT ) ) \
            .add( MergeFieldSpec( self.data1['N2'],           self.data2['N2'],           PRIORITY_RIGHT ) )
            
        self.merge_def.required_merge_keys = [ self.merge_def[ 'barcode_num' ] ]
        self.merge_def.optional_merge_keys = [ self.merge_def['studentid'], self.merge_def['gender'] ]
        self.merge_def.fuzzy_merge_keys    = [ ( self.merge_def['studentlnm'], self.merge_def['studentfnm'] ) ]
        self.merge_def.fuzzy_report_table  = 'fuzzy_report'
        self.merge_def.left_remain_table = 'left_remainder'
        self.merge_def.right_remain_table = 'right_remainder'
        
    def tearDown(self):
        self.run_context.close()
