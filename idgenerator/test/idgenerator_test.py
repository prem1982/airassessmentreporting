'''
Created on May 29, 2013

@author: temp_dmenes
'''

import os
import unittest

from airassessmentreporting.airutility import SafeExcelReader, table_exists, get_temp_table
from airassessmentreporting.testutility import SuiteContext, compare_tables, integer_compare, mixed_compare
from airassessmentreporting.idgenerator import IDGenerator


_XLS_FILE = 'student_g3_narrow.xlsb'
SUBJECT_LST       = [ 'r', 'm' ]
TEACHER_LST       = [ 'uc{}x_teachername'.format( subject ) for subject in SUBJECT_LST ]
CLASS_LST         = [ 'uc{}x_classname'    .format( subject ) for subject in SUBJECT_LST ]
SECTION_LST       = [ 'uc{}x_section_num'.format( subject ) for subject in SUBJECT_LST ]
CLASS_LABEL_LST   = [ '{}label'          .format( subject ) for subject in SUBJECT_LST ]
CLASS_ID_LST      = [ '{}class_id'       .format( subject ) for subject in SUBJECT_LST ]
TEACHER_LABEL_LST = [ 'ic{}xnm'          .format( subject ) for subject in SUBJECT_LST ]
TEACHER_ID_LST    = [ '{}teacher_id'   .format( subject ) for subject in SUBJECT_LST ]

COLUMNS=[
    ( 'ssid', 'ssid', mixed_compare ),
    ( 'serial_number', 'serial_number', integer_compare ),
    ( 'dcrxid_attend', 'dcrxid_attend', integer_compare ),
    ( 'ucmx_classname', 'ucmx_classname', mixed_compare ),
    ( 'ucmx_section_num', 'ucmx_section_num', mixed_compare ),
    ( 'ucmx_teachername', 'ucmx_teachername', mixed_compare ),
    ( 'ucrx_classname', 'ucrx_classname', mixed_compare ),
    ( 'ucrx_section_num', 'ucrx_section_num', mixed_compare ),
    ( 'ucrx_teachername', 'ucrx_teachername', mixed_compare ),
    ( 'grade', 'grade', mixed_compare ),
    ( 'rteacher_id', 'rteacher_id', mixed_compare ),
    ( 'rclass_id', 'rclass_id', mixed_compare ),
    ( 'icrxnm', 'icrxnm', mixed_compare ),
    ( 'rlabel', 'rlabel', mixed_compare ),
    ( 'mteacher_id', 'mteacher_id', mixed_compare ),
    ( 'mclass_id', 'mclass_id', mixed_compare ),
    ( 'icmxnm', 'icmxnm', mixed_compare ),
    ( 'mlabel', 'mlabel', mixed_compare ),

]

class IDGeneratorTest( unittest.TestCase ):
    def setUp( self ):
        self.run_context = SuiteContext("unittest")
        self.db_context = self.run_context.getDBContext("unittest")
        self.static_context = self.run_context.getDBContext("static")
        self.source_data_dir = self.run_context.getConfigFile("TESTS", "id_generator_test_source_data_dir",
              "%(tests_safe_dir)s/id_generator_test/source_data")
        if not table_exists( 'student_g3', self.static_context ):
            source_file = os.path.join( self.source_data_dir, _XLS_FILE )
            reader = SafeExcelReader(self.run_context, source_file, "Sheet1", 'student_g3', self.static_context, scan_all=True )
            reader.createTable()

        self.answer_dir = os.path.join( self.run_context.logs_dir, 'id_generator_test' )
        if not os.path.exists( self.answer_dir ):
            os.makedirs( self.answer_dir )
        
        self.specimen_dir = os.path.join( self.run_context.tests_safe_dir,
                                      'id_generator_test', 'sas_outputs' )

    
    def test_1( self ):
        # Replicates the SAS test 1
        g3 = self.static_context.getTableSpec( 'student_g3' )
        with get_temp_table( self.db_context ) as in_ds:
            query = "SELECT TOP 100 * INTO {in_ds:qualified} FROM {g3:qualified}".format( in_ds=in_ds, g3=g3 )
            self.db_context.executeNoResults(query)
            query = "UPDATE {} REPLACE [ucrx_teachername]='---CLARK PATRISE---' WHERE [import_order]=98"
            out_ds = self.db_context.getTableSpec('g3_ready')
            id_generator = IDGenerator(ds_in             = in_ds,
                                       ds_out            = out_ds,
                                       db_context        = self.db_context,
                                       grade_var         = 'grade',
                                       district_var      = 'dcrxid_attend',
                                       school_var        = 'bcrxid_attend',
                                       subject_char_lst  = SUBJECT_LST,
                                       teacher_var_lst   = TEACHER_LST,
                                       teacher_label_lst = TEACHER_LABEL_LST,
                                       teacher_id_lst    = TEACHER_ID_LST,
                                       class_var_lst     = CLASS_LST,
                                       section_var_lst   = SECTION_LST,
                                       class_label_lst   = CLASS_LABEL_LST,
                                       class_id_lst      = CLASS_ID_LST,
                                       test_date         = '0509',
                                       err_var_name      = 'errvar')
            id_generator.execute()
        
        key_function = lambda row: ( row.studentid if row.studentid is not None else 0,
                                     int( row.serial_number ),
                                     row.ssid if row.ssid is not None else '' )
        
        answer_dir = os.path.join( self.answer_dir, 'test_10' )
        if not os.path.exists( answer_dir ):
            os.makedirs( answer_dir )
        answer_file = os.path.join( answer_dir, 'comparison.log' )
        result = compare_tables(
                answer_file,
                table="g3_ready",
                specimen_name= os.path.join( self.specimen_dir, 'G3_READY.XLS' ),
                columns=COLUMNS,
                table_key_function=key_function,
                specimen_key_function=key_function,
                db_context=self.db_context)
        self.assertTrue( result, "Table comparison failed. See log in {}".format( answer_file ) )