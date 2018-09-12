######################################################################
#
# (c) Copyright American Institutes for Research, unpublished work created 2013
#  All use, disclosure, and/or reproduction of this material is
#  prohibited unless authorized in writing. All rights reserved.
#
#  Rights in this program belong to:
#   American Institutes for Research.
#
######################################################################

#
# TODO - documentation
#

"""
Created on June 7, 2013

@author: temp_tprindle
"""

import csv
import filecmp

import os
import unittest
from airassessmentreporting import complementarymerge

from airassessmentreporting.airutility import dbutilities, SafeExcelReader
from airassessmentreporting.complementarymerge import ComplementaryMerge, FLAT_TABLE_KEY_NAME
from airassessmentreporting.testutility.suite_context import SuiteContext

CONTEXT_NAME = 'unittest'
UNITTEST_SUBDIR = 'complementary_merge'

MERGESPEC_FILE_VALID = os.path.join( UNITTEST_SUBDIR, 'complementary_merge_TestData1_SpecSheet.xls' )
MERGESPEC_FILE_MISSING_COLUMN = os.path.join( UNITTEST_SUBDIR, 'complementary_merge_mergespec_missing_column.xls' )
MERGESPEC_FILE_INVALID_COLUMN = os.path.join( UNITTEST_SUBDIR, 'complementary_merge_mergespec_invalid_column.xls' )
MERGESPEC_FILE_NO_VARIABLE_PRIORITY = os.path.join( UNITTEST_SUBDIR, 'complementary_merge_mergespec_novarprio.xls' )
MERGESPEC_FILE_VALID_NONFUZZY = os.path.join( UNITTEST_SUBDIR, 'complementary_merge_TestData1_SpecSheet-nonfuzzy.xls' )
MERGESPEC_FILE_VALID_LARGE_NONFUZZY = os.path.join( UNITTEST_SUBDIR, 'ComplimentaryMerge-spec-ogt20130620.xls' )
MERGESPEC_FILE_VALID_NONFUZZY_NORECPRIORITY = os.path.join( UNITTEST_SUBDIR,
    'complementary_merge_TestData1_SpecSheet-nonfuzzy-norecpriority.xls' )
MERGESPEC_FILE_SHEET_VALID = 'File1'
MERGESPEC_OUTPUT_FILE = 'complementary_merge_output_mergespec.csv'
DS_INPUT_FILE_VALID = os.path.join( UNITTEST_SUBDIR, 'complementary_merge_TestData1.xls' )
COMPARE_FILE_NONFUZZY = os.path.join( UNITTEST_SUBDIR, 'cmpmrg_run_nfuz-fromSAS-20130702.csv' )
COMPARE_FILE_NONFUZZY_NOFORCE = os.path.join( UNITTEST_SUBDIR, 'cmpmrg_run_nfuz_nofrc-fromSAS-20130703.csv' )
COMPARE_FILE_NONFUZZY_NORECPRIORITY = os.path.join( UNITTEST_SUBDIR, 'cmpmrg_run_nfuz_norp-fromSAS-20130702.csv' )
COMPARE_FILE_MERGESPEC_OUTPUT = os.path.join( UNITTEST_SUBDIR, 'cmpmrg_output_mergespec-20130711.csv' )
DS_INPUT_FILE_SHEET_VALID = 'File1'

VALID_DS_INPUT_TABLE_NAME = 'cmpmrg_ds_input_valid'
VALID_DS_INPUT_TABLE_NAME_LARGE = 'ogt20130620'
VALID_DS_OUTPUT_TABLE_NAME = 'cmpmrg_test_table'
VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY = 'cmpmrg_dsoutput_nfuz'
VALID_DS_OUTPUT_TABLE_NAME_LARGE_NONFUZZY = 'cmpmrg_dsoutput_lg_nfuz'
VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NOFORCE = 'cmpmrg_dsoutput_nfuz_nofrc'
VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NORECPRIORITY = 'cmpmrg_dsoutput_nfuz_norp'

_DUMP_TABLE_SQL = '''
    SELECT {columns_str}
        FROM {table_schema}.[{table_name}]
        {where_clause}
        {order_by}
    '''


class TestComplementaryMerge( unittest.TestCase ):
    def setUp( self ):
        super( TestComplementaryMerge, self ).setUp( )
        self._run_context = SuiteContext( CONTEXT_NAME )
        self._log = self._run_context.get_logger( CONTEXT_NAME )

    # def tearDown( self ):
    #     dbcontext = self._run_context.getDBContext( )
    #     dbutilities.clear_all( db_context=dbcontext, table_schema=dbcontext.schema )

    #########
    #  Tests for null or empty parameters
    #########

    def test_null_run_context( self ):
        self._log.info( 'test_null_run_context() - testing...' )
        with self.assertRaises( StandardError ):
            ComplementaryMerge( run_context=None )
        self._log.info( 'test_null_run_context() - tested.' )

    def test_cm_null_input_table_names( self ):
        self._log.info( 'test_null_input_table_names() - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID )
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( StandardError ):
            cmpmrg.complementary_merge( mergespec_file=mergespec_file, input_table_names=None, output_table_names=None )
        self._log.info( 'test_null_input_table_names() - tested.' )

    def test_null_output_table_names( self ):
        self._log.info( 'test_null_output_table_names() - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID )
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        input_table_names = { FLAT_TABLE_KEY_NAME: 'no_table' }
        with self.assertRaises( StandardError ):
            cmpmrg.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_table_names,
                output_table_names=None )
        self._log.info( 'test_null_output_table_names() - tested.' )

    def test_cmf_null_input_table_names( self ):
        self._log.info( 'test_null_input_table_names() - testing...' )
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( StandardError ):
            cmpmrg.create_mergespec_file( input_table_name=None, new_mergespec_file=None )
        self._log.info( 'test_null_input_table_names() - tested.' )

    def test_cmf_null_mergespec_file( self ):
        self._log.info( 'test_null_mergespec_file() - testing...' )
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( StandardError ):
            cmpmrg.create_mergespec_file( input_table_name='no_table', new_mergespec_file=None )
        self._log.info( 'test_null_mergespec_file() - tested.' )

    #########
    # Tests for invalid parameters
    #########

    def test_mergespec_file_not_found( self ):
        self._log.info( 'test_mergespec_file_not_found() - testing...' )
        valid_ds_input_file = os.path.join( self._run_context.tests_safe_dir, DS_INPUT_FILE_VALID )
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        input_table_names = { FLAT_TABLE_KEY_NAME: 'no_table' }
        output_table_names = { FLAT_TABLE_KEY_NAME: 'no_table' }
        with self.assertRaises( IOError ):
            cmpmrg.complementary_merge( mergespec_file='no_file_found', input_table_names=input_table_names,
                output_table_names=output_table_names )
        self._log.info( 'test_mergespec_file_not_found() - tested.' )

    def test_mergespec_file_already_exists( self ):
        self._log.info( 'test_mergespec_file_already_exists() - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID )
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( IOError ):
            cmpmrg.create_mergespec_file( input_table_names={FLAT_TABLE_KEY_NAME: 'no_table'},
                new_mergespec_file=mergespec_file )
        self._log.info( 'test_mergespec_file_already_exists() - tested.' )

    def test_ds_input_not_found( self ):
        self._log.info( 'test_ds_input_not_found() - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID )
        input_table_names = { FLAT_TABLE_KEY_NAME: 'no_table' }
        output_table_names = { FLAT_TABLE_KEY_NAME: 'no_table' }
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( IOError ):
            cmpmrg.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_table_names,
                output_table_names=output_table_names )
        self._log.info( 'test_ds_input_not_found() - tested.' )

    def test_ds_output_already_exists( self ):
        self._log.info( 'test_ds_output_already_exists() - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID )
        valid_ds_input_file = os.path.join( self._run_context.tests_safe_dir, DS_INPUT_FILE_VALID )
        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ), table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader = SafeExcelReader( run_context=self._run_context, filename=valid_ds_input_file,
            sheet_name=DS_INPUT_FILE_SHEET_VALID, db_context=self._run_context.getDBContext( ),
            output_table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader.createTable( )
        input_table_names = { FLAT_TABLE_KEY_NAME: VALID_DS_INPUT_TABLE_NAME }
        output_table_names = { FLAT_TABLE_KEY_NAME: VALID_DS_INPUT_TABLE_NAME }
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( IOError ):
            cmpmrg.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_table_names,
                output_table_names=output_table_names )
        self._log.info( 'test_ds_output_already_exists() - tested.' )

    #########
    # Tests for invalid input formats
    #########

    def test_mergespec_file_invalid_format( self ):
        self._log.info( 'test_mergespec_file_invalid_format() - testing...' )
        valid_ds_input_file = os.path.join( self._run_context.tests_safe_dir, DS_INPUT_FILE_VALID )
        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ), table=VALID_DS_INPUT_TABLE_NAME )
        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ), table=VALID_DS_OUTPUT_TABLE_NAME )
        input_table_reader = SafeExcelReader( run_context=self._run_context, filename=valid_ds_input_file,
            sheet_name=DS_INPUT_FILE_SHEET_VALID, db_context=self._run_context.getDBContext( ),
            output_table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader.createTable( )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_MISSING_COLUMN )
        input_table_names = { FLAT_TABLE_KEY_NAME: VALID_DS_INPUT_TABLE_NAME }
        output_table_names = { FLAT_TABLE_KEY_NAME: VALID_DS_OUTPUT_TABLE_NAME }
        cmpmrg1 = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( EOFError ):
            cmpmrg1.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_table_names,
                output_table_names=output_table_names, force_one_only=True )

        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_INVALID_COLUMN )
        cmpmrg2 = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( ValueError ):
            cmpmrg2.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_table_names,
                output_table_names=output_table_names, force_one_only=True )
        self._log.info( 'test_mergespec_file_invalid_format() - tested.' )

        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_NO_VARIABLE_PRIORITY )
        cmpmrg2 = ComplementaryMerge( run_context=self._run_context )
        with self.assertRaises( ValueError ):
            cmpmrg2.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_table_names,
                output_table_names=output_table_names, force_one_only=True )
        self._log.info( 'test_mergespec_file_invalid_format() - tested.' )

    #########
    # Tests for mergespec output validations
    #########

    def test_valid_mergespec_file_output_format( self ):
        self._log.info( 'test_valid_mergespec_file_output_format() - testing...' )
        valid_ds_input_file = os.path.join( self._run_context.tests_safe_dir, DS_INPUT_FILE_VALID )
        input_tables = { FLAT_TABLE_KEY_NAME: VALID_DS_INPUT_TABLE_NAME }
        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ), table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader = SafeExcelReader( run_context=self._run_context, filename=valid_ds_input_file,
            sheet_name=DS_INPUT_FILE_SHEET_VALID, db_context=self._run_context.getDBContext( ),
            output_table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader.createTable( )
        mergespec_file = 'C:/{}'.format( MERGESPEC_OUTPUT_FILE )
        if os.path.exists( mergespec_file ):
            os.remove( mergespec_file )
        cmpmrg = ComplementaryMerge( run_context=self._run_context )
        cmpmrg.create_mergespec_file( input_table_names=input_tables, new_mergespec_file=mergespec_file )
        self._log.info( 'test_valid_mergespec_file_output_format() - tested.' )
        filename1 = mergespec_file
        filename2 = os.path.join( self._run_context.tests_safe_dir, COMPARE_FILE_MERGESPEC_OUTPUT )
        assert filecmp.cmp( filename1, filename2 ), 'Python test output does not match expected output.'

    #########
    #  Tests for complete processing of real data
    #########

    def test_merge_nonfuzzy( self ):
        self._log.info( 'test_merge_nonfuzzy - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID_NONFUZZY )
        valid_ds_input_file = os.path.join( self._run_context.tests_safe_dir, DS_INPUT_FILE_VALID )

        input_tables = { FLAT_TABLE_KEY_NAME: VALID_DS_INPUT_TABLE_NAME }
        output_tables = { FLAT_TABLE_KEY_NAME: VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY }

        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ), table=VALID_DS_INPUT_TABLE_NAME )
        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ),
            table=VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY )
        for subject in output_tables:
            table_name = VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY + '_' + subject
            self._log.info( 'dropping output table [{}]'.format( table_name ) )
            dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ),
                table=table_name )

        input_table_reader = SafeExcelReader( run_context=self._run_context, filename=valid_ds_input_file,
            sheet_name=DS_INPUT_FILE_SHEET_VALID, db_context=self._run_context.getDBContext( ),
            output_table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader.createTable( )

        cmpmrg = ComplementaryMerge( run_context=self._run_context, flat_table_identity_field_name='[import_order]' )
        cmpmrg.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_tables,
            output_table_names=output_tables, force_one_only=True )

        columns_str = ( 'Barcode, ID, LastName, FirstName, Score1, Race, Old, Score2, Score3, Score4, variable_priority,'
                        ' record_priority, Attempt1, Attempt2, Attempt3, Attempt4, iep' )
        filename1 = self._dump_table( VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY, columns_str=columns_str,
            sort_column_nbrs=[ 1, 6, 0 ] )
        filename2 = os.path.join( self._run_context.tests_safe_dir, COMPARE_FILE_NONFUZZY )
        assert filecmp.cmp( filename1, filename2 ), 'Python test output does not match SAS test output.'

        self._log.info( 'test_merge_nonfuzzy - tested.' )

    def test_merge_nonfuzzy_noforce( self ):
        self._log.info( 'test_merge_nonfuzzy_noforce - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID_NONFUZZY )
        valid_ds_input_file = os.path.join( self._run_context.tests_safe_dir, DS_INPUT_FILE_VALID )

        input_tables = { FLAT_TABLE_KEY_NAME: VALID_DS_INPUT_TABLE_NAME }
        output_tables = { FLAT_TABLE_KEY_NAME: VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NOFORCE }

        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ), table=VALID_DS_INPUT_TABLE_NAME )
        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ),
            table=VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NOFORCE )

        input_table_reader = SafeExcelReader( run_context=self._run_context, filename=valid_ds_input_file,
            sheet_name=DS_INPUT_FILE_SHEET_VALID, db_context=self._run_context.getDBContext( ),
            output_table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader.createTable( )

        cmpmrg = ComplementaryMerge( run_context=self._run_context, flat_table_identity_field_name='[import_order]' )
        cmpmrg.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_tables,
            output_table_names=output_tables, force_one_only=False )

        columns_str = ( 'Barcode, ID, LastName, FirstName, Score1, Race, Old, Score2, Score3, Score4, variable_priority,'
                        ' record_priority, Attempt1, Attempt2, Attempt3, Attempt4, iep' )
        filename1 = self._dump_table( VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NOFORCE, columns_str=columns_str,
            sort_column_nbrs=[ 1, 6, 0 ] )
        filename2 = os.path.join( self._run_context.tests_safe_dir, COMPARE_FILE_NONFUZZY_NOFORCE )
        assert filecmp.cmp( filename1, filename2 ), 'Python test output does not match SAS test output.'

        self._log.info( 'test_merge_nonfuzzy_noforce - tested.' )

    def test_merge_nonfuzzy_norecpriority( self ):
        self._log.info( 'test_merge_nonfuzzy_norecpriority - testing...' )
        mergespec_file = os.path.join( self._run_context.tests_safe_dir, MERGESPEC_FILE_VALID_NONFUZZY_NORECPRIORITY )
        valid_ds_input_file = os.path.join( self._run_context.tests_safe_dir, DS_INPUT_FILE_VALID )

        input_tables = { FLAT_TABLE_KEY_NAME: VALID_DS_INPUT_TABLE_NAME }
        output_tables = { FLAT_TABLE_KEY_NAME: VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NORECPRIORITY }

        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ),
            table=VALID_DS_INPUT_TABLE_NAME )
        dbutilities.drop_table_if_exists( db_context=self._run_context.getDBContext( ),
            table=VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NORECPRIORITY )

        input_table_reader = SafeExcelReader( run_context=self._run_context, filename=valid_ds_input_file,
            sheet_name=DS_INPUT_FILE_SHEET_VALID,
            db_context=self._run_context.getDBContext( ),
            output_table=VALID_DS_INPUT_TABLE_NAME )
        input_table_reader.createTable( )

        cmpmrg = ComplementaryMerge( run_context=self._run_context, flat_table_identity_field_name='[import_order]' )
        cmpmrg.complementary_merge( mergespec_file=mergespec_file, input_table_names=input_tables,
            output_table_names=output_tables, force_one_only=False )

        columns_str = ( 'Barcode, ID, LastName, FirstName, Score1, Race, Old, Score2, Score3, Score4, variable_priority,'
                        ' record_priority, Attempt1, Attempt2, Attempt3, Attempt4, iep' )
        filename1 = self._dump_table( VALID_DS_OUTPUT_TABLE_NAME_NONFUZZY_NORECPRIORITY, columns_str=columns_str,
            sort_column_nbrs=[ 1, 6, 0 ] )
        filename2 = os.path.join( self._run_context.tests_safe_dir, COMPARE_FILE_NONFUZZY_NORECPRIORITY )
        assert filecmp.cmp( filename1, filename2 ), 'Python test output does not match SAS test output.'

        self._log.info( 'test_merge_nonfuzzy_norecpriority - tested.' )

    #########
    #  Utility functions
    #########

    def _dump_table( self, table_name, columns_str='*', where_clause='', order_by='', sort_column_nbrs=None ):
        return_filespec = ''
        assert table_name, "table_name is null or empty."
        assert columns_str, "columns_str is null or empty."
        assert where_clause is not None, "where_clause is null or empty."
        assert order_by is not None, "order_by is null or empty."
        return_filespec = 'C:/{}-{}.csv'.format( table_name, os.getpid( ) )
        dbcontext = self._run_context.getDBContext( )
        sql = _DUMP_TABLE_SQL.format( table_schema=dbcontext.schema, table_name=table_name, columns_str=columns_str,
            where_clause=where_clause, order_by=order_by )
        output_rows = { }
        for result in dbcontext.execute( sql ):
            row = [ ]
            for column in result:
                if isinstance( column, basestring ):
                    value = column.rstrip( )
                else:
                    value = column
                row.append( value )
            key = [ ]
            for column_nbr in sort_column_nbrs:
                key.append( result[ column_nbr ] )
            output_rows[ tuple( key ) ] = row
        with open( return_filespec, 'wb' ) as csvfile:
            writer = csv.writer( csvfile )
            writer.writerow( columns_str.split( ", " ) )
            for key in sorted( output_rows.iterkeys( ) ):
                writer.writerow( output_rows[ key ] )
        return return_filespec


if __name__ == '__main__':
    unittest.main( )
