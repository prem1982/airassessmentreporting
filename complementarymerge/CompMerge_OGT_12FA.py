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

"""
Created on August 23, 2013

@author: temp_tprindle
"""

#
# Converted from original source file = CompMerge.sas
#

import os.path
from airassessmentreporting import complementarymerge

from airassessmentreporting.airutility import dbutilities
from airassessmentreporting.complementarymerge import ComplementaryMerge, FLAT_TABLE_KEY_NAME
from airassessmentreporting.erasure import BookMapReader


def complementary_merge( run_context, bookmap_location_file_name, bookmap_sheet, mergespec_file_name, input_table_names,
        output_table_names ):
    db_context = run_context.getDBContext( )
    temp_bookmap_table_name = dbutilities.get_temp_table( db_context ).table_name
    BookMapReader( inputfile=bookmap_location_file_name, inputsheet=bookmap_sheet, read_to_db=True, db_context=db_context,
        outputTable=temp_bookmap_table_name )
    temp_prep_table_name = dbutilities.get_temp_table( db_context ).table_name
    _comp_prepare1( run_context=run_context, bookmaps_table_name=temp_bookmap_table_name,
        input_table_names=input_table_names, output_table_name=temp_prep_table_name )

    prepped_input_table_names = { }
    for key in input_table_names:
        if key == FLAT_TABLE_KEY_NAME:
            prepped_input_table_names[ key ] = temp_prep_table_name
        else:
            prepped_input_table_names[ key ] = input_table_names[ key ]

    cmpmrg = ComplementaryMerge( run_context=run_context )
    cmpmrg.identity_insert = False
    cmpmrg.complementary_merge( mergespec_file=mergespec_file_name, input_table_names=prepped_input_table_names,
        output_table_names=output_table_names )


def create_mergespec_file( run_context, input_table_names, new_mergespec_file ):
    cmpmrg = ComplementaryMerge( run_context=run_context )
    cmpmrg.create_mergespec_file( input_table_names=input_table_names, new_mergespec_file=new_mergespec_file )

#######################################################################
## OGT Related Code. Replace as necessary...
#######################################################################

_CREATE_TEMP_TABLE_SQL = '''
    SELECT *
        INTO {temp_table_schema}.{temp_table_name}
        FROM {input_table_schema}.[{input_table_name}] AS itn

    ALTER TABLE {temp_table_schema}.{temp_table_name}
        ADD PRIMARY KEY ( {key_field_name} )
    '''

_SELECT_ALL_SQL = '''
    SELECT *
        FROM {table_schema}.{table_name}
    '''

_ATTEMPT_COLUMN_NAME_PREFIX = "attempt_"
_RAW_COLUMN_NAME_SUFFIX = "_raw_item"
_DUMMY_GRADE = 10


def _comp_prepare1( run_context, bookmaps_table_name, input_table_names, output_table_name ):
    log = run_context.get_logger( 'ComplementaryMerge' )
    db_context = run_context.getDBContext( )

    sql = _CREATE_TEMP_TABLE_SQL.format( input_table_schema=db_context.schema,
        input_table_name=input_table_names[ FLAT_TABLE_KEY_NAME ], temp_table_schema=db_context.schema,
        temp_table_name=output_table_name, key_field_name=complementarymerge.DEFAULT_FLAT_TABLE_IDENTITY_FIELD_NAME )
    log.debug( '_comp_prepare1 - sql[{}]'.format( sql ) )
    db_context.executeNoResults( query=sql, commit=True )

    add_fields_to_temp_table( run_context=run_context, subjects=input_table_names.keys( ), temp_table_name=output_table_name )

    update_temp_table( run_context=run_context, bookmaps_table_name=bookmaps_table_name, input_table_names=input_table_names,
        temp_table_name=output_table_name )


def add_fields_to_temp_table( run_context, subjects, temp_table_name ):
    log = run_context.get_logger( 'ComplementaryMerge' )
    db_context = run_context.getDBContext( )

    alter_sql = '''
        ALTER TABLE {temp_table_schema}.{temp_table_name}
            ADD [dob]           DATETIME        NULL,
                [gender]        NCHAR(1)        NULL,
                [variable_priority]     INTEGER     NOT NULL    DEFAULT 0,
        '''.format( temp_table_schema=db_context.schema, temp_table_name=temp_table_name ).rstrip( ' ' )

    for subject in subjects:
        if subject == FLAT_TABLE_KEY_NAME:
            continue
        field_name = _ATTEMPT_COLUMN_NAME_PREFIX + subject
        alter_sql += ' ' * 16 + '[' + field_name + ']     INTEGER         NOT NULL        DEFAULT 0,\n'
    alter_sql = alter_sql.rstrip( ',\n' ) + '\n'

    log.debug( 'add_fields_to_temp_table - alter_sql[{}]'.format( alter_sql ) )
    db_context.executeNoResults( query=alter_sql, commit=True )


def update_temp_table( run_context, bookmaps_table_name, input_table_names, temp_table_name ):
    log = run_context.get_logger( 'ComplementaryMerge' )
    db_context = run_context.getDBContext( )

    # Set dob, gender, and variable_priority fields. Updates all rows so no final where clause.
    # CompMerge.sas line 30
    update_sql = '''
        UPDATE {temp_table_schema}.{temp_table_name}
            SET ucrxfnm = {temp_table_schema}.ToProperCase(ucrxfnm, 10)
                , ucrxlnm = {temp_table_schema}.ToProperCase(ucrxlnm, 10)
                , [dob] = ( CASE WHEN ([dob_year] IS NOT NULL) AND ([dob_month] IS NOT NULL) AND ([dob_day] IS NOT NULL)
                            THEN ( CASE WHEN ([dob_year] BETWEEN 1801 AND 2100) AND ([dob_month] BETWEEN 1 AND 12)
                                        AND ([dob_day] BETWEEN 1 AND 31)
                                   THEN ( CASE WHEN (isnumeric([dob_year]) = 1) AND (isnumeric([dob_month]) = 1)
                                            AND (isnumeric([dob_day]) = 1)
                                          THEN ( CASE WHEN isdate(convert(varchar,convert(integer,[dob_year])) + '-'
                                                    + convert(varchar,convert(integer,[dob_month]))
                                                    + '-' + convert(varchar,convert(integer,[dob_day]))) = 1
                                                 THEN convert(varchar,convert(integer,[dob_year])) + '-'
                                                    + convert(varchar,convert(integer,[dob_month]))
                                                    + '-' + convert(varchar,convert(integer,[dob_day]))
                                                 ELSE NULL END )
                                          ELSE NULL END )
                                   ELSE NULL END )
                            ELSE NULL END )
                , [gender] = ( CASE WHEN ( ([ucrxgen] IS NOT NULL) AND ([ucrxgen] <> '*') )
                                THEN ( CASE WHEN isnumeric([ucrxgen]) = 1 THEN
                                        ( CASE [ucrxgen]
                                            WHEN 1.0 THEN '1'
                                            WHEN 2.0 THEN '2'
                                            ELSE '' END )
                                        ELSE '' END )
                                ELSE '' END )
                , [variable_priority] = ( CASE WHEN ( [preidflag] = 1 ) THEN 10 ELSE 0 END )
        '''.format( temp_table_schema=db_context.schema, temp_table_name=temp_table_name )
    log.debug( 'update_temp_table - update_sql[{}]'.format( update_sql ) )
    update_sql = update_sql.strip(' \n')
    db_context.executeNoResults( query=update_sql, commit=True )

    # Sets attempts fields. Updates all rows so no final where clause.
    # CompMerge.sas line 42
    for subject in input_table_names.keys( ):
        if subject == FLAT_TABLE_KEY_NAME:
            continue
        field_name = _ATTEMPT_COLUMN_NAME_PREFIX + subject
        update_sql = '''
            UPDATE {temp_table_schema}.{temp_table_name}
                SET [{field_name}] = CASE
                    WHEN ( SELECT DISTINCT 1
                        FROM {input_table_schema}.[{subject_input_table_name}] AS {subject}
                        JOIN {input_table_schema}.{bookmaps_table_name} AS [bt]
                            ON [bt].[item position] is not null AND [bt].[item position] = [{subject}].[id]
                        WHERE [{subject}].[flat_table_id] = [flat].[id]
                        AND [{subject}].[up{subject}x{raw_field_name_suffix}] IS NOT NULL
                        AND [{subject}].[up{subject}x{raw_field_name_suffix}] <> ''
                        AND [{subject}].[up{subject}x{raw_field_name_suffix}] <> '-'
                        AND [bt].[form_values] IN ( 'A', 'B' )
                        AND [bt].[subject_values] = '{subject}'
                        AND [bt].[grade_values] = {grade}
                        AND [bt].[role] = 'Operational'
                        AND [bt].[item format] = 'MC'
                    ) IS NOT NULL THEN 1
                    WHEN ( SELECT DISTINCT 1
                        FROM {input_table_schema}.[{subject_input_table_name}] AS {subject}
                        JOIN {input_table_schema}.{bookmaps_table_name} AS [bt]
                            ON [bt].[item position] is not null AND [bt].[item position] = [{subject}].[id]
                        WHERE [{subject}].[flat_table_id] = [flat].[id]
                        AND [bt].[form_values] IN ( 'A', 'B' )
                        AND [bt].[subject_values] = '{subject}'
                        AND [bt].[grade_values] = {grade}
                        AND [bt].[role] = 'Operational'
                        AND [bt].[item format] <> 'MC'
                        AND [{subject}].[up{subject}x_oe_final] NOT IN ('A', 'B', '' )
                    ) IS NOT NULL THEN 1
                    ELSE 0 END
            FROM {temp_table_schema}.{temp_table_name} AS [flat]
            '''.format( temp_table_schema=db_context.schema, temp_table_name=temp_table_name, field_name=field_name,
            input_table_schema=db_context.schema,
            subject_input_table_name=input_table_names[ subject ], subject=subject, bookmaps_table_name=bookmaps_table_name,
            grade=_DUMMY_GRADE, raw_field_name_suffix=_RAW_COLUMN_NAME_SUFFIX )
        update_sql = update_sql.strip(' \n')
        log.debug( 'update_temp_table - update_sql[{}]'.format( update_sql ) )
        db_context.executeNoResults( query=update_sql, commit=True )

        # Unsets attempts fields if invalid field exists for subject. Only update tables with invalid field. Only update rows
        # where invalid field value is 1.
        # CompMerge.sas line 67
        for subject in input_table_names.keys( ):
            if subject == FLAT_TABLE_KEY_NAME:
                continue
            invalid_field_name = 'uf' + subject + 'x_invalid'
            tablespec = dbutilities.get_table_spec( db_context=db_context, table_schema=db_context.schema,
                table=temp_table_name )
            tablespec.populate_from_connection( )
            if invalid_field_name in tablespec:
                attempt_field_name = _ATTEMPT_COLUMN_NAME_PREFIX + subject
                update_sql = '''
                    UPDATE {temp_table_schema}.{temp_table_name}
                        SET [{attempt_field_name}] = 0
                        WHERE ( [{invalid_field_name}] IS NOT NULL ) AND ( [{invalid_field_name}] = 1 )
                    '''.format( temp_table_schema=db_context.schema, temp_table_name=temp_table_name,
                    attempt_field_name=attempt_field_name, invalid_field_name=invalid_field_name )
            log.debug( 'update_temp_table - update_sql[{}]'.format( update_sql ) )
            db_context.executeNoResults( query=update_sql, commit=True )

        # Updates the variable_priority field, again. Updates all rows so no final where clause.
        # CompMerge.sas line 68
        update_sql = '''
            UPDATE {temp_table_schema}.{temp_table_name}
                SET [variable_priority] = (
                    CASE WHEN [{field_name}] = 1 THEN ( [variable_priority] + 1 ) ELSE [variable_priority] END )
            '''.format( temp_table_schema=db_context.schema, temp_table_name=temp_table_name, field_name=field_name )
        log.debug( 'update_temp_table - update_sql[{}]'.format( update_sql ) )
        db_context.executeNoResults( query=update_sql, commit=True )


#######################################################################
## TEST Code. Replace as necessary...
#######################################################################

from airassessmentreporting.testutility import SuiteContext

CVSROOT = 'C:/Users/temp_tprimble/OtherStuff/cvs projects'
INPUT_DS_FILE_SHEET = 'File1'
#BOOKMAP_LOCATION_FILE_NAME = 'Bookmaplocations1.xls'
BOOKMAP_LOCATION_FILE_NAME = 'BookMapLocations1.xls'
BOOKMAP_SHEET = 'BookMap'
UNITTEST_SUBDIR = 'complementary_merge'
MERGESPEC_FILE_NAME = os.path.join( UNITTEST_SUBDIR, 'ComplementaryMerge-spec-ogt20130620-flat.xls' )


def main( ):
    """
    This runs a test of the complementary_merge wrapper/glue code.
    """
    run_context = SuiteContext( 'OGT_12FA' )
    log = run_context.get_logger( 'ComplementaryMerge' )
    db_context = run_context.getDBContext( )

    pathname = os.path.join(CVSROOT, 'CSSC Score Reporting', 'OGT Fall 2012', 'Code/Development/Intake')
    bookmap_location_file_name = os.path.join( pathname, BOOKMAP_LOCATION_FILE_NAME )
    log.debug("main - bookmap_location_file_name[%s]" % bookmap_location_file_name)
    print("bookmap_location_file_name[%s]" % bookmap_location_file_name)
    mergespec_file_name = os.path.join( run_context.tests_safe_dir, MERGESPEC_FILE_NAME )

    input_table_names = { FLAT_TABLE_KEY_NAME: 'rc2FINAL', 'C': 'mc_table_C', 'M': 'mc_table_M', 'R': 'mc_table_R',
        'S': 'mc_table_S', 'W': 'mc_table_W' }
    output_table_names = { FLAT_TABLE_KEY_NAME: 'rc2FINAL_cmrg', 'C': 'mc_table_C_cmrg', 'M': 'mc_table_M_cmrg',
        'R': 'mc_table_R_cmrg', 'S': 'mc_table_S_cmrg', 'W': 'mc_table_W_cmrg' }

    for key in output_table_names:
        dbutilities.drop_table_if_exists( db_context=db_context, table=output_table_names[ key ] )

    try:
        complementary_merge( run_context=run_context, bookmap_location_file_name=bookmap_location_file_name,
            bookmap_sheet=BOOKMAP_SHEET, mergespec_file_name=mergespec_file_name, input_table_names=input_table_names,
            output_table_names=output_table_names )
        #create_mergespec_file( run_context=run_context, input_table_names=input_table_names,
        #    new_mergespec_file='C:/new_mergespec_file.csv' )
    except Exception, error_msg:
        log.exception( '\n\n' )
        raise


if __name__ == '__main__':
    main( )
