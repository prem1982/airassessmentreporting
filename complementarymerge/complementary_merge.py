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
# TODO - FUTURE - implement fuzzy
# TODO - FUTURE - improve comment output to complementary_merge_report field for picked/forced items.
#

#
# - Unit tests work.
# - Spring 2012 seems to be correct.
# - Fall 2012 is off by 17 records. 
#    - Problem appears to be how a conflict is resolved between two matching records that both have an 
#        attempt indicator set for the same subject, i.e. "overlapping". 
#        (also see notes in ComplementaryMerge-NOTES.sas)
#    - A warning occurs for one of the records, but the above problem is unrelated. The warning may not 
#        be an issue once the problem above is resolved. 
#

"""
Created on May 2013

@author: temp_tprindle
"""

import csv
import os
import re
from airassessmentreporting.airutility import dbutilities, formatutilities
from airassessmentreporting.airutility import SafeExcelReader

__all__ = [ 'ComplementaryMerge', 'FLAT_TABLE_KEY_NAME', 'DEFAULT_FLAT_TABLE_IDENTITY_FIELD_NAME' ]

# FLAT_TABLE_KEY_NAME is the key name for the "FLAT" table (as opposed to the "SUBJECT" or "ITEM" tables).
FLAT_TABLE_KEY_NAME = 'FLAT'

# DEFAULT_FLAT_TABLE_IDENTITY_FIELD_NAME is the default name of the key field in the flat table created by the intake process.
DEFAULT_FLAT_TABLE_IDENTITY_FIELD_NAME = '[id]'


class ComplementaryMerge( object ):
    """
Class implementing ComplementaryMerge macro

The functionality is designed to be as close as possible to the SAS macro ComplementaryMerge, but no closer.

According to the specification:

    - Records match if they match on their primary keys and ( on their secondary keys or either secondary key is empty ).

    - Records are complementary if they match and do not conflict for any test unit.

Parameters:

    *run_context* : :class:`RunContext`
        The run context to use for logging.

    *flat_table_identity_field_name* : string
        The field name of the identity field in the flat table, e.g. "[id]". Defaults to constant
        DEFAULT_FLAT_TABLE_IDENTITY_FIELD_NAME.

Assumptions:

    - All of the fields used in the selection and comparison criteria are contained in the "flat" table,
    and that only subject (item) information is contained in the "subject" tables. The fields that must exist only in the
    "flat" table especially include the primary and secondary keys,
    the sort field, the fuzzy keys, and the variable and record priority fields.

    - Merging occurs across all matched, complementary records across all tables.

Know Issue(s):

    Currently, this does not handle fuzzy keys.

.. note::

        Complementary Merge: "That merge you have there. That's a really nice merge."
    """

    _SUBJECT_KEY_FIELDS = [ '[flat_table_id]', '[id]' ]
    _SUBJECT_FLAT_TABLE_ID_FIELD = '[flat_table_id]'

    # _RECORD_PRIORITY_THRESHOLD is defined as 1000 in the specifications.
    _RECORD_PRIORITY_THRESHOLD = 1000

    # _CREATE_TEMP_MATCHING_TABLE_SQL creates a temporary table containing all of the rows in the input table
    #       which have matching rows in the
    _CREATE_TEMP_MATCHING_TABLE_SQL = '''
        SELECT [itn1].{key_field_name}
            INTO {temp_table_schema}.{temp_table_name}
            FROM {input_table_schema}.{input_table_name} AS itn1
            WHERE EXISTS (
                SELECT 1
                    FROM {input_table_schema}.{input_table_name} AS itn2
                    WHERE {matching_where_clause}
                )
        '''

    #
    # Create Output Flat Table:
    #    - Create output flat table and copy non-matching records.
    #    - Add comment column (complementary_merge_report) for recording how complementary rows were merged.
    #    - Set identity column as primary key.
    #    - Add unique index on the sort column (barcode in the tests) to guarantee uniqueness.
    #
    _CREATE_OUTPUT_FLAT_TABLE_WO_MATCHING_SQL = '''
        SELECT *
            INTO {output_table_schema}.[{output_table_name}]
            FROM {input_table_schema}.{input_table_name} AS itn
            WHERE NOT EXISTS (
                SELECT 1
                    FROM {temp_table_schema}.{temp_table_name} AS tt
                    WHERE tt.{key_field_name} = itn.{key_field_name}
                )

        ALTER TABLE {output_table_schema}.[{output_table_name}]
            ADD [{merge_report_field_name}] nvarchar(255) null

        --ALTER TABLE {output_table_schema}.[{output_table_name}]
        --    ADD PRIMARY KEY ( {key_field_name} )

        CREATE UNIQUE INDEX [{index_name}]
            ON {output_table_schema}.[{output_table_name}]
            ( {sort_column_name} )
        '''

    #
    # Create Output Subject Table:
    #    - Create output subject table and copy non-matching records.
    #    - Set flat table identity and subject table identity columns as composite primary key.
    #
    _CREATE_OUTPUT_SUBJECT_TABLE_WO_MATCHING_SQL = '''
        SELECT *
            INTO {output_table_schema}.[{output_table_name}]
            FROM {input_table_schema}.{input_table_name} AS itn
            WHERE NOT EXISTS (
                SELECT 1
                    FROM {temp_table_schema}.{temp_table_name} AS tt
                    WHERE tt.{key_field_name} = itn.{subject_key_field_name}
                )

        --ALTER TABLE {output_table_schema}.[{output_table_name}]
        --    ADD PRIMARY KEY ( {key_field_name} )
        '''

    _SELECT_ONLY_MATCHING = '''
        SELECT *
            FROM {input_table_schema}.{input_table_name} AS itn
            WHERE EXISTS (
                SELECT 1
                    FROM {temp_table_schema}.{temp_table_name} AS tt
                    WHERE tt.{key_field_name} = itn.{key_field_name}
                )
            ORDER BY {sort_column_name}
        '''

    # Select the corresponding records.
    _SELECT_CORRESP = '''
        SELECT *
            FROM {input_table_schema}.{input_table_name} AS itn
            WHERE EXISTS (
                SELECT 1
                    FROM {temp_table_schema}.{temp_table_name} AS tt
                    WHERE tt.{key_field_name} = itn.{key_field_name}
                )
            AND {corresp_where_clause}
            ORDER BY {sort_column_name}
        '''

    _INSERT_SQL_WO_IDENTITY_INSERT = '''
        --SET IDENTITY_INSERT {table_schema}.[{table_name}] OFF

        INSERT INTO {table_schema}.[{table_name}]
            ( {column_names} )
            values ( {column_values} )

        --SET IDENTITY_INSERT {table_schema}.[{table_name}] ON
        '''

    # SQL for insert statement into output table including allowing the identity field (import_order) to accept the input
    # table's import_order values. (IDENTITY_INSERT seems to be for only one table at a time, so turn it off when done.)
    _INSERT_SQL_W_IDENTITY_INSERT = '''
        SET IDENTITY_INSERT {table_schema}.[{table_name}] ON

        INSERT INTO {table_schema}.[{table_name}]
            ( {column_names} )
            values ( {column_values} )

        SET IDENTITY_INSERT {table_schema}.[{table_name}] OFF
        '''

    _SELECT_OUTPUT_COUNT = '''
        SELECT count({key_field_name})
            FROM {output_table_schema}.[{output_table_name}]
        '''

    _SELECT_SUBJECT = '''
        SELECT *
            FROM {input_table_schema}.[{input_table_name}]
            WHERE {id_field} = ?
        '''

    _PRIMARY_KEY_RE = re.compile( '^primary$' )
    _SECONDARY_KEY_RE = re.compile( '^secondary$' )
    _SORT_KEY_RE = re.compile( '^sort$' )

    _RESRULE_RECORD_PRIORITY = 'record priority'
    _RESRULE_VARIABLE_PRIORITY = 'variable priority'

    _RESRULE_COMMON_RE = re.compile( '^common$' )
    _RESRULE_COMMON_NONMISSING_RE = re.compile( '^common, *non-missing$' )
    _RESRULE_OR_RE = re.compile( '^or$' )
    _RESRULE_RECORD_PRIORITY_RE = re.compile( '^record[ _]priority$' )
    _RESRULE_SORT_RE = re.compile( '^sort$' )
    _RESRULE_TEST_UNIT_RE = re.compile( '^test unit ([0-9]+)$' )
    _RESRULE_UNIT_INDICATOR_RE = re.compile( '^unit ([0-9]+) indicator$' )
    _RESRULE_VARIABLE_PRIORITY_RE = re.compile( '^variable[ _]priority$' )

    _MERGE_REPORT_FIELD_NAME = 'complementary_merge_report'

    def __init__( self, run_context, flat_table_identity_field_name=DEFAULT_FLAT_TABLE_IDENTITY_FIELD_NAME ):
        if not run_context:
            raise ValueError( 'run_context is null or empty.' )
        self._run_context = run_context
        self._flat_table_identity_field_name = flat_table_identity_field_name
        self._input_db_context = run_context.getDBContext( instance_name='default', cached=True )
        self._output_db_context = run_context.getDBContext( instance_name='output', cached=True )
        self._log = run_context.get_logger( 'ComplementaryMerge' )
        self._counts = Counts( )
        self._identity_insert = True
        self._processed_idents = [ ]  # List of flat records that have already been processed.
        self._processed_subject_items = [ ]  # List of subject records that have already been processed.
        self._debug_sql_firstpass = True  # Only show the constructed SQL queries the first time for debugging. (About 192.)
        self._temp_matching_table_name = dbutilities.get_temp_table( self._input_db_context ).table_name
        self._log.debug( '__init__ - _temp_matching_table_name[{}]'.format( self._temp_matching_table_name ) )

        # mergespec_file: string - Mergespec file path specification.
        self._mergespec_file = ''

        # input_table_names: dictionary of table designators and table names (flat and subjects).
        # Contains all of the input database table names (input dataset).
        self._input_table_names = { }

        # output_table_names: dictionary of table designators and table names (flat and subjects).
        # Contains all of the output database table names (output dataset).
        self._output_table_names = { }

        self._variables_by_name = { }
        self._variables_by_resolution_rule = { }
        self._primary_key_variable_names = { }
        self._secondary_key_variable_names = { }
        self._sort_variable_name = None
        self._input_tablespecs = { }

    @property
    def flat_table_identity_field_name( self ):
        """
        *flat_table_identity_field_name* : string
            Contains the field name of the unique primary key field in the flat table.
        """
        return self._flat_table_identity_field_name

    @property
    def identity_insert( self ):
        """
        *identity_insert* : boolean (true/false)
            When *identity_insert* = true, :meth:`complementary_merge` will insert identity values into the output table.

            When *identity_insert* = false, :meth:`complementary_merge` will not insert identity values into the output table.
            (:meth:`complementary_merge` will fail if identity values would be inserted.)
        """
        return self._identity_insert

    @identity_insert.setter
    def identity_insert( self, identity_insert=False ):
        """
        Setter for identity_insert attribute.
        """
        self._identity_insert = identity_insert

    def complementary_merge( self, mergespec_file, input_table_names, output_table_names, force_one_only=False ):
        """
        This method performs the complementary merge on the specified dataset (input tables) using the specified merge
        specification file and creates the specified output dataset (output tables).

        Parameters:

            *mergespec_file* : string
                Mergespec file path specification.

            *input_table_names* : dictionary of table designators and table names (flat and subjects)
                Contains all of the input database table names (input dataset).

            *output_table_names* : dictionary of table designators and table names (flat and subjects)
                Contains all of the output database table names (output dataset).

            *force_one_only* : boolean (true/false)
                Generally: For a given record, if true and there are no complementary records, then select the record with the
                highest record_priority value and then lowest sort value. If false, keep both records.

                In this method: Pass the force_one_only value on to :meth:`_process_matching()`.

        Returns: nothing
        """
        if not mergespec_file:
            raise ValueError( 'mergespec_file is null or empty.' )
        if not input_table_names:
            msg = 'input_table_names is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not output_table_names:
            msg = 'output_table_names is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        if not os.path.exists( mergespec_file ):
            self._counts.inc_errors( )
            msg = 'mergespec_file[{}] not found.'.format( mergespec_file )
            self._log.error( msg )
            raise IOError( msg )

        for key in input_table_names:
            table_name = input_table_names[ key ]
            if not dbutilities.table_exists( table=table_name, db_context=self._input_db_context ):
                self._counts.inc_errors( )
                msg = 'input_table[{}] for key[{}] not found.'.format( table_name, key )
                self._log.error( msg )
                raise IOError( msg )
            if key == FLAT_TABLE_KEY_NAME:
                if not dbutilities.n_obs( table=input_table_names[ key ], db_context=self._input_db_context ):
                    self._counts.inc_errors( )
                    msg = 'input_flat_table[{}] has no rows.'.format( input_table_names[ key ] )
                    self._log.error( msg )
                    raise EOFError( msg )

        for key in output_table_names:
            table_name = output_table_names[ key ]
            self._log.debug( 'complementary_merge - key[{}] table_name[{}]'.format( key, table_name ) )
            if dbutilities.table_exists( table=table_name, db_context=self._output_db_context ):
                self._counts.inc_errors( )
                msg = 'output_table[{}] for subject[{}] already exists.'.format( table_name, key )
                self._log.error( msg )
                raise IOError( msg )

        self._mergespec_file = mergespec_file
        self._input_table_names = input_table_names
        self._output_table_names = output_table_names

        self._log.debug(
            'complementary_merge - flat_table_identity_field_name[{}]'.format( self._flat_table_identity_field_name ) )

        try:
            self._initialize_variable_dicts( )
            self._clone_non_matching( )
            self._process_matching( force_one_only=force_one_only )
        finally:
            self._log.info( 'complementary_merge - {}'.format( self._counts.format_str( ) ) )

    def create_mergespec_file( self, input_table_names, new_mergespec_file ):
        """
        Create a new mergespec template file using the input table columns.

        Parameters:

            *input_table_name* : string
                Input database table name (input dataset).

            *new_mergespec_file* : string
                Mergespec file path specification for new mergespec file.

        Returns: nothing
        """
        if not input_table_names:
            msg = 'input_table_names is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not new_mergespec_file:
            raise ValueError( 'new_mergespec_file is null or empty.' )

        for table_key in input_table_names:
            table_name = input_table_names[ table_key ]
            if not dbutilities.table_exists( db_context=self._input_db_context, table_schema=self._input_db_context.schema,
                    table=table_name ):
                self._counts.inc_errors( )
                msg = 'input_table_name[{}] not found.'.format( table_name )
                self._log.error( msg )
                raise IOError( msg )

        if os.path.exists( new_mergespec_file ):
            self._counts.inc_errors( )
            msg = 'mergespec_file[{}] already exists.'.format( new_mergespec_file )
            self._log.error( msg )
            raise IOError( msg )

        tablespecs = { }
        for table_key in input_table_names:
            table_name = input_table_names[ table_key ]
            tablespec = dbutilities.get_table_spec( db_context=self._input_db_context,
                table_schema=self._input_db_context.schema,
                table=table_name )
            tablespec.populate_from_connection( )
            tablespecs[ table_name ] = tablespec

        with open( new_mergespec_file, 'wb' ) as mergespec_file:
            writer = csv.writer( mergespec_file )
            writer.writerow( [ 'Variable_Name', 'Resolution_Rule', 'Keys' ] )
            for table_name in tablespecs:
                for fieldspec in tablespecs[ table_name ]:
                    if fieldspec.field_name in self._SUBJECT_KEY_FIELDS:
                        continue
                    field_name = formatutilities.db_identifier_unquote( fieldspec.field_name )
                    writer.writerow( [ field_name, '', '' ] )

    def _assign_values_by_name( self, tablespec, row ):
        """
        Creates a dictionary of values by name from the specified row using the specified tablespec.

        Parameters:

            *tablespec* : :class:`TableSpec`
                TableSpec that defines the layout of the information in the current row - the row metadata.

            *row* : one row SQL result set
                The row from the SQL result set.

        Returns: int
            A count of the records in the specified output table.
        """
        if not tablespec:
            msg = 'tablespec is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not row:
            msg = 'row is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        #self._log.debug( '_assign_values_by_name - tablespec[{}]'.format( tablespec ) )
        #self._log.debug( '_assign_values_by_name - row[{}]'.format( row ) )

        values_by_name = { }
        for column_num in range( len( row ) ):
            # self._log.debug( '_assign_values_by_name - column_num[{}]'.format( column_num ) )
            fieldspec = tablespec[ column_num ]
            value = row[ fieldspec.ordinal_position - 1 ]
            values_by_name[ fieldspec.field_name ] = value
            #self._log.debug( '_assign_values_by_name - values_by_name[{}]'.format( values_by_name ) )

        # self._log.debug( '_assign_values_by_name - values_by_name[{}]'.format( values_by_name ) )
        return values_by_name

    def _clone_non_matching( self ):
        """
        This method first creates a temporary table containing all of the matching (as defined above) records. Then, this
        clones (copies) the non-matching (as define above) records from the input tables to the output
        tables by using the temporary table to weed out all of the matches. This leaves only the matching rows to be
        processed and written to the output tables by other methods.

        Parameters: none

        Returns: nothing
        """
        self._create_temp_matching_table( input_table_name=self._input_table_names[ FLAT_TABLE_KEY_NAME ],
            temp_table_name=self._temp_matching_table_name )
        self._create_output_tables_wo_matching( temp_table_name=self._temp_matching_table_name )

    def _compare_and_merge( self, initial_row, corresp_rows, force_one_only ):
        """
        Performs the merge by integrating the initial row with the corresponding rows.

        Corresponding rows are rows that match the initial row.

        Parameters:

            *initial_row* : one row SQL result set
                The initial record to compare to.

            *corresp_rows* : SQL result set
                The records corresponding (matching) the initial row.

            *force_one_only* : boolean (true/false)
                For a given record, if true and there are no complementary records, then select the record with the
                highest record_priority value and then lowest sort value. If false, keep both records.

        Returns: dictionary of lists
            A dictionary of tables each containing an list of the merged records (dictionaries of field names
            and field values).
        """
        merged_records_by_table = { }
        merged_records_by_table[ self._output_table_names[ FLAT_TABLE_KEY_NAME ] ] = [ ]
        if not initial_row:
            msg = 'initial_row is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not corresp_rows:
            msg = 'corresp_rows is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if force_one_only is None:
            msg = 'force_one_only is null.'
            self._log.error( msg )
            raise ValueError( msg )

        flat_values_by_name_A = { }
        for column_num in range( len( initial_row ) ):
            fieldspec = self._input_tablespecs[ FLAT_TABLE_KEY_NAME ][ column_num ]
            value = initial_row[ fieldspec.ordinal_position - 1 ]
            if isinstance( value, str ):
                value = value.rstrip( )
            flat_values_by_name_A[ fieldspec.field_name ] = value

        self._log.info( '_compare_and_merge - A ({sort_field}/{ident_field})[{sort_value_A}/{ident_value1}]'.format(
            sort_field=self._sort_variable_name, ident_field=self._flat_table_identity_field_name,
            sort_value_A=flat_values_by_name_A[ self._sort_variable_name ],
            ident_value1=flat_values_by_name_A[ self._flat_table_identity_field_name ] ) )

        flat_merged_record = flat_values_by_name_A
        for flat_result in corresp_rows:
            flat_values_by_name_B = self._assign_values_by_name( tablespec=self._input_tablespecs[ FLAT_TABLE_KEY_NAME ],
                row=flat_result )

            if flat_values_by_name_B[ self._flat_table_identity_field_name ] in self._processed_idents:
                # Skip if the record has already been processed.
                continue  # Record already processed -- skipping.

            self._log.info( '_compare_and_merge - A ({sort_field}/{ident_field})[{sort_value_A}/{ident_value1}] '
                            ' vs B ({sort_field}/{ident_field})[{sort_value_B}/{ident_value2}]'.format(
                sort_field=self._sort_variable_name, ident_field=self._flat_table_identity_field_name,
                sort_value_A=flat_merged_record[ self._sort_variable_name ],
                ident_value1=flat_merged_record[ self._flat_table_identity_field_name ],
                sort_value_B=flat_values_by_name_B[ self._sort_variable_name ],
                ident_value2=flat_values_by_name_B[ self._flat_table_identity_field_name ] ) )

            record_priority_A = None
            if self._RESRULE_RECORD_PRIORITY in self._variables_by_resolution_rule:
                record_priority_A = flat_merged_record[
                    self._variables_by_resolution_rule[ self._RESRULE_RECORD_PRIORITY ].variable_name ]
                # self._log.debug( '_compare_and_merge - record_priority_A[{}]'.format( record_priority_A ) )

            record_priority_B = None
            if self._RESRULE_RECORD_PRIORITY in self._variables_by_resolution_rule:
                record_priority_B = flat_values_by_name_B[
                    self._variables_by_resolution_rule[ self._RESRULE_RECORD_PRIORITY ].variable_name ]
                # self._log.debug( '_compare_and_merge - record_priority_B[{}]'.format( record_priority_B ) )

            if ( record_priority_A >= self._RECORD_PRIORITY_THRESHOLD) or (
                    record_priority_B >= self._RECORD_PRIORITY_THRESHOLD):
                flat_merged_record = self._pick_one( record_priority_A=record_priority_A, record_priority_B=record_priority_B,
                    record_A=flat_merged_record, record_B=flat_values_by_name_B )
                continue

            overlapping_unit_indicator = False
            for resrule in self._variables_by_resolution_rule:
                if self._RESRULE_UNIT_INDICATOR_RE.match( resrule ):
                    variable_name = self._variables_by_resolution_rule[ resrule ].variable_name
                    if ( flat_merged_record[ variable_name ] == 1 ) and (flat_values_by_name_B[ variable_name ] == 1 ):
                        overlapping_unit_indicator = True
                        self._log.info(
                            '_compare_and_merge - A ({sort_field}/{ident_field})[{sort_value_A}/{ident_value1}] '
                            ' vs B ({sort_field}/{ident_field})[{sort_value_B}/{ident_value2}] '
                            ' {resrule} [{value1}/{value2}] overlap so either force_one_only or keep both...'.format(
                                sort_field=self._sort_variable_name,
                                ident_field=self._flat_table_identity_field_name,
                                sort_value_A=flat_merged_record[ self._sort_variable_name ],
                                ident_value1=flat_merged_record[ self._flat_table_identity_field_name ],
                                sort_value_B=flat_values_by_name_B[ self._sort_variable_name ],
                                ident_value2=flat_values_by_name_B[ self._flat_table_identity_field_name ], resrule=resrule,
                                value1=flat_merged_record[ variable_name ],
                                value2=flat_values_by_name_B[ variable_name ] ) )

            if overlapping_unit_indicator:  # Skip if unit indicator is true for both records.
                self._counts.inc_overlapping_records( )
                if force_one_only:
                    self._counts.inc_force_one_only_records( )
                    flat_merged_record = self._pick_one( record_priority_A=record_priority_A,
                        record_priority_B=record_priority_B,
                        record_A=flat_merged_record, record_B=flat_values_by_name_B )
                    if self._MERGE_REPORT_FIELD_NAME not in flat_merged_record:
                        flat_merged_record[ self._MERGE_REPORT_FIELD_NAME ] = ''
                    flat_merged_record[ self._MERGE_REPORT_FIELD_NAME ] += ' Forced.'
                    merged_records_by_table.update( self._merge_subject_records( flat_values_by_name_A=flat_merged_record,
                        flat_values_by_name_B=flat_merged_record ) )
                else:
                    if self._MERGE_REPORT_FIELD_NAME not in flat_merged_record:
                        flat_merged_record[ self._MERGE_REPORT_FIELD_NAME ] = ''
                    flat_merged_record[ self._MERGE_REPORT_FIELD_NAME ] += (
                        ' Records [{}] and [{}] have an overlapping attempt indicator column.'.format(
                            flat_merged_record[ self._sort_variable_name ],
                            flat_values_by_name_B[ self._sort_variable_name ] ) )
                    if self._MERGE_REPORT_FIELD_NAME not in flat_values_by_name_B:
                        flat_values_by_name_B[ self._MERGE_REPORT_FIELD_NAME ] = ''
                    flat_values_by_name_B[ self._MERGE_REPORT_FIELD_NAME ] += (
                        ' Records [{}] and [{}] have an overlapping attempt indicator column.'.format(
                            flat_values_by_name_B[ self._sort_variable_name ],
                            flat_merged_record[ self._sort_variable_name ] ) )

                    # Keep both child records. flat_values_by_name_A in this case.
                    # First, check if any subject item records exist for the parent.
                    subject_item = ('C', flat_merged_record[self._flat_table_identity_field_name], 1)
                    # self._log.debug( '_compare_and_merge - subject_item[{}]'.format( subject_item ) )
                    if subject_item in self._processed_subject_items:
                        self._counts.inc_warnings( )
#                         self._log.warn( '_compare_and_merge - flat_table_id/id[{subject_item}] already has items.'.format(
#                             subject_item=subject_item ) )
                        self._log.warn( '_compare_and_merge - ({sort_field}/{ident_field})[{sort_value_A}/{ident_value1}] '
                                        'subject[{subject_item}] already has items.'.format( 
                                        sort_field=self._sort_variable_name, ident_field=self._flat_table_identity_field_name, 
                                        sort_value_A=flat_merged_record[ self._sort_variable_name ],
                                        ident_value1=flat_merged_record[ self._flat_table_identity_field_name ], 
                                        subject_item=subject_item ) )
                    else:
                        merged_records_by_table.update( self._merge_subject_records( flat_values_by_name_A=flat_merged_record,
                            flat_values_by_name_B=flat_merged_record ) )

                    # Keep both child records. flat_values_by_name_B in this case.
                    records_B = self._merge_subject_records( flat_values_by_name_A=flat_values_by_name_B,
                        flat_values_by_name_B=flat_values_by_name_B )
                    for table_key in merged_records_by_table:
                        if table_key != self._output_table_names[ FLAT_TABLE_KEY_NAME ]:
                            merged_records_by_table[ table_key ].extend( records_B[ table_key ] )

                    merged_records_by_table[ self._output_table_names[ FLAT_TABLE_KEY_NAME ] ].append( flat_values_by_name_B )
                    self._processed_idents.append( flat_values_by_name_B[ self._flat_table_identity_field_name ] )
            else:
                self._log.info(
                    '_compare_and_merge - A ({sort_field}/{ident_field})[{sort_value_A}/{ident_value1}] '
                    ' vs B ({sort_field}/{ident_field})[{sort_value_B}/{ident_value2}] '
                    ' merge A with B...'.format( sort_field=self._sort_variable_name,
                        ident_field=self._flat_table_identity_field_name,
                        sort_value_A=flat_merged_record[ self._sort_variable_name ],
                        ident_value1=flat_merged_record[ self._flat_table_identity_field_name ],
                        sort_value_B=flat_values_by_name_B[ self._sort_variable_name ],
                        ident_value2=flat_values_by_name_B[ self._flat_table_identity_field_name ] ) )

                flat_values_by_name_B = self._assign_values_by_name( tablespec=self._input_tablespecs[ FLAT_TABLE_KEY_NAME ],
                    row=flat_result )

                # self._log.debug( '_compare_and_merge - flat_values_by_name_B[{}]'.format(
                # flat_values_by_name_B ) )
                if self._MERGE_REPORT_FIELD_NAME not in flat_merged_record:
                    flat_merged_record[ self._MERGE_REPORT_FIELD_NAME ] = ''
                flat_merged_record[ self._MERGE_REPORT_FIELD_NAME ] += (
                    ' Merged [{}] with '.format( flat_merged_record[ self._sort_variable_name ] ) )
                flat_merged_record = self._merge_values( flat_values_by_name_A=flat_merged_record,
                    flat_values_by_name_B=flat_values_by_name_B, merge_values_by_name_A=flat_merged_record,
                    merge_values_by_name_B=flat_values_by_name_B )
                if self._MERGE_REPORT_FIELD_NAME in flat_merged_record:
                    flat_merged_record[ self._MERGE_REPORT_FIELD_NAME ] += (
                        ' [{}].'.format( flat_values_by_name_B[ self._sort_variable_name ] ) )

                self._counts.inc_merged_records( )
                self._processed_idents.append(
                    flat_values_by_name_B[ self._flat_table_identity_field_name ] )  # Consider both records processed.

                merged_records_by_table.update( self._merge_subject_records( flat_values_by_name_A=flat_merged_record,
                    flat_values_by_name_B=flat_values_by_name_B ) )

        merged_records_by_table[ self._output_table_names[ FLAT_TABLE_KEY_NAME ] ].append( flat_merged_record )
        self._processed_idents.append( flat_values_by_name_A[ self._flat_table_identity_field_name ] )
        # self._log.debug( '_compare_and_merge - merged_records_by_table[{}]'.format( merged_records_by_table ) )
        return merged_records_by_table

    def _count_output_rows( self, output_table_name ):
        """
        Counts the number of records in the specified output table.

        Parameters:

            *output_table_name* : string
                Output database table name (output dataset).

        Returns: int
            A count of the records in the specified output table.
        """
        count = None
        if not output_table_name:
            msg = 'output_table_name is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        sql = self._SELECT_OUTPUT_COUNT.format( key_field_name=self._flat_table_identity_field_name,
            output_table_schema=self._output_db_context.schema, output_table_name=output_table_name )
        results = self._input_db_context.execute( sql )
        if results and results[ 0 ] and results[ 0 ][ 0 ]:
            count = results[ 0 ][ 0 ]
        return count

    def _create_output_tables_wo_matching( self, temp_table_name ):
        """
        Creates the specified output table containing all of the non-matching records from the specified input table. Uses
        the specified temporary table of matching records to select all of the non-matching records to put into the output
        table.

        Parameters:

            *temp_table_name* : string
                Database table name for the temporary table containing the matching records.

        Returns: nothing
        """
        if not temp_table_name:
            msg = 'temp_table_name is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        for key in self._output_table_names:
            if key == FLAT_TABLE_KEY_NAME:
                sql = self._CREATE_OUTPUT_FLAT_TABLE_WO_MATCHING_SQL.format( output_table_schema=self._output_db_context.schema,
                    output_table_name=self._output_table_names[ key ], input_table_schema=self._input_db_context.schema,
                    input_table_name=self._input_table_names[ key ], temp_table_schema=self._input_db_context.schema,
                    temp_table_name=temp_table_name, key_field_name=self._flat_table_identity_field_name,
                    merge_report_field_name=self._MERGE_REPORT_FIELD_NAME,
                    index_name=(formatutilities.db_identifier_unquote( self._sort_variable_name ) + '_ui'),
                    sort_column_name=self._sort_variable_name )
            else:
                sql = self._CREATE_OUTPUT_SUBJECT_TABLE_WO_MATCHING_SQL.format(
                    output_table_schema=self._output_db_context.schema,
                    output_table_name=self._output_table_names[ key ], input_table_schema=self._input_db_context.schema,
                    input_table_name=self._input_table_names[ key ], temp_table_schema=self._input_db_context.schema,
                    temp_table_name=temp_table_name, key_field_name=self._flat_table_identity_field_name,
                    subject_key_field_name=self._SUBJECT_FLAT_TABLE_ID_FIELD,
                    merge_report_field_name=self._MERGE_REPORT_FIELD_NAME,
                    index_name=(formatutilities.db_identifier_unquote( self._sort_variable_name ) + '_ui'),
                    sort_column_name=self._sort_variable_name )

            if self._debug_sql_firstpass:
                self._log.debug( '_create_output_tables_wo_matching - sql[{}]'.format( sql ) )
            self._output_db_context.executeNoResults( query=sql, commit=True )

        count = self._count_output_rows( output_table_name=self._output_table_names[ FLAT_TABLE_KEY_NAME ] )

        if count:
            self._counts.inc_unmatching_input_rows_read( rows_read=count )
            self._counts.inc_unmatching_output_rows_written( rows_written=count )

    def _create_temp_matching_table( self, input_table_name, temp_table_name ):
        """
        Creates the specified temporary table containing the identity values of all of the matching records from the specified
        input table.

        Parameters:

            *input_table_name* : string
                Input database table name (input dataset).

            *temp_table_name* : string
                Database table name for the temporary table containing the matching records.

        Returns: nothing
        """
        if not input_table_name:
            msg = 'input_table_name is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not temp_table_name:
            msg = 'temp_table_name is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        matching_where_clause = self._format_matching_where_clause( )
        sql = self._CREATE_TEMP_MATCHING_TABLE_SQL.format( key_field_name=self._flat_table_identity_field_name,
            input_table_schema=self._input_db_context.schema, input_table_name=input_table_name,
            temp_table_schema=self._input_db_context.schema, temp_table_name=temp_table_name,
            matching_where_clause=matching_where_clause, sort_column_name=self._sort_variable_name )
        if self._debug_sql_firstpass:
            self._log.debug( '_create_temp_matching_table - sql[{}]'.format( sql ) )
        self._output_db_context.executeNoResults( query=sql, commit=True )

    def _find_corresp( self, input_table_name, pk_values, sk_values, identity ):
        """
        Using the specified primary keys, secondary keys, and identity values queries for the corresponding rows from the
        specified input table.

        Parameters:

            *input_table_name* : string
                Input database table name (input dataset).

            *pk_values* : list
                An list of the primary key values.

            *sk_values* : list
                An list of the secondary key values.

            *identity* : int
                The identity column value.

        Returns: list
            An list of the corresponding records (a SQL result set). None if none.
        """
        results = None
        if not input_table_name:
            msg = 'input_table_name is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not pk_values:
            msg = 'pk_values is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not sk_values:
            msg = 'sk_values is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not identity:
            msg = 'identity is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        # self._log.debug( '_find_corresp - input_table_name[{}] column_names[{}] pk_values[{}] sk_values[{}] sort_column[{}]'
        #     .format( input_table_name, column_names, pk_values, sk_values, sort_column ) )

        where_clause = self._format_corresp_where_clause( )

        sql = self._SELECT_CORRESP.format( input_table_schema=self._input_db_context.schema, input_table_name=input_table_name,
            temp_table_schema=self._input_db_context.schema, temp_table_name=self._temp_matching_table_name,
            key_field_name=self._flat_table_identity_field_name, corresp_where_clause=where_clause,
            sort_column_name=self._sort_variable_name )
        if self._debug_sql_firstpass:
            self._log.debug( '_find_corresp - sql[{}]'.format( sql ) )

        # For each pk_value, there are 3 parameter positions that need to have values in the where clause,
        #     i.e. three question marks for each pk_value.
        extended_pk_values = [ ]
        for value in pk_values:
            extended_pk_values.append( value )
            extended_pk_values.append( value )
            extended_pk_values.append( value )

        # For each sk_value, there are 3 parameter positions that need to have values in the where clause,
        #     i.e. three question marks for each sk_value.
        extended_sk_values = [ ]
        for value in sk_values:
            extended_sk_values.append( value )
            extended_sk_values.append( value )
            extended_sk_values.append( value )

        parameters = extended_pk_values + extended_sk_values + [ identity ]
        self._log.debug( '_find_corresp - parameters[{}]'.format( parameters ) )
        results = self._input_db_context.execute( sql, parameters )
        if results and results[ 0 ]:
            self._counts.inc_corresp_records( )

        # self._log.debug( '_find_corresp - results[{}]'.format( results ) )
        return results

    def _format_corresp_where_clause( self ):
        """
        Creates a where clause for all of the corresponding records in the input table using the primary and secondary keys.

        Returns: string
            The formatted SQL where clause for corresponding records to be combined with the select clause.
        """
        where_clause = ' '
        is_first = True
        for variable_name in self._primary_key_variable_names:
            if is_first:
                is_first = False
            elif len( self._primary_key_variable_names ) > 1:
                where_clause += " \n" + " " * 12 + "AND "
            where_clause += (
                " ( ? IS NOT NULL ) AND ( ? <> '' ) AND ( itn.{variable_name} IS NOT NULL )  AND ( itn.{variable_name} <> '' ) "
                "AND ( itn.{variable_name} = ? ) ".format( variable_name=variable_name ) )
        where_clause += '\n'
        tablespec = self._input_tablespecs[ FLAT_TABLE_KEY_NAME ]
        is_first = True
        for variable_name in self._secondary_key_variable_names:
            # self._log.debug( "_format_corresp_where_clause - variable_name[{}]".format( variable_name ) )
            if is_first:
                is_first = False
                where_clause += " \n" + " " * 12 + "AND "
            elif len( self._secondary_key_variable_names ) > 1:
                where_clause += " \n" + " " * 12 + "AND "

            if tablespec[ variable_name ].basic_type in ('VARCHAR', 'NVARCHAR'):
                #
                # NOTE: For some cases, when the replacement parameter is for a VARCHAR or NVARCHAR field,
                # the replacement parameter is seen as TEXT or NTEXT. It appears to be a problem in the database driver,
                # although the developer of the driver claims not. The solution seems to be to convert the replacement
                # parameter to NVARCHAR (in our case), wherever it would be tested against character data.
                #
                where_clause += ( " ( ( ? IS NULL ) OR ( CONVERT(NVARCHAR(MAX), ?) = '' ) OR ( itn.{variable_name} IS NULL ) "
                                  " OR ( itn.{variable_name} = '' ) "
                                  " OR ( itn.{variable_name} = CONVERT(NVARCHAR(MAX), ?) ) ) ".format(
                    variable_name=variable_name ) )
            else:
                where_clause += ( " ( ( ? IS NULL ) OR ( ? = '' ) OR ( itn.{variable_name} IS NULL ) "
                                  " OR ( itn.{variable_name} = '' ) OR ( itn.{variable_name} = ? ) ) ".format(
                    variable_name=variable_name ) )
        where_clause += " \n" + " " * 12 + "AND {} <> ?".format( self._flat_table_identity_field_name )
        #self._log.debug("_format_corresp_where_clause - where_clause[{}]".format(where_clause))
        return where_clause

    def _format_matching_where_clause( self ):
        """
        Creates a where clause for all of the matching records in the input table using the primary and secondary keys.

        Returns: string
            The formatted SQL where clause for matching records to be combined with the select clause.
        """
        where_clause = " "
        is_first = True
        for variable_name in self._primary_key_variable_names:
            if is_first:
                is_first = False
            elif len( self._primary_key_variable_names ) > 1:
                where_clause += " \n" + " " * 24 + "AND "
            where_clause += ( " ( itn1.{variable_name} IS NOT NULL ) AND ( itn1.{variable_name} <> '' ) "
                              " AND ( itn2.{variable_name} IS NOT NULL ) AND ( itn2.{variable_name} != '' ) "
                              " AND ( itn2.{variable_name} = itn1.{variable_name} ) ".format( variable_name=variable_name ) )
        where_clause += "\n"
        is_first = True
        for variable_name in self._secondary_key_variable_names:
            if is_first:
                is_first = False
                where_clause += " \n" + " " * 24 + "AND "
            elif len( self._secondary_key_variable_names ) > 1:
                where_clause += " \n" + " " * 24 + "AND "
            where_clause += (
                " ( ( itn1.{variable_name} IS NULL ) OR ( itn1.{variable_name} = '' ) "
                " OR ( itn2.{variable_name} IS NULL ) OR ( itn2.{variable_name} IN ('', itn1.{variable_name}) ) ) "
                .format( variable_name=variable_name ) )
        where_clause += "\n"
        where_clause += ( " \n" + " " * 24 + "AND ( itn2.{identity_field} <> itn1.{identity_field} ) ").format(
            identity_field=self._flat_table_identity_field_name )
        return where_clause

    def _initialize_variable_dicts( self ):
        """
        Initializes various instance variables using the specified mergespec file and input table name,
        most notably the dictionaries of variables sorted by name, resolution rule, import order, primary keys,
        and secondary keys. It also retrieves the :class:`TableSpec` object for the specified input table.

        Parameters: none

        Returns: nothing
        """
        mergespec_reader = SafeExcelReader( run_context=self._run_context, filename=self._mergespec_file )
        if not self._validate_mergespec_file( mergespec_reader=mergespec_reader ):
            self._counts.inc_errors( )
            msg = 'Invalid mergespec file [{}].'.format( self._mergespec_file )
            self._log.error( msg )
            raise ValueError( msg )

        order = 0
        for row in mergespec_reader.getRows( ):
            if row[ 0 ]:
                variable_name = formatutilities.db_identifier_quote( row[ 0 ].lower( ) )
                resolution_rule = row[ 1 ].lower( ) if row[ 1 ] else ''
                key = row[ 2 ].lower( ) if row[ 2 ] else ''
                variable = Variable( variable_name=variable_name, resolution_rule=resolution_rule, key=key, import_order=order )
                self._variables_by_name[ variable_name ] = variable
                self._variables_by_resolution_rule[ resolution_rule ] = variable
                if key:
                    if self._PRIMARY_KEY_RE.match( key ):
                        self._primary_key_variable_names[ variable_name ] = variable
                    elif self._SECONDARY_KEY_RE.match( key ):
                        self._secondary_key_variable_names[ variable_name ] = variable
                    elif self._SORT_KEY_RE.match( key ):
                        if self._sort_variable_name is not None:
                            self._counts.inc_errors( )
                            msg = 'There can only be one Sort variable defined in the spec.'
                            self._log.error( msg )
                            raise ValueError( msg )
                        self._sort_variable_name = variable_name
                order += 1

        for key in self._input_table_names:
            self._input_tablespecs[ key ] = self._input_db_context.getTableSpec( table_name=self._input_table_names[ key ] )

    def _insert_output( self, output_table_name, output_record ):
        """
        This method inserts the values in the output_record into the output table.

        Parameters:

            *output_table_name* : string
                Output database table name (output dataset).

            *output_record* : dictionary { string : value }
                A dictionary of field names and field values for the record.

        Returns: nothing
        """
        if not output_table_name:
            msg = 'output_table_name is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not output_record:
            msg = 'output_record is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
            # self._log.debug( '_insert_output - output_table_name[{}] output_record[{}]'.format(
            # output_table_name,output_record ) )

        column_names_str = ''
        parameters_str = ''
        values = [ ]
        for name in output_record:
            column_names_str += '{}, '.format( name )
            parameters_str += '?, '
            values.append( output_record[ name ] )
        column_names_str = column_names_str.rstrip( ', ' )
        parameters_str = parameters_str.rstrip( ', ' )
        # self._log.debug( '_insert_output - column_names_str[{}] column_values_str[{}]'.format(
        # column_names_str, column_values_str ) )

        if self._identity_insert:
            insert_sql = self._INSERT_SQL_W_IDENTITY_INSERT
        else:
            insert_sql = self._INSERT_SQL_WO_IDENTITY_INSERT
        sql = insert_sql.format( table_schema=self._output_db_context.schema, table_name=output_table_name,
            column_names=column_names_str, column_values=parameters_str )
        if self._debug_sql_firstpass:
            self._log.debug( '_insert_output - sql[{}]'.format( sql ) )

        self._output_db_context.executeNoResults( sql, values )

    def _merge_subject_records( self, flat_values_by_name_A, flat_values_by_name_B ):
        """
        This method merges the subject (item) records.

        Parameters:

            *flat_values_by_name_A* : dictionary { string : value }
                A dictionary of field names and field values for flat record A.

            *flat_values_by_name_B* : dictionary { string : value }
                A dictionary of field names and field values for flat record B.

        Returns: dictionary of lists
            A dictionary of tables each containing an list of the merged records (dictionaries of field names
            and field values).
        """
        merged_records_by_table = { }

        # self._log.debug( '_merge_subject_records - _input_table_names[{}]'.format( self._input_table_names ) )
        for table_key in self._input_table_names:
            # self._log.debug( '_merge_subject_records - input_table_key[{}]'.format( table_key ) )
            if table_key == FLAT_TABLE_KEY_NAME:
                pass
            else:
                merged_records_by_table[ self._output_table_names[ table_key ] ] = [ ]
                subject_merged_record = { }
                query_A = self._SELECT_SUBJECT.format( input_table_schema=self._input_db_context.schema,
                    input_table_name=self._input_table_names[ table_key ], id_field=self._SUBJECT_FLAT_TABLE_ID_FIELD )
                if self._debug_sql_firstpass:
                    self._log.debug( '_merge_subject_records - sql[{}]'.format( query_A ) )

                parameters_A = [ flat_values_by_name_A[ self._flat_table_identity_field_name ] ]
                # self._log.debug( '_merge_subject_records - parameters_A[{}]'.format( parameters_A ) )
                for subject_result in self._input_db_context.execute( query_A, parameters_A ):
                    subject_values_by_name_A = self._assign_values_by_name(
                        tablespec=self._input_tablespecs[ table_key ], row=subject_result )
                    # self._log.debug(
                    # '_merge_subject_records - subject_values_by_name_A[{}]'.format( subject_values_by_name_A ) )
                    subject_item_A = (
                        table_key, subject_values_by_name_A[ self._SUBJECT_FLAT_TABLE_ID_FIELD ],
                        subject_values_by_name_A[ '[id]' ] )
                    # self._log.debug( '_merge_subject_records - subject_item_A[{}]'.format( subject_item_A ) )

                    if flat_values_by_name_A == flat_values_by_name_B:  # Compare reference okay here.
                        # If the two input parameters are the same, then just return the record from the first query.
                        subject_merged_record = subject_values_by_name_A
                    else:
                        # If the two input parameters are different, then merge the values in the related records.
                        query_B = query_A + " AND [id] = ? "
                        parameters_B = [ flat_values_by_name_B[ self._flat_table_identity_field_name ],
                            subject_values_by_name_A[ '[id]' ] ]
                        # self._log.debug( '_merge_subject_records - parameters_B[{}]'.format( parameters_B ) )
                        for subject_result in self._input_db_context.execute( query_B, parameters_B ):
                            subject_values_by_name_B = self._assign_values_by_name(
                                tablespec=self._input_tablespecs[ table_key ],
                                row=subject_result )
                            # self._log.debug(
                            # '_merge_subject_records - subject_values_by_name_B[{}]'.format( subject_values_by_name_B ) )

                            subject_item_B = (
                                table_key, subject_values_by_name_A[ self._SUBJECT_FLAT_TABLE_ID_FIELD ],
                                subject_values_by_name_A[ '[id]' ] )
                            # self._log.debug( '_merge_subject_records - subject_item_B[{}]'.format( subject_item_B ) )

                            # NOTE: Could have (Should have?) processed all of the item columns and item rows for a subject for
                            # a flat table record using the same rules, so no need to evaluate each field.
                            subject_merged_record = self._merge_values( flat_values_by_name_A=flat_values_by_name_A,
                                flat_values_by_name_B=flat_values_by_name_B,
                                merge_values_by_name_A=subject_values_by_name_A,
                                merge_values_by_name_B=subject_values_by_name_B )
                            # self._log.debug(
                            #     '_merge_subject_records - subject_merged_record[{}]'.format( subject_merged_record ) )
                            self._processed_subject_items.append( subject_item_A )
                            self._processed_subject_items.append( subject_item_B )
                            # self._log.debug(
                            #     '_merge_subject_records - processed_                            subject_items[{}]'.format(
                            #         self._processed_subject_items ) )

                    merged_records_by_table[ self._output_table_names[ table_key ] ].append( subject_merged_record )

        return merged_records_by_table

    def _merge_value( self, merge_variable_name, variable_priority_A, variable_priority_B, values_by_name_A, values_by_name_B ):
        """
        For the given variable, select one of the two values provided in the input records. Uses the variable_priority values
        to influence the selection.

        Parameters:

            *merge_variable_name* : string
                The name of the variable

            *variable_priority_A* : integer
                An integer specifying the variable_priority of record A. Can be null.

            *variable_priority_B* : integer
                An integer specifying the variable_priority of record B. Can be null.

            *values_by_name_A* : dictionary { string : value }
                A dictionary of field names and field values for record 1.

            *values_by_name_B* : dictionary { string : value }
                A dictionary of field names and field values for record 2.

        Returns: whatever type the value is
            The merged value.
        """
        value = None
        if not merge_variable_name:
            msg = 'merge_variable_name is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not values_by_name_A:
            msg = 'values_by_name_A is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not values_by_name_B:
            msg = 'values_by_name_B is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        if variable_priority_B is None:
            value = values_by_name_A[ merge_variable_name ]
        elif variable_priority_A is None:
            value = values_by_name_B[ merge_variable_name ]
        elif variable_priority_A >= variable_priority_B:
            value = values_by_name_A[ merge_variable_name ]
        else:
            value = values_by_name_B[ merge_variable_name ]

        # self._log.debug( '_merge_value - variable[{}] priority a/b[{}/{}] values[{}/{}] selection[{}]'.format(
        #     merge_variable_name, variable_priority_A, variable_priority_B,
        #     values_by_name_A[ merge_variable_name ], values_by_name_B[ merge_variable_name ], value ) )
        return value

    def _merge_values( self, flat_values_by_name_A, flat_values_by_name_B, merge_values_by_name_A, merge_values_by_name_B ):
        """
        Merge the values provided in the input records. Uses the variable_priority values to influence the selection.

        NOTE: When merging the values in the flat tables, flat_values_by_name_A will be the same as merge_values_by_name_A,
        and the same will be true of the "B" records.

        Parameters:

            *flat_values_by_name_A* : dictionary { string : value }
                A dictionary of field names and field values for flat record A.

            *flat_values_by_name_B* : dictionary { string : value }
                A dictionary of field names and field values for flat record B.

            *merge_values_by_name_A* : dictionary { string : value }
                A dictionary of field names and field values for record A to be merged. May be the same record as
                flat_values_by_name_A or may be one of the subject/items tables.

            *merge_values_by_name_B* : dictionary { string : value }
                A dictionary of field names and field values for record B to be merged. May be the same record as
                flat_values_by_name_B or may be one of the subject/items tables.

        Returns: whatever type the value is
            The merged value.
        """
        merged_record = { }
        if not flat_values_by_name_A:
            msg = 'flat_values_by_name_A is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not flat_values_by_name_B:
            msg = 'flat_values_by_name_B is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not merge_values_by_name_A:
            msg = 'merge_values_by_name_A is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not merge_values_by_name_B:
            msg = 'merge_values_by_name_B is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        # self._log.debug(
        #     "_merge_values -\n\tmerge_values_by_name_A[{}]\n\tmerge_values_by_name_B[{}]".format( merge_values_by_name_A,
        # merge_values_by_name_B ) )

        variable_priority_A = (
            flat_values_by_name_A[ self._variables_by_resolution_rule[ self._RESRULE_VARIABLE_PRIORITY ].variable_name ] )
        variable_priority_B = (
            flat_values_by_name_B[ self._variables_by_resolution_rule[ self._RESRULE_VARIABLE_PRIORITY ].variable_name ] )

        # Make a deep copy of record to initialize the merged_record.
        for column in merge_values_by_name_A:
            # self._log.debug( "_merge_values - column[{}]".format( column ) )
            merged_record[ column ] = merge_values_by_name_A[ column ]

        for name in self._variables_by_name:
            resolution_rule = self._variables_by_name[ name ].resolution_rule.lower( )
            if not name in merged_record:  # Only process the fields in this table.
                continue
            if self._RESRULE_COMMON_RE.match( resolution_rule ):
                merged_record[ name ] = self._merge_value( merge_variable_name=name, variable_priority_A=variable_priority_A,
                    variable_priority_B=variable_priority_B, values_by_name_A=merged_record,
                    values_by_name_B=merge_values_by_name_B )
            elif self._RESRULE_COMMON_NONMISSING_RE.match( resolution_rule ):
                if merged_record[ name ] and not merge_values_by_name_B[ name ]:
                    pass  # Leave merged_record[name] with the same value.
                elif merge_values_by_name_B[ name ] and not merged_record[ name ]:
                    merged_record[ name ] = merge_values_by_name_B[ name ]
                else:  # Validate using variable_priority.
                    merged_record[ name ] = self._merge_value( merge_variable_name=name,
                        variable_priority_A=variable_priority_A, variable_priority_B=variable_priority_B,
                        values_by_name_A=merged_record, values_by_name_B=merge_values_by_name_B )
            elif self._RESRULE_OR_RE.match( resolution_rule ):
                merged_record[ name ] = merged_record[ name ] or merge_values_by_name_B[ name ]
            elif self._RESRULE_RECORD_PRIORITY_RE.match( resolution_rule ):
                merged_record[ name ] = self._merge_value( merge_variable_name=name, variable_priority_A=variable_priority_A,
                    variable_priority_B=variable_priority_B, values_by_name_A=merged_record,
                    values_by_name_B=merge_values_by_name_B )
            elif self._RESRULE_TEST_UNIT_RE.match( resolution_rule ):
                unit = self._RESRULE_TEST_UNIT_RE.match( resolution_rule ).group( 1 )
                attempt_variable_name = (
                    self._variables_by_resolution_rule[ 'unit {} indicator'.format( unit ) ].variable_name )
                # self._log.debug('_merge_values - attempt_variable_name[{}]'.format(attempt_variable_name))
                if attempt_variable_name in flat_values_by_name_B:
                    if flat_values_by_name_B[ attempt_variable_name ] > 0:
                        if attempt_variable_name in merge_values_by_name_B:
                            merged_record[ attempt_variable_name ] = merge_values_by_name_B[ attempt_variable_name ]
                        merged_record[ name ] = merge_values_by_name_B[ name ]
                    elif ((attempt_variable_name in flat_values_by_name_A)
                    and (flat_values_by_name_A[ attempt_variable_name ] == 0 )
                    and (flat_values_by_name_B[ attempt_variable_name ] == 0 )):
                        merged_record[ name ] = self._merge_value( merge_variable_name=name,
                            variable_priority_A=variable_priority_A,
                            variable_priority_B=variable_priority_B, values_by_name_A=merged_record,
                            values_by_name_B=merge_values_by_name_B )
                    else:
                        pass  # Leave merged_record[name] with the same value.
                else:
                    self._counts.inc_warnings( )
                    # self._log.warn(
                    #     '_merge_values - B ({sort_field}/{ident_field})[{sort_value}/{ident_value}] - '
                    #         'attempt_variable[{variable}] not in B'.format(
                    #         sort_field=self._sort_variable_name,
                    #         ident_field=self._flat_table_identity_field_name,
                    #         sort_value=flat_values_by_name_B[ self._sort_variable_name ],
                    #         ident_value=flat_values_by_name_B[
                    #             self._flat_table_identity_field_name ],
                    #         variable=attempt_variable_name ) )
                    self._log.warn(
                        '_merge_values - attempt_variable[{variable}] not in B'.format(
                            variable=attempt_variable_name ) )
            elif self._RESRULE_UNIT_INDICATOR_RE.match( resolution_rule ):
                pass  # Use Unit Indicator resolution rule to process Test Unit.
            elif self._RESRULE_VARIABLE_PRIORITY_RE.match( resolution_rule ):
                pass  # Evaluate after all other fields. (below)
            elif name == self._flat_table_identity_field_name:
                pass  # Added processing field. Not part of the original data.
            else:
                self._counts.inc_errors( )
                msg = 'No resolution rule for variable [{}]'.format( name )
                self._log.error( msg )
                raise ValueError( msg )

        variable_name = self._variables_by_resolution_rule[ self._RESRULE_VARIABLE_PRIORITY ].variable_name
        if variable_name in merged_record:
            merged_record[ variable_name ] = self._merge_value( merge_variable_name=variable_name,
                variable_priority_A=variable_priority_A, variable_priority_B=variable_priority_B,
                values_by_name_A=merged_record,
                values_by_name_B=merge_values_by_name_B )

        # self._log.debug('_merge_values - merged_record[{}]'.format(merged_record))
        return merged_record

    def _pick_one( self, record_priority_A, record_priority_B, record_A, record_B ):
        """
        Choose one of two specified records based on their specified record_priorities.

        Parameters:

            *record_priority_A* : int
                The record_priority for record 1. (null or >=0)

            *record_priority_B* : int
                The record_priority for record 2. (null or >=0)

            *record_A* : dictionary { string : value }
                A dictionary of field names and field values for record 1.

            *record_B* : dictionary { string : value }
                A dictionary of field names and field values for record 2.

        Returns: dictionary { string : value }
            A dictionary of field names and field values for the selected record.
        """
        return_record = None
        if not record_A:
            msg = 'record_A is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )
        if not record_B:
            msg = 'record_B is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        # self._log.debug( '_pick_one - sort_field/ident 1[{}/{}]  sort_field/ident 2[{}/{}]  record_priority 1/2[{}/{}]'
        # .format(
        #     record_A[ self._sort_variable_name ], record_A[self._flat_table_identity_field_name ],
        # record_B[ self._sort_variable_name ],
        #     record_B[self._flat_table_identity_field_name ], record_priority_A, record_priority_B ) )

        if (record_priority_B is None) or (record_priority_A > record_priority_B):
            return_record = record_A
            if self._MERGE_REPORT_FIELD_NAME not in return_record:
                return_record[ self._MERGE_REPORT_FIELD_NAME ] = ''
            return_record[ self._MERGE_REPORT_FIELD_NAME ] += ' Picked one. (A)'
        elif (record_priority_A is None) or (record_priority_A < record_priority_B):
            return_record = record_B
            if self._MERGE_REPORT_FIELD_NAME not in return_record:
                return_record[ self._MERGE_REPORT_FIELD_NAME ] = ''
            return_record[ self._MERGE_REPORT_FIELD_NAME ] += ' Picked one. (B)'
            # self._processed_idents.append( record_A[ self._flat_table_identity_field_name ] )
        else:
            return_record = record_A
            if self._MERGE_REPORT_FIELD_NAME not in return_record:
                return_record[ self._MERGE_REPORT_FIELD_NAME ] = ''
            return_record[ self._MERGE_REPORT_FIELD_NAME ] += ' Picked one. (A)'
            # self._processed_idents.append( record_B[ self._flat_table_identity_field_name ] )

        self._counts.inc_pick_one_records( )
        self._processed_idents.append( record_A[ self._flat_table_identity_field_name ] )  # Consider both records processed.
        self._processed_idents.append( record_B[ self._flat_table_identity_field_name ] )  # Consider both records processed.

        # self._log.debug( '_pick_one - sort_field/ident 1[{}/{}]  sort_field/ident 2[{}/{}]  picked [{}/{}]'.format(
        #     record_A[ self._sort_variable_name ], record_A[self._flat_table_identity_field_name ],
        # record_B[ self._sort_variable_name ],
        #     record_B[self._flat_table_identity_field_name ], return_record[ self._sort_variable_name ],
        #     return_record[self._flat_table_identity_field_name ] ) )

        return return_record

    def _process_matching( self, force_one_only=False ):
        """
        This method retrieves all of the matching records and calls :meth:`_compare_and_merge()` to merge the records,
        then inserts the results of the merge into the output table.

        Parameters:

            *force_one_only* : boolean (true/false)
                Generally: For a given record, if true and there are no complementary records, then select the record with the
                highest record_priority value and then lowest sort value. If false, keep both records.

                In this method: If true, then there must be a record_priority resolution rule. Pass the force_one_only value on
                to :meth:`_compare_and_merge()`.

        Returns: nothing
        """
        if force_one_only and (self._RESRULE_RECORD_PRIORITY not in self._variables_by_resolution_rule):
            self._counts.inc_errors( )
            msg = 'When force_one_only is specified, mergespec sheet must specify a "Record Priority" resolution rule.'
            self._log.error( msg )
            raise ValueError( msg )

        self._processed_idents = [ ]
        sql = self._SELECT_ONLY_MATCHING.format( input_table_schema=self._input_db_context.schema,
            input_table_name=self._input_table_names[ FLAT_TABLE_KEY_NAME ],
            temp_table_schema=self._input_db_context.schema,
            temp_table_name=self._temp_matching_table_name, key_field_name=self._flat_table_identity_field_name,
            sort_column_name=self._sort_variable_name )
        if self._debug_sql_firstpass:
            self._log.debug( '_process_matching - sql[{}]'.format( sql ) )

        for result in self._input_db_context.execute( sql ):
            self._counts.inc_matching_input_rows_read( )
            # self._log.debug( '_process_matching - input_rows_read[{}]'.format( self._counts.input_rows_read ) )
            if not self._counts.merge_input_rows_read % 100:
                self._log.info( '_process_matching - {}'.format( self._counts.format_str( ) ) )

            values_by_name = self._assign_values_by_name( tablespec=self._input_tablespecs[ FLAT_TABLE_KEY_NAME ],
                row=result )

            if values_by_name[ self._sort_variable_name ] is None:
                self._counts.inc_missing_sort_keys( )
                self._log.warn( '_process_matching - sort_variable/ident[{}/{}] missing sort key.'.format(
                    values_by_name[ self._sort_variable_name ], values_by_name[ self._flat_table_identity_field_name ] ) )
                continue  # Skipping missing sort key.

            if values_by_name[ self._flat_table_identity_field_name ] in self._processed_idents:
                # Skip if the record has already been processed.
                continue  # Record already processed -- skipping.

            pk_values = [ ]
            for key in self._primary_key_variable_names:
                pk_values.append( values_by_name[ self._variables_by_name[ key ].variable_name ] )
                # self._log.debug( ' _process_matching - pk_values[{}]'.format( pk_values ) )

            sk_values = [ ]
            for key in self._secondary_key_variable_names:
                value = values_by_name[ self._variables_by_name[ key ].variable_name ]
                if value:
                    sk_values.append( value )
                else:
                    sk_values.append( '' )
                    # self._log.debug( ' _process_matching - sk_values[{}]'.format( sk_values ) )

            merged_records_by_table = { }

            # self._log.debug( '_process_matching - A ({sort_field}/{ident_field})[{sort_value_A}/{ident_value1}]'.format(
            #     sort_field=self._sort_variable_name, ident_field=self._flat_table_identity_field_name,
            #     sort_value_A=values_by_name[self._sort_variable_name ], ident_value1=values_by_name[self
            # ._flat_table_identity_field_name ] ) )

            corresp_results = self._find_corresp( input_table_name=self._input_table_names[ FLAT_TABLE_KEY_NAME ],
                pk_values=pk_values, sk_values=sk_values, identity=values_by_name[ self._flat_table_identity_field_name ] )
            #self._log.debug( '_process_matching - \n    result1[{}]\n    corresp_results[{}]'.format( result1,
            #    corresp_results ) )
            intermediate_rows = [ ]
            if corresp_results:
                for corresp_result in corresp_results:
                    intermediate_rows.append( corresp_result )
                merged_records_by_table = self._compare_and_merge( initial_row=result, corresp_rows=intermediate_rows,
                    force_one_only=force_one_only )
            else:
                self._counts.inc_errors( )
                self._log.error(
                    '_process_matching - pk_values[{}] sk_values[{}] - No corresponding matching rows found'.format(
                        pk_values, sk_values ) )
                self._processed_idents.append( values_by_name[ self._flat_table_identity_field_name ] )

            for table_name in merged_records_by_table:
                # self._log.debug( '_process_matching - table[{}]'.format( table_name ) )
                for row in merged_records_by_table[ table_name ]:
                    # self._log.debug( '_process_matching - table[{}] row[{}]'.format( table_name, row ) )
                    self._insert_output( output_table_name=table_name, output_record=row )
                    if table_name == self._output_table_names[ FLAT_TABLE_KEY_NAME ]:
                        self._counts.inc_processed_output_rows_written( )

            self._debug_sql_firstpass = False

    def _validate_mergespec_file( self, mergespec_reader ):
        """
        Performs validations on the specified mergespec file using the provided mergespec_reader.

        Mergespec File Validations:

            There must be at least three columns in the mergespec file.

            The three required columns must be labeled 'variable_name', 'resolution_rule', and 'keys'.

            Exactly one variable must be marked 'variable_priority'.

            Exactly one variable must be marked 'sort'.

            Only one variable can be marked 'record_priority', but there may be none.

        Parameters:

            *mergespec_reader* : :class:`SafeExcelReader`
                An instance of :class:`SafeExcelReader` instantiated with the mergespec file.

        Returns: boolean
            True if the mergespec file is valid, false otherwise.
        """
        is_valid = False
        if not mergespec_reader:
            msg = 'mergespec_reader is null or empty.'
            self._log.error( msg )
            raise ValueError( msg )

        mergespec_file = mergespec_reader.filename
        variable_priority_count = 0
        record_priority_count = 0
        sort_count = 0
        rownum = 0
        for row in mergespec_reader.getRows( ):
            rownum += 1
            if len( row ) < 3:
                self._counts.inc_errors( )
                msg = 'Row[{}]: Invalid number of columns in mergespec file [{}].'.format( rownum, mergespec_file )
                self._log.error( msg )
                raise EOFError( msg )
            if 'variable_name' not in row:
                self._counts.inc_errors( )
                msg = 'Row[{}]: Missing column "variable_name" in mergespec file [{}].'.format( rownum, mergespec_file )
                self._log.error( msg )
                raise ValueError( msg )
            if 'resolution_rule' not in row:
                self._counts.inc_errors( )
                msg = 'Row[{}]: Missing column "resolution_rule" in mergespec file [{}].'.format( rownum, mergespec_file )
                self._log.error( msg )
                raise ValueError( msg )
            if 'keys' not in row:
                self._counts.inc_errors( )
                msg = 'Row[{}]: Missing column "keys" in mergespec file [{}].'.format( rownum, mergespec_file )
                self._log.error( msg )
                raise ValueError( msg )

            resolution_rule = row[ 'resolution_rule' ].lower( )
            # self._log.debug( '_validate_mergespec_file - row.resolution_rule[{}]'.format( row[ 'resolution_rule' ] ) )
            if self._RESRULE_VARIABLE_PRIORITY_RE.match( resolution_rule ):
                variable_priority_count += 1
            elif self._RESRULE_RECORD_PRIORITY_RE.match( resolution_rule ):
                record_priority_count += 1

            if row[ 'keys' ]:
                key = row[ 'keys' ]
                if self._RESRULE_SORT_RE.match( key ):
                    sort_count += 1

        if variable_priority_count > 1:
            self._counts.inc_errors( )
            msg = 'Too many variables marked as "variable_priority".'
            self._log.error( msg )
            raise ValueError( msg )
        elif variable_priority_count < 1:
            self._counts.inc_errors( )
            msg = 'No variables marked as "variable_priority".'
            self._log.error( msg )
            raise ValueError( msg )

        if record_priority_count > 1:
            self._counts.inc_errors( )
            msg = 'Too many variables marked as "record_priority".'
            self._log.error( msg )
            raise ValueError( msg )

        if sort_count != 1:
            self._counts.inc_errors( )
            msg = 'Must be exactly one variable marked as "sort".'
            self._log.error( msg )
            raise ValueError( msg )

        is_valid = True

        return is_valid


class Variable( object ):
    """
    Supporting class for :class:`ComplementaryMerge`. This class contains all of the attributes related to a variable as
    defined by the mergespec file, i.e. name, resolution_rule, and key, as well as the order in which the variables were
    imported. The count fields would be used to keep track of the number of the resolution rule or key should there be any,
    e.g. Fuzzy1 and Fuzzy2 for key.
    """
    variable_name = ''
    resolution_rule = ''
    resolution_rule_count = 0
    key = ''
    key_count = 0
    import_order = 0

    def __init__( self, variable_name, import_order, resolution_rule='', resolution_rule_count=0, key='', key_count=0 ):
        self.variable_name = variable_name
        self.resolution_rule = resolution_rule
        self.resolution_rule_count = resolution_rule_count
        self.key = key
        self.key_count = key_count
        self.import_order = import_order

    def __str__( self ):
        return ( 'name/rule/key/order[{0.variable_name}/{0.resolution_rule}={0.resolution_rule_count}/{0.key}={0.key_count}'
                 '/{0.import_order}]'.format( self ) )

    def format_str( self ):
        return_str = '''
            Variable:
                Name:       {0.variable_name}
                Rule:       {0.resolution_rule}
                Rule Count: {0.resolution_rule_count}
                Key:        {0.key}
                Key Count:  {0.key_count}
                Order:      {0.import_order}
            '''.format( self )
        return return_str


class Counts( object ):
    """
    Supporting class for :class:`ComplementaryMerge`. Provides counters for significant steps for checksums and debugging.
    """
    _errors = 0
    _warnings = 0
    _unmatching_input_rows_read = 0
    _matching_input_rows_read = 0
    _unmatching_output_rows_written = 0
    _processed_output_rows_written = 0
    _missing_sort_keys = 0
    _corresp_records = 0
    _force_one_only_records = 0
    _pick_one_records = 0
    _overlapping_records = 0
    _merged_records = 0

    @property
    def unmatching_input_rows_read( self ):
        return self._unmatching_input_rows_read

    @property
    def merge_input_rows_read( self ):
        return self._matching_input_rows_read

    @property
    def total_input_rows_read( self ):
        return self._unmatching_input_rows_read + self._matching_input_rows_read

    @property
    def premerge_output_rows_written( self ):
        return self._unmatching_output_rows_written

    @property
    def merge_output_rows_written( self ):
        return self._processed_output_rows_written

    @property
    def total_output_rows_written( self ):
        return self._unmatching_output_rows_written + self._processed_output_rows_written

    def inc_errors( self ):
        self._errors += 1

    def inc_warnings( self ):
        self._warnings += 1

    def inc_unmatching_input_rows_read( self, rows_read=1 ):
        self._unmatching_input_rows_read += rows_read

    def inc_matching_input_rows_read( self ):
        self._matching_input_rows_read += 1

    def inc_unmatching_output_rows_written( self, rows_written=1 ):
        self._unmatching_output_rows_written += rows_written

    def inc_processed_output_rows_written( self ):
        self._processed_output_rows_written += 1

    def inc_missing_sort_keys( self ):
        self._missing_sort_keys += 1

    def inc_corresp_records( self ):
        self._corresp_records += 1

    def inc_force_one_only_records( self ):
        self._force_one_only_records += 1

    def inc_pick_one_records( self ):
        self._pick_one_records += 1

    def inc_overlapping_records( self ):
        self._overlapping_records += 1

    def inc_merged_records( self ):
        self._merged_records += 1

    def __str__( self ):
        return (
            'errors/warnings/premerge_inputs_read/merge_inputs_read/premerge_outputs_written/merge_outputs_written'
            '/missing_sorts'
            '/overlapping/corresponding/force_one/merged/already_processed['
            '{0._errors}/{0._warnings}/{0._unmatching_input_rows_read}/{0._matching_input_rows_read}'
            '/{0._unmatching_output_rows_written}/{0._processed_output_rows_written}/{0._missing_sort_keys}'
            '/{0._overlapping_records}/{0._corresp_records}/{0._force_one_only_records}/{0._pick_one_records}'
            '/{0._merged_records}/{0._already_processed}]'.format( self ) )

    def format_str( self ):
        return_str = '''
            Counts:
                ERRORS:                            {0._errors}
                Warnings:                          {0._warnings}

                Unmatching Input Records Read:     {0._unmatching_input_rows_read}
                Matching Input Records Read:       {0._matching_input_rows_read}
                Total Input Records Read:          {0.total_input_rows_read}

                Missing Sort Keys:                 {0._missing_sort_keys}
                Corresponding Records:             {0._corresp_records}
                Force One Only Records:            {0._force_one_only_records}
                Pick One Records:                  {0._pick_one_records}
                Overlapping Unit Indicators:       {0._overlapping_records}
                Merged Score Records:              {0._merged_records}

                Unmatching Output Records Written: {0._unmatching_output_rows_written}
                Processed Output Records Written:  {0._processed_output_rows_written}
                Total Output Records Written:      {0.total_output_rows_written}
            '''.format( self )
        return return_str
