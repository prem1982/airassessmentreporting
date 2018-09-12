'''
Created on May 17, 2013

@author: temp_dmenes
'''

import copy
from exceptions import KeyboardInterrupt
import os
import re
import string

import win32com.client

from airassessmentreporting.airutility import SafeExcelReader, db_identifier_unquote

from mergedef import *

__all__ = [ 'create_spec_file', 'read_spec_file' ]

_KEY_ASSIGNMENT_RE = re.compile( '^(primary|required|secondary|optional|fuzzy)([0-9]+)$' )

def create_spec_file( filename, merge_def, if_not_exists=True,
                      break_if_create=True ):
    
    """Create an Excel file in which the data to be merged can be specified.
    
    When performing a merge, details of how to process the input tables can
    be read from a properly structured Excel workbook. This function creates
    such a workbook and populates it with default values. The default values
    are based on the structure of the input tables, which should exist at
    the time this function is called.
    
    Parameters
    ----------
    filename : str
        Name of the file to create.
        
    merge_def : :class:`MergeDef`
        A :class:`merge_def` object that defines the db_context and the names
        of the left and right input files.
        
    if_not_exists : bool optional
        Defaults to `True`. If `True`, only create the file if it does not
        already exist.
        
    break_if_create : bool optional
        Defaults to `True`. If `True`, raise an
        :class:`KeyboardInterrupt` after creating a new workbook. This
        will break the program so that the user can enter information into
        the workbook.
        
    Returns
    -------
    True if a new workbook was created, else False
    """
    messages = []
    db_context = merge_def.db_context
    run_context = db_context.runContext
    if not merge_def.get_actual_tables( messages ):
        for message in messages:
            run_context.error( message )
        raise ValueError( "Input tables not properly specified in MergeDef" )
    
    if os.path.exists( filename ):
        if if_not_exists:
            return False
        else:
            os.remove( filename )
        
    excel = win32com.client.Dispatch('Excel.Application')
    book = excel.Workbooks.Add()
    excel.DisplayAlerts = False
    while len( excel.Worksheets ) > 2:
        excel.Worksheets[2].Delete()
    excel.DisplayAlerts = True
        
    _write_sheet( excel.Worksheets[0], 'LEFT', merge_def.left_input_table )
    _write_sheet( excel.Worksheets[1], 'RIGHT', merge_def.right_input_table )
    excel.DisplayAlerts = False
    book.SaveAs( os.path.normpath( filename ) )
    book.Close()
    excel.DisplayAlerts = True
    if break_if_create:
        raise KeyboardInterrupt( "Execution ended. Please complete merge field specifier {}".format( filename ) )
    return True
        
def _write_sheet( sheet, name, table ):
    sheet.Name = name
    sheet.Range('A1:F1').Value = ('variable name', 'newname', 'priority',
                                  'keys', 'length', 'vartype')
    next_range = sheet.Range('A2:F2')
    priority = 'first' if name == 'LEFT' else 'second'
    for col in table:
        newname = '<drop>' if col in table.primary_key else '<same>'
        length = col.data_length if col.is_charish else None
        next_range.Value = ( col.field_name, newname, priority,
                             None, length, col.basic_type )
        next_range = next_range.Range( 'A2:F2' )


def read_spec_file( filename, merge_def ):
    """Populate the field definitions of a :class:`MergeDef`
    
    Reads field definitions for a spec file and create the appropriate
    :class:`MergeFieldSpec` items to the merge_def. The left_table, right_table
    and db_context properties of the :MergeDef:` must already be set before
    calling this method.
    
    Parameters
    ----------
    filename : str
        Name of the merge spec file.
        
    merge_def : :class:`MergeDef`
        The merge definition object into which the field definitions will be
        read.
        
    Returns
    -------
    None
    """
    messages = []
    db_context = merge_def.db_context
    run_context = db_context.runContext
    if not merge_def.get_actual_tables( messages ):
        for message in messages:
            run_context.error( message )
        raise ValueError( "Input tables not properly specified in MergeDef" )
    
    reader = SafeExcelReader( run_context, filename, sheet_name=0, get_names=True )
    del merge_def[:]
    
    # Checklists to confirm that all variables from each input table appear exactly once
    n_occurred_left = {}
    for field in merge_def.left_input_table.iterkeys():
        n_occurred_left[ field ] = 0
    n_occurred_right = {}
    for field in merge_def.right_input_table.iterkeys():
        n_occurred_right[ field ] = 0
        
    
    # Read the left side of the merge first
    required_keys = merge_def.required_merge_keys = []
    optional_keys = merge_def.optional_merge_keys = []
    fuzzy_keys = merge_def.fuzzy_merge_keys = []
    for row in reader.getRows():
        field_name, input_name, priority, key_assignment, data_type, data_length = \
                _extract_field_properties( row )
        if input_name not in merge_def.left_input_table:
            raise ValueError( 'Did not find column named {} in table {}'.format( field_name, merge_def.left_input_table ) )
        left_field = merge_def.left_input_table[ input_name ]
        n_occurred_left[ left_field.field_name ] += 1
        if field_name != '<drop>':
            if field_name in merge_def:
                raise ValueError( 'Attempting to create two merge fields with the same name' )
            merge_field = MergeFieldSpec( left_field, None, None )
            merge_field.field_name = field_name
            merge_def.add( merge_field )
            _write_type( merge_field, data_type, data_length )
            if priority == 'first':
                merge_field.priority_field = PRIORITY_LEFT
            elif priority == 'firstnonmissing':
                merge_field.priority_field = PRIORITY_LEFT_NONMISSING
            elif priority == 'second':
                # Note, this may later get changed to PRIORITY_RIGHT_NONMISSING
                merge_field.priority_field = PRIORITY_RIGHT
            elif priority is None or priority == '':
                merge_field.priority_field = PRIORITY_LEFT_ONLY
            else:
                raise ValueError( "Found priority {}; must be one of \"FIRST\", \"SECOND\", or \"FIRST NON-MISSING\"" )
        
        if key_assignment is not None and key_assignment != "":
            key_field = MergeFieldSpec( left_field, None, None )
            key_field.field_name = key_assignment
            match = _KEY_ASSIGNMENT_RE.match(key_assignment)
            if match is None:
                raise ValueError("Did not know how to interpret key expression {}".format(key_assignment))
            key_type, key_nbr = match.groups( (1, 2) )
            if key_type in ("primary", "required"):
                _add_key(required_keys, key_field, key_nbr, key_assignment)
            elif key_type in ("secondary", "optional"):
                _add_key(optional_keys, key_field, key_nbr, key_assignment)
            elif key_type == "fuzzy":
                _add_fuzzy_key(fuzzy_keys, key_field, key_nbr, key_assignment)
            else:
                raise AssertionError("This error really should not happen")
        

        
    # Now read the right side of the merge
    reader.sheetName = 1
    for row in reader.getRows():
        field_name, input_name, priority, key_assignment, data_type, data_length = \
                _extract_field_properties( row )
        if input_name not in merge_def.right_input_table:
            raise ValueError( 'Did not find column named {} in table {}'.format( field_name, merge_def.right_input_table ) )
        right_field = merge_def.right_input_table[ input_name ]
        n_occurred_right[ right_field.field_name ] += 1
        if field_name != '<drop>':
            if field_name not in merge_def:
                merge_field = MergeFieldSpec( None, right_field, PRIORITY_RIGHT_ONLY )
                merge_field.field_name = field_name
                merge_def.add( merge_field )
                _write_type( merge_field, data_type, data_length )
                if not ( priority is None or priority == '' ):
                    raise ValueError( "Field exists only on right side of merge: must have a blank priority" )

            else:
                merge_field = merge_def[ field_name ]
                merge_field.right_field = copy.copy( right_field )
                
                # Check for compatible type assignment
                if merge_field.basic_type != data_type:
                    raise ValueError( "Incompatible data types for field {}: {} on left to {} on right".format(
                            merge_field, merge_field.basic_type, data_type ) )
                if merge_field.is_charish and merge_field.data_length != data_length:
                    raise ValueError( "Data lengths differ for field {}: {} on left to {} on right".format(
                            merge_field, merge_field.data_length, data_length ) )
                    
                # Check for consistent priority assignment
                if merge_field.priority_field in (  PRIORITY_LEFT, PRIORITY_LEFT_NONMISSING ) and \
                        priority != 'second':
                    raise ValueError( "Inconsistent priority designations for field {}".format( merge_field ) )
                if merge_field.priority_field == PRIORITY_RIGHT and \
                        priority == 'second':
                    raise ValueError( "Inconsistent priority designations for field {}".format( merge_field ) )
                if merge_field.priority_field == PRIORITY_LEFT_ONLY:
                    raise ValueError( "Blank priority designation not permitted when field appears on both sides of merge" )
                if priority == 'firstnonmissing':
                    merge_field.priority_field = PRIORITY_RIGHT_NONMISSING
            
            # Do key assignment
            if key_assignment is not None and key_assignment != "":
                match = _KEY_ASSIGNMENT_RE.match(key_assignment)
                if match is None:
                    raise ValueError("Did not know how to interpret key expression {}".format(key_assignment))
                key_type, key_nbr = match.groups( (1, 2) )
                key_nbr = int( key_nbr )-1
                key_field = None
                if key_type in ("primary", "required"):
                    if key_nbr < len( required_keys ):
                        key_field = required_keys[ key_nbr ] 
                elif key_type in ("secondary", "optional"):
                    if key_nbr < len( optional_keys ):
                        key_field = optional_keys[ key_nbr ] 
                elif key_type == "fuzzy":
                    if key_nbr < len( fuzzy_keys ):
                        for key_field in fuzzy_keys[ key_nbr ]:
                            if key_field.right_field is None:
                                break
                else:
                    raise AssertionError("This error really should not happen")
                if key_field is None:
                    raise ValueError( "Key {} was specified only on right side of merge".format( key_assignment ) )
                if key_field.right_field is not None:
                    raise ValueError( "Too many keys defined on right side of merge with key designation {}".format( key_assignment ) )
                key_field.right_field = right_field
                
    # Confirm that each input variable was processed exactly once.
    succeed = True
    for k, v in n_occurred_left.items():
        if v != 1:
            succeed = False
            run_context.error( "Each variable from left input table must occur exactly once on spec sheet. {} appeared {} times".format( k, v ) )
    for k, v in n_occurred_right.items():
        if v != 1:
            succeed = False
            run_context.error( "Each variable from right input table must occur exactly once on spec sheet. {} appeared {} times".format( k, v ) )
    if not succeed:
        raise ValueError( "Missing or duplicate variables on merge spec sheet" )

def _extract_field_properties( row ):
    input_name = row['variable name'].lower()
    field_name = row['newname'].lower()
    if field_name == '<same>':
        field_name = input_name
    priority = row['priority'] or ''
    priority = str( priority ).lower().translate(None, string.whitespace + ',-')
    key_assignment = row['keys']
    if key_assignment is not None:
        key_assignment = str( key_assignment ).lower().translate(None, string.whitespace + '-,')
    data_type = row['vartype'].upper()
    if data_type == 'N':
        data_type = 'FLOAT'
    elif data_type == 'C':
        data_type = 'NVARCHAR'
    data_length = row['length']
    return field_name, input_name, priority, key_assignment, data_type, data_length


def _write_type( merge_field, data_type, data_length ):
    merge_field.basic_type = data_type
    if merge_field.is_charish:
        if data_length is None:
            raise ValueError( "Data length is required for data type {}".data_type )
        merge_field.data_length = data_length


def _add_key( collection, key_field, key_nbr, key_assignment ):
    key_nbr = int( key_nbr ) - 1
    while len( collection ) <= key_nbr:
        collection.append( None )
    if collection[ key_nbr ] is not None:
        raise ValueError( "Assigned two variables with key designation {}" \
                          .format( key_assignment ) )
    collection[ key_nbr ] = key_field
    
def _add_fuzzy_key( collection, key_field, key_nbr, key_assignment ):
    key_nbr = int( key_nbr ) - 1
    while len( collection ) <= key_nbr:
        collection.append( None )
    if collection[ key_nbr ] is None:
        collection[ key_nbr ] = ( key_field, )
        key_field.field_name = db_identifier_unquote( key_field.field_name ) + '_1'
    else:
        existing_key = collection[ key_nbr ]
        if len( existing_key ) > 1:
            raise ValueError( "Too many fuzzy keys defined with designation {}" \
                              .format( key_assignment ) )
        collection[ key_nbr ] = ( existing_key[ 0 ], key_field, )
        key_field.field_name = db_identifier_unquote( key_field.field_name ) + '_2'
