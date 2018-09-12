'''
Created on May 23, 2013

@author: temp_dmenes
'''

from numbers import Number

from airassessmentreporting.airutility import SafeExcelReader, dump, get_table_spec

__all__ = [ 'compare_tables', 'integer_compare', 'mixed_compare', 'to_str' ]

def dots_to_none( x ):
    if x == '.':
        return None
    return x

def to_str( x ):
    x = dots_to_none( x )
    if isinstance( x, Number ):
        return str( int( x ) )
    return str( x )

def integer_compare( x, y ):
    y = dots_to_none( y )
    if x is None:
        return y is None
    if y is None:
        return False
    try:
        return int( float( x ) + 0.5 ) == int( float( y ) + 0.5 )
    except ValueError:
        return False

def mixed_compare( x, y ):
    y = dots_to_none( y )
    if x is None:
        return y is None
    if y is None:
        return False
    if isinstance( x, ( str, unicode ) ) and isinstance( y, Number ):
        x = x.lstrip( '0' )
        return x == str( int( y ) )
    return x == y

def compare_tables( log_name, table, specimen_name, columns,
                    table_key_function, specimen_key_function, db_context=None,
                    skip=0 ):
    succeed = True
    log_stream = open( log_name, "w" )
    table = get_table_spec( table, db_context )
    db_context=table.db_context
    try:
        table_under_test = dump( table, db_context=db_context )
        specimen = [ row for row in SafeExcelReader(
                db_context.runContext,
                specimen_name,
                skip=skip ).getRows() ]
        
        if len( table_under_test ) == 0:
            if len( specimen ) == 0:
                log_stream.write( "Both tables are empty!" )
                return True
            log_stream.write( "Output table is empty; specimen has {} rows".format( len( specimen ) ) )
            return False
        
        if len( specimen ) == 0:
            log_stream.write( "Specimen is empty; output table has {} rows".format( len( table_under_test ) ) )
            return False
        
        # Check the column names
        table_columns = table_under_test[0].keys()
        specimen_columns = specimen[0].keys()
        new_columns=[]
        for t in columns:
            table_col_name, specimen_col_name, tester = t
            result = True
            if not table_col_name in table_columns:
                log_stream.write( "Output table missing column {}\n".format( table_col_name ) )
                result = False
            if not specimen_col_name in specimen_columns:
                log_stream.write( "Specimen missing column {}\n".format( specimen_col_name ) )
                result = False
            if result:
                new_columns.append( t )
            succeed = succeed and result
        columns = new_columns
        table_under_test.sort( key = table_key_function )
        specimen.sort( key = specimen_key_function )
        table_iterator = table_under_test.__iter__()
        specimen_iterator = specimen.__iter__()
        n_table_rows = 0
        n_specimen_rows = 0
        n_mismatched_data = 0
        n_compared = 0
        # Loop through both iterators in parallel
        try:
            table_row = table_iterator.next()
            n_table_rows += 1
            table_key = table_key_function( table_row )
            specimen_row = specimen_iterator.next()
            n_specimen_rows += 1
            specimen_key = specimen_key_function( specimen_row )
            while True:
                while table_key < specimen_key:
                    log_stream.write( 'Row {} in output but not in specimen\n'.format( table_key ) )
                    table_row = table_iterator.next()
                    n_table_rows += 1
                    table_key = table_key_function( table_row )
                    succeed = False
                while table_key > specimen_key:
                    log_stream.write( 'Row {} in specimen but not in output\n'.format( specimen_key ) )
                    specimen_row = specimen_iterator.next()
                    specimen_key = specimen_key_function( specimen_row )
                    n_specimen_rows += 1
                    succeed = False
                while table_key == specimen_key:
                    n_compared += 1
                    if not compare_rows( table_row, specimen_row, table_key, log_stream, columns ):
                        succeed = False
                        n_mismatched_data += 1
                    table_row = table_iterator.next()
                    n_table_rows += 1
                    table_key = table_key_function( table_row )
                    specimen_row = specimen_iterator.next()
                    n_specimen_rows += 1
                    specimen_key = specimen_key_function( specimen_row )
                    
        except StopIteration:
            pass
        
        # Consume any remaining records from table
        # Need to rework the iterator logic: it drops a count of one mismatched row on one side
        try:
            while True:
                table_row = table_iterator.next()
                table_key = table_key_function( table_row )
                log_stream.write( 'Row {} in output but not in specimen\n'.format( table_key ) )
                n_table_rows += 1
                succeed = False
        except StopIteration:
            pass
        
        # Consume any remaining records from specimen
        try:
            while True:
                specimen_row = specimen_iterator.next()
                specimen_key = specimen_key_function( specimen_row )
                log_stream.write( 'Row {} in specimen but not in output\n'.format( specimen_key ) )
                n_specimen_rows += 1
                succeed = False
        except StopIteration:
            pass
        
        if succeed:
            log_stream.write( 'Output tables {} and {} exactly matched\n'.format( table, specimen_name ) )
        else:
            log_stream.write( 'Difference found between output tables {} and {}\n'.format( table, specimen_name ) )
            if n_specimen_rows != n_table_rows:
                log_stream.write( 'Length of tables mismatched: output has {} rows, specimen has {} rows\n'.
                                  format( n_table_rows, n_specimen_rows ) )
            log_stream.write( '{} rows in output but not in specimen.\n'.format( n_table_rows - n_compared ) )
            log_stream.write( '{} rows in specimen but not in output.\n'.format( n_specimen_rows - n_compared ) )
            log_stream.write( '{} rows in specimen and output had mismatched data.\n'.format( n_mismatched_data ) )

    finally:
        log_stream.close()
    return succeed
        
        
def compare_rows( table_row, specimen_row, row_id, log_stream, columns ):
    succeed = True
    for table_col, specimen_col, tester in columns:
        if tester is None:
            tester = lambda x, y: x == y
        x = getattr( table_row, table_col )
        y = specimen_row[specimen_col]
        if not tester( x, y ):
            succeed = False
            log_stream.write( 'Row {} has mismatched values. {}={}, {}={}\n'.format(
                    row_id, table_col, x, specimen_col, y ) )
    return succeed

