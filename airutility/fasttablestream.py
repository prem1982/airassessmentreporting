'''
Created on Jun 25, 2013

@author: temp_dmenes
'''

import os

import dbutilities
import fasttablestream_processors
import fasttablestream_server

__all__ = [ 'FastTableStream', 'DEFAULT_BUFFER_SIZE' ]

DEFAULT_BUFFER_SIZE = 1<<24

_BULK_INSERT_QUERY = """
    BULK INSERT {table} FROM '{data_pipe}' WITH (
        TABLOCK,
        ROWS_PER_BATCH={l},
        DATAFILETYPE='widenative',
        KEEPNULLS
)
"""

class FastTableStream( object ):
    '''An object that uses the database's bulk insert feature to quickly write large amounts of data
    '''
    
    def __init__( self, table, db_context=None, schema=None, buffer_size = 120000, use_names = True, raw=False, dumpfile=None ):
        self.table = table
        self.db_context = db_context
        self.schema = schema
        self.buffer_size = buffer_size
        self.use_names = use_names
        self.raw = raw
        self.is_open = False
        self.dumpfile = dumpfile
        
    def open( self ):
        self.validate_write_inputs()
        self._setup_write()
        self.is_open = True
        
    def write( self, row ):
        p = self.ptr_buffer
        b = self.buffer
        for proc in self.process_funs:
            p = proc( row, b, p )
        self.ptr_buffer = p
        self.i_batch += 1
        self.i_row += 1
        if self.i_batch == self.n_buffer_rows:
            self.flush()
            
    def write_many( self, rows ):
        for row in rows:
            self.write(row)
        
    def close( self ):
        if not self.is_open:
            raise ValueError( "Fast table stream closed when it wasn't open" )
        if self.i_batch > 0:
            self.flush()
        self.is_open = False
        
    def flush(self):
        self.logger.debug( "Sending rows {} to {} to database".format( self.i_row - self.i_batch + 1, self.i_row ) )
        writer = fasttablestream_server.WriterContext( self.run_context, self.buffer, self.ptr_buffer, self.dumpfile )
        
        query = _BULK_INSERT_QUERY.format(
                data_pipe=writer.data_pipe_name,
                table=self.table,
                l=self.i_batch )
        with writer:
            self.db_context.executeNoResults( query )
        self.i_batch = 0
        self.ptr_buffer = 0
        
    def validate_write_inputs( self ):
        self.table = dbutilities.get_table_spec( self.table, self.db_context, self.schema )
        self.db_context = self.table.db_context
        self.run_context = self.db_context.runContext
        self.logger = self.run_context.get_logger( "fasttablestream" )
        
        # If the table exists, make sure that its structure is consistent with the TableSpec
        if dbutilities.table_exists( self.table ):
            if len( self.table ) == 0:
                self.table.populate_from_connection()
                for field in self.table:
                    field.db_position = field.ordinal_position
            else:
                other_table = self.db_context.getTableSpec( self.table )
                if len( other_table ) != len( self.table ):
                    raise ValueError( "Table in database has different column count than table as specified" )
                for field in self.table:
                    if field not in other_table:
                        raise ValueError( "Column {} not found in table in database".format( field ) )
                    other_field = other_table[ field ]
                    if other_field.basic_type != field.basic_type:
                        raise ValueError( "Type {} specified for field {} differs from type {} in database".format(
                                field.basic_type, field, other_field.basic_type ) )
                    if field.is_charish and other_field.data_length < field.data_length:
                        raise ValueError( "Data length {} specified for field {} longer than length {} in database".format(
                                field.data_length, field, other_field.data_length ) )
                    field.db_position = other_field.ordinal_position
        else:
            if len( self.table ) == 0:
                raise ValueError( "Creating new table {}, but no columns have been specified".format( self.table ) )
            for field in self.table:
                field.db_position = field.ordinal_position
            self.db_context.executeNoResults( self.table.definition )
        
    def _setup_write( self ):
        
        # Create an array of processors for each column
        if self.use_names:
            for field in self.table:
                if not hasattr( field, 'processor' ):
                    fac = fasttablestream_processors.PROCESSOR_FACTORIES[ field.basic_type ]
                    field.processor = fac( field.name, field, self.raw )
        else:
            i = 0
            for field in self.table:
                if not hasattr( field, 'processor' ):
                    fac = fasttablestream_processors.PROCESSOR_FACTORIES[ field.basic_type ]
                    field.processor = fac( i, field, self.raw )
                i += 1
        self.process_funs = [ field.processor.process for field in self.table ]
        
        # Set up the buffer memory
        bytes_per_row = 0
        for field in self.table:
            bytes_per_row += field.processor.n_bytes
        if bytes_per_row > self.buffer_size:
            self.real_buffer_size = bytes_per_row
            self.n_buffer_rows = 1
        else:
            self.n_buffer_rows = self.buffer_size // bytes_per_row
            self.real_buffer_size = self.n_buffer_rows * bytes_per_row
        self.buffer = bytearray( self.real_buffer_size )
        self.ptr_buffer = 0    
        self.i_batch = 0
        self.i_row = 0
        
        # Make sure the table exists
        if not dbutilities.table_exists( self.table ):
            self.db_context.executeNoResults( self.table.definition )
            
        # Drop the dump file
        if self.dumpfile is not None and os.path.isfile( self.dumpfile ):
            os.remove( self.dumpfile )
        
    def __enter__(self):
        self.open()
        
    def __exit__( self, exc_type, exc_value, tb ):
        if exc_type is None:
            self.close()
        else:
            self.is_open = False;
        
