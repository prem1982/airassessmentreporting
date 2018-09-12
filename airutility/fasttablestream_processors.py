'''
Created on Jun 25, 2013

@author: temp_dmenes
'''

import struct
from numbers import Integral


PROCESSOR_FACTORIES = {}

class NVarcharProcessor( object ):
    def __init__( self, index, field, raw ):
        if field.ordinal_position is None:
            raise ValueError( "The field {} doesn't seem to have an ordinal position. Maybe it isn't in a table yet?".format( field ) )
        if field.basic_type not in ( "NVARCHAR", "VARCHAR" ):
            raise ValueError( "NVarcharProcessor doesn't know what to do with field type {} on field {}".format( field.basic_type, field ) )
        if ( not isinstance( field.data_length, Integral ) ) or field.data_length < 1:
            raise ValueError( "Field {} does not have a valid length specified".format( field ) )
        self.field = field
        data_length = field.data_length * 2
        if data_length < 32768:
            prefix_length = 2
            struct_pack = struct.Struct( 'h{}s'.format( data_length ) ).pack_into
            def none_pack( b, p ):
                b[p:p+2]=b'\xff\xff'
        else:
            prefix_length = 4
            struct_pack = struct.Struct( 'i{}s'.format( data_length ) ).pack_into
            def none_pack( b, p ):
                b[p:p+4]=b'\xff\xff\xff\xff'
        
        self.prefix_length = prefix_length
        self.n_bytes = prefix_length + data_length
        
        if raw:
            if field.nullable:
                def process( row, buffer_, ptr ):
                    val = row[ index ]
                    if val is None:
                        none_pack( buffer_, ptr )
                        return ptr + prefix_length
                    val = val.encode( 'UTF-16LE' )
                    n = len( val )
                    struct_pack( buffer_, ptr, n, val )
                    return ptr + n + prefix_length
            else:
                def process( row, buffer_, ptr ):
                    v = row[ index ].encode( 'UTF-16LE' )
                    n = len( v )
                    struct_pack( buffer_, ptr, n, v )
                    return ptr + n + prefix_length
        else:
            def process( row, buffer_, ptr ):
                val = row[ index ]
                if val is None:
                    none_pack( buffer_, ptr )
                    return ptr + prefix_length
                if isinstance( val, str ):
                    val = val.decode( 'UTF-8' )
                val = unicode( val ).encode( 'UTF-16LE' )
                n = len( val )
                if n > data_length:
                    raise ValueError( "Value too long to fit in NVARCHAR({})field".format( data_length // 2 ) )
                struct_pack( buffer_, ptr, n, val )
                return ptr + n + prefix_length
        self.process = process

PROCESSOR_FACTORIES[ 'NVARCHAR' ] = NVarcharProcessor
PROCESSOR_FACTORIES[ 'VARCHAR' ] = NVarcharProcessor

class IntegerProcessor( object ):
    
    # Key values for defining each integer type
    # Number of bytes, minimum value, max value, BCP type code, and struct type code
    specs = {
        'TINYINT' :  ( 1,                   0,                256,  'SQLTINYINT', 'B' ),
        'SMALLINT' : ( 2,              -32768,              32767, 'SQLSMALLINT', 'h' ),
        'INT' :      ( 4,         -0x80000000,         0x7fffffff,      'SQLINT', 'i' ),
        'BIGINT' :   ( 8, -0x8000000000000000, 0x7fffffffffffffff,   'SQLBIGINT', 'q' ),
    }
    
    def __init__( self, index, field, raw ):
        if field.ordinal_position is None:
            raise ValueError( "The field {} doesn't seem to have an ordinal position. Maybe it isn't in a table yet?".format( field ) )
        if field.basic_type not in IntegerProcessor.specs:
            raise ValueError( "IntegerProcessor doesn't know what to do with field type {} on field {}".format( field.basic_type, field ) )
        self.field = field
        data_length, min_, max_, bcp_col_type, struct_type = IntegerProcessor.specs[ field.basic_type ]
        self.bcp_col_type = bcp_col_type
        if field.nullable:
            n_bytes = 1 + data_length
            self.n_bytes = n_bytes
            struct_pack = struct.Struct( '<b{}'.format( struct_type ) ).pack_into
            def none_pack( b, p ):
                b[p]=b'\xff'
            if raw:
                def process( row, buffer_, ptr ):
                    val = row[ index ]
                    if val is None:
                        none_pack( buffer_, ptr )
                        return ptr + 1
                    struct_pack( buffer_, ptr, data_length, val )
                    return ptr + n_bytes
            else:
                def process( row, buffer_, ptr ):
                    val = row[ index ]
                    if val is None:
                        none_pack( buffer_, ptr )
                        return ptr + 1
                    val = int( val )
                    if val < min_ or val > max_:
                        raise ValueError( "Value {} out of range for column {} of type {}".format( val, field, field.basic_type ) )
                    struct_pack( buffer_, ptr, data_length, val )
                    return ptr + n_bytes
        else: # field is not nullable
            if raw:
                def process( row, buffer_, ptr ):
                    struct_pack( buffer_, ptr, data_length, row[ index ] )
                    return ptr + n_bytes
            else:
                def process( row, buffer_, ptr ):
                    val = int( row[ index ] )
                    if val < min_ or val > max_:
                        raise ValueError( "Value {} out of range for column {} of type {}".format( val, field, field.basic_type ) )
                    struct_pack( buffer_, ptr, data_length, val )
                    return ptr + n_bytes
        self.process = process

PROCESSOR_FACTORIES[ 'TINYINT' ] = IntegerProcessor
PROCESSOR_FACTORIES[ 'SMALLINT' ] = IntegerProcessor
PROCESSOR_FACTORIES[ 'INT' ] = IntegerProcessor
PROCESSOR_FACTORIES[ 'BIGINT' ] = IntegerProcessor


class FloatProcessor( object ):
    
    def __init__( self, index, field, raw ):
        if field.ordinal_position is None:
            raise ValueError( "The field {} doesn't seem to have an ordinal position. Maybe it isn't in a table yet?".format( field ) )
        if field.basic_type != "FLOAT":
            raise ValueError( "FloatProcessor doesn't know what to do with field type {} on field {}".format( field.basic_type, field ) )
        self.field = field
        if field.nullable:
            self.n_bytes = 9
            struct_pack = struct.Struct( '<bd' ).pack_into
            def none_pack( b, p ):
                b[p]=b'\xff'
            if raw:
                def process( row, buffer_, ptr ):
                    val = row[ index ]
                    if val is None:
                        none_pack( buffer_, ptr )
                        return ptr + 1
                    struct_pack( buffer_, ptr, 8, val )
                    return ptr + 9
            else:
                def process( row, buffer_, ptr ):
                        val = row[ index ]
                        if val is None:
                            none_pack( buffer_, ptr )
                            return ptr + 1
                        struct_pack( buffer_, ptr, 8, float( val ) )
                        return ptr + 9
        else: # field is not nullable
            self.n_bytes = 8
            struct_pack = struct.Struct( '<d' ).pack_into
            if raw:
                def process( row, buffer_, ptr ):
                    struct_pack( buffer_, ptr, row[ index ] )
                    return ptr + 8
            else:
                def process( row, buffer_, ptr ):
                    struct_pack( buffer_, ptr, float( row[ index ] ) )
                    return ptr + 8
        self.process = process

PROCESSOR_FACTORIES[ 'FLOAT' ] = FloatProcessor
