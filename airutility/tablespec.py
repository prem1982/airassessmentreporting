'''
Created on Apr 29, 2013

@author: temp_dmenes
'''

import copy
import re

from formatutilities import db_identifier_quote, db_identifier_unquote, Joiner
from fieldspec import FieldSpec
from dictlist import DictList
"""
"""

__all__ = [ 'TableSpec' ]

_GET_TABLE_SPEC_QUERY = """
    SELECT column_name, data_type, character_maximum_length,
        numeric_precision, numeric_scale, numeric_precision_radix,
        ordinal_position, column_default, is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = ? AND table_schema=?
    ORDER BY ordinal_position
"""

_GET_PRIMARY_KEY_QUERY = """
    SELECT column_name, ordinal_position
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A
    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B
    ON A.constraint_name=B.constraint_name
        AND A.constraint_schema=B.constraint_schema
        AND A.constraint_catalog=B.constraint_catalog
        AND A.table_name=B.table_name
        AND A.table_schema=B.table_schema
        AND A.table_catalog=B.table_catalog
    WHERE B.constraint_type='PRIMARY KEY'
        AND A.table_name=?
        AND A.table_schema=?
    ORDER BY ordinal_position
"""

_FOREIGN_KEY_PATTERN = "FOREIGN KEY({columns:','}) REFERENCES {foreign_table:qualified}({columns: ',', itemfmt='{{:foreign}}'})"
_PRIMARY_KEY_PATTERN = "PRIMARY KEY({columns:','})"
_CREATE_TABLE_PATTERN = "CREATE TABLE {table:qualified}({phrases:delimiter=',',item='X',itemfmt='{{X:definition}}'})"

_WHITESPACE_PATTERN = re.compile(r'\s+')

class TableSpec( DictList ):
    '''
    Describes a table in a database
    
    This class provides a convenient carrier for metadata describing a
    database table or view. In addition to providing information about the
    data, it has formatting methods to make it easier to incorporate the
    field into a SQL query.
    
    This object can be used to read information about existing tables,
    and to set up a new table for creation.
    
    This object behaves as a collection storing FieldSpec objects. When
    referencing objects, you may index either by name or by ordinal
    position. When iterating through the collection, the field specs
    will be returned in the correct order.
    

    Many of the attributes are database identifiers. Generally this
    class will return those attributes in a form that is safely
    quoted for use in a SQL query (i.e., wrapped in [])

    .. todo::
        Does not populate foreign_keys table in populate_from_connection
        
    .. todo::
        Does not configure field's identity property in populate_from_connection
        
    .. py:attribute:: table_name
      
      :class:`str` (read-write). The name of the table (automatically normalized)
      
    .. py:attribute:: table_schema
      
      :class:`str` (read-write). The name of the database schema in which this table is located 
      (automatically normalized). If omitted, the :attr:`DBContext.schema` property of the
      :attr:`db_context` will be used 
      
    .. attribute:: db_context
    
      :class:`DBContext` (read/write). The database context that was specified when this object was created
    
    .. attribute:: alias
    
      :class:`str` (read-write). An alias by which this table is being referenced in a query. This
      is used when constructing complex queries
      
    .. attribute:: name_with_as
    
      :class:`str` (read-only). The string :samp:`"{name} AS {alias}"`
      
    .. attribute:: alias_or_name
    
      :class:`str` (read-only). If an alias is set, return that, otherwise return the name
      
    .. attribute:: qualifed_name
    
      :class:`str` (read-only). The table name, qualified with the schema name

    .. attribute:: field_spec_class
    
      :class:`Class` (read/write). Sometimes it is useful to create
      field specs that have special formatting methods or other special
      proeprties. For this purpose, an alternative class can be specified
      which will be used whenever the object creates new field specs. 
      This property is currently only used by the :meth:`populate_from_connection` method
    
    .. attribute:: primary_key
    
      :class:`PrimaryKey` (read/write). An ordered list of fields that constitute the table's primary key.
      The :class:`PrimaryKey` class has special formatting methods that help to format the table
      definition, but otherwise is just a :class:`list` object
      
    .. attribute:: foreign_key
    
      :class:`list` of :class:`ForeignKey` objects (read/write). A list of foreign key constraints on this
      table. Each foreign key object must be of class :class:`ForeignKey`. This is a modified :class:`List`
      similar to :class:`PrimaryKey`. Existing foreign key constraints will be read when the table metadata
      is read (see :meth:`populate_from_connection`). The easiest way to create a new foreign key constraint
      is using :meth:`create_foreign_key`.
      
    .. attribute:: definition
    
      :class:`str` (read-only). A `CREATE TABLE` statement that would create the table described by this object.
      
    .. attribute:: bytes_in_row
    
      :class:`str` (read-only). An estimate of the number of bytes required to store each row of data. This is calculated from the
      data types of the stored fields.
    '''

    def __init__( self, db_context, table_name, table_schema=None,
                  field_spec_class=FieldSpec ):
        """
        :arg db_context: The DBContext object identifying the database where this table may be found or should be created.
            
        :arg table_name: Name for table. Do NOT provide a qualified name (no dots).
            
        :arg table_schema: Schema name that should be used to qualify the table name. If None, then the schema name associated with
            the db_context will be used. Defaults to None
            
        :arg field_spec_class: Class used to create field_spec members. Defaults to :class:`FieldSpec`
        """
        super( TableSpec, self ).__init__( )
        self.table_name = table_name
        self.table_schema = table_schema
        self.db_context = db_context
        self.field_spec_class = field_spec_class
        self.primary_key = PrimaryKey( self )
        self.foreign_keys = []
        self.alias = None

    def populate_from_connection( self ):
        """Record the layout of an existing table in a database
        """
        self.clear()

        for row in self.db_context.execute( _GET_TABLE_SPEC_QUERY, (
                db_identifier_unquote( self.table_name ),
                db_identifier_unquote( self.table_schema ) ) ):
            field_spec = self.field_spec_class()
            field_spec.populate_from_information_schema( row )
            self.add( field_spec )

        for row in self.db_context.execute( _GET_PRIMARY_KEY_QUERY, (
                db_identifier_unquote( self.table_name ),
                db_identifier_unquote( self.table_schema ) ) ):
            self.primary_key.append( self[ row.column_name ] )
            
    def clear( self ):
        super( TableSpec, self ).clear()
        del self.primary_key[:]
        self.foreign_keys = []

    @property
    def alias( self ):
        return self._alias

    @alias.setter
    def alias( self, value ):
        self._alias = db_identifier_quote( value )

    @property
    def table_name( self ):
        return self._table_name

    @table_name.setter
    def table_name( self, value ):
        if isinstance( value, TableSpec ):
            self._table_name = value.table_name
        else:
            self._table_name = db_identifier_quote( value )

    @property
    def table_schema( self ):
        if self._table_schema is not None:
            return self._table_schema
        return self.db_context.schema

    @table_schema.setter
    def table_schema( self, value ):
        self._table_schema = db_identifier_quote( value )

    @property
    def name_with_as( self ):
        if self.alias is None:
            return self.qualified_name
        return self.qualified_name + ' AS ' + self.alias

    @property
    def alias_or_name( self ):
        if self.alias is None:
            return self.qualified_name
        return self.alias

    @property
    def qualified_name( self ):
        if self.table_schema is not None:
            return ".".join( ( self.db_context.db_name, self.table_schema, self.table_name ) )
        return self.table_name

    @property
    def definition( self ):
        if len( self.primary_key ) > 0:
            phrases = Joiner( self, ( self.primary_key, ), self.foreign_keys )
        else:
            phrases = Joiner( self, self.foreign_keys )
        return _CREATE_TABLE_PATTERN.format( table=self,
                                             phrases=phrases )
    @property
    def bytes_in_row( self ):
        return sum( [ field.n_bytes for field in self ] )
    
    def add( self, field_spec ):
        '''Add a :class:`FieldSpec` to the table definition.
        
        You cannot add the same :class:`FieldSpec` to two different :class:`TableSpec`\ s, or to one
        :class:`TableSpec` twice. If you want to copy a field specification from another table, use
        the :meth:`clone` method.
        
        :param field_spec: A :class:`FieldSpec` to add
        
        :returns: :samp:`self` (supports "fluent" style calls)
        '''
        n = field_spec.ordinal_position
        if n is not None:
            i = len( self )
            while( i < n ):
                placeholder = FieldSpec( 'placeholder_{}'.format( i ) )
                self.append( placeholder )
                i += 1
            self[ n - 1 ] = field_spec
        else:
            self.append( field_spec )
        return self
    
    def add_all( self, field_specs ):
        '''Add all :class:`FieldSpec`\ s from a sequence to the table definition.
        
        A subtle difference between this method and :meth:`add` is that this method makes a clone of each :class:`FieldSpec`.
        Therefore you can use :meth:`add_all` on a list of :class:`FieldSpec`\ s that are already found in another
        :class:`TableSpec`. The easiest way to make a copy of a table's structure is::
        
            >>> existing_table = db_context.getTableSpec( 'existing_table' )
            >>> new_table = get_table_spec( 'new_table', db_context )
            >>> new_table.add_all( existing_table )
        
        :param field_specs: An iterable containing :class:`FieldSpec` to add
        
        :returns: :samp:`self` (supports "fluent" style calls)
        '''
        for field_spec in field_specs:
            self.add( field_spec.clone() )
        return self

    def create_foreign_key( self, foreign_table, new_names=False, base_name=None ):
        '''Create a foreign key constraint referencing an existing table.
        
        The foreign table must have a primary key defined.
        
        :arg foreign_table: A :class:`TableSpec`, which must define a primary key

        :arg new_names: A :class:`bool`. If ``False``, keep the column names used in the foreign table's primary key. If ``True``,
            use new names based on ``base_name``. Optional, defaults to ``False``

        :arg base_name: A :class:`str`. An integer will be appended to this string to form the names for the foreign key fields
            if ``new_names`` is ``True``. Required if ``new_names`` is ``True``, ignored if ``new_names`` is ``False``
        
        :returns: :samp:`self` (supports "fluent" style calls)
        '''
        i = 1

        new_key = ForeignKey( self, foreign_table )
        self.foreign_keys.append( new_key )
        for key_col in foreign_table.primary_key:
            new_key_col = ForeignKeyFieldSpec( copy.copy( key_col ) )
            if new_names:
                new_key_col.field_name = base_name + str( i )
                i += 1
            else:
                new_key_col.field_name = key_col.field_name
            new_key_col.basic_type = key_col.basic_type
            new_key_col.data_length = key_col.data_length
            new_key_col.precision = key_col.precision
            new_key_col.scale = key_col.scale
            new_key_col.radix = key_col.radix
            new_key_col.default_value = None
            new_key_col.nullable = True
            new_key_col.alias = None

            self.add( new_key_col )
            new_key.append( new_key_col )
        return self


    def __format__( self, spec ):
        if spec == "":
            return self.table_name
        elif spec == "qualified":
            return self.qualified_name
        elif spec == "alias_or_name":
            return self.alias_or_name
        elif spec == "definition":
            return self.definition
        if spec == "with_as":
            return self.name_with_as
        raise ValueError( "Invalid format specification: " + spec )
    
    @staticmethod
    def _set_ordinal( field_spec, i ):
        field_spec.ordinal_position = i+1

    @staticmethod
    def _get_name( field_spec ):
        return field_spec.field_name
    
    @staticmethod
    def _normalize_name( name ):
        if isinstance( name, FieldSpec ):
            return name.field_name
        elif isinstance( name, ( str, unicode ) ):
            return db_identifier_quote ( name )
        else:
            raise ValueError( "Don't know how to index using a {}".format( name.__class__ ) )
    
    @staticmethod
    def _normalize_property( name ):
        return _WHITESPACE_PATTERN.sub( '_', db_identifier_unquote( name ) )
    
    def _connect( self, field_spec ):
        if field_spec.table is not None:
            raise ValueError( "Tried to add a FieldSpec object that has already been added to a TableSpec" )
        field_spec.table = self
    
    def _disconnect( self, field_spec ):
        field_spec.table = None
        field_spec.ordinal_position = None
        

class ForeignKeyFieldSpec( FieldSpec ):
    def __init__( self, foreign_field=None ):
        self.foreign_field = foreign_field
        super( ForeignKeyFieldSpec, self ).__init__()

    @FieldSpec.field_name.setter
    def field_name( self, value ):
        if self.foreign_field is not None:
            self.foreign_field.alias = value
        # Ignore the error in Eclipse: the fset method does exist.
        FieldSpec.field_name.fset( self, value )

    @property
    def foreign_name( self ):
        return self.foreign_field.field_name

    @property
    def foreign_with_as( self ):
        return self.foreign_field.withAs

    @property
    def foreign_qualified_name( self ):
        return self.foreign_field.qualified_name

    def __format__( self, spec ):
        if spec == "foreign":
            return self.foreign_name
        elif spec == "foreign_with_as":
            return self.foreign_with_as
        elif spec == "foreign_qualified":
            return self.foreign_qualified_name
        return super( ForeignKeyFieldSpec, self ).__format__( spec )

class ForeignKey( list ):
    def __init__( self, table, foreign_table ):
        self.table = table
        self.foreign_table = foreign_table


    @property
    def definition( self ):
        return _FOREIGN_KEY_PATTERN.format( columns=Joiner( self ), foreign_table = self.foreign_table )

    def __format__( self, spec ):
        if spec == "definition":
            return self.definition
        elif spec == "":
            return str( self )
        raise ValueError( "Invalid format specification: " + spec )


class PrimaryKey( list ):
    def __init__( self, table ):
        self.table = table

    @property
    def definition( self ):
        return _PRIMARY_KEY_PATTERN.format( columns=Joiner( self ) )
     
    def __format__( self, spec ):
        if spec == "definition":
            return self.definition
        elif spec == "":
            return str( self )
        raise ValueError( "Invalid format specification: " + spec )

