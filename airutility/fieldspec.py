'''
Created on May 31, 2013

@author: temp_dmenes
'''

from formatutilities import db_identifier_quote
import copy

_CHARISH_TYPES = [ 'CHAR', 'VARCHAR', 'TEXT', 'NCHAR', 'NVARCHAR', 'NTEXT',
                  'BINARY', 'VARBINARY', 'IMAGE' ]

_PRECISION_TYPES = [ 'NUMERIC', 'DECIMAL', 'DEC' ]


class FieldSpec( object ):
    '''Describes a database column
    
    This class provides a convenient carrier for metadata describing a column
    in a database table or view. In addition to providing information about the
    data, it has formatting methods to make it easier to incorporate the
    field into a SQL query. 
    
    .. todo::

        Omits many things from field definition, including default values,
        collation, identity columns etc.
        
    Format Support
    --------------
    This class supports special format specifiers when used as an argument to
    the Python formatter (`"".format( X )`). These specifiers are most useful
    when a list of :ref:`FieldSpec` objects is wrapped in a :ref:`Joiner`
    object.
    
    They are:
    
    `{X}` :
        Without a specifier, this will format as the unadorned field name 
        (`field_name` property)
        
    `{X:qualified}` :
        The `qualified` specifier returns the `field_name` qualified with the
        name of the table. If the table has an alias, the table's alias name
        will be used; otherwise the table's name will be used.
    
    `{X:with_as}` :
        If the alias property is set, this returns the same as `{X} AS {X.alias}`,
        otherwise this returns the same as `{X}`
    
    `{X:alias_or_name}` :
        If the alias property is set, this returns the same as `{X.alias}`,
        otherwise this returns the same as `{x}`
    
    `{X:definition}` :
        Returns a column definition expression suitable for use in SQL `CREATE
        TABLE` or `ALTER TABLE` statements.
        
    
    

    Attributes
    ----------
    field_name : (str)
        Column name. The value that this returns will always be normalized
        and wrapped in square brackets.
        
        .. caution:: Do not try to assign a qualified name to this attribute
           (as in `[dbo].[my_table]`). This may work in the future,
           but for the time being the code is not smart enough to notice the
           qualification, so various methods will return a doubly-qualified
           name (`[dbo].[dbo].[my_table]`), which is unhelpful
           
    alias : (str)
        An alternative name that will be used in some cases when constructing
        queries.
        
    alias_or_name : (str) READ ONLY
        Returns the alias if it has been set; otherwise the field_name. Also
        available as a format specifier.
        
    qualified_name : (str) READ ONLY
        Returns the field_name, prefixed with the (possibly aliased) name of
        the table. Also available as a format specifier.
        
    name_with_as : (str) READ ONLY
        the field_name followed by the word 'AS' and the alias. If no alias
        has been set, then this simply returns field_name. Also available as
        a format specifier.
        
    definition : (str) READ ONLY
        A SQL column definition expression that would create this column, suitable
        for use in `CREATE TABLE` or `ALTER TABLE` statements.
           
    table : (TableSpec)
        :ref:`TableSpec` object describing the table of which this field is
        a column. Ordinarily you should not write to this field, as it will
        be populated automatically when the field is added to a table.
        
    basic_type : (str)
        The name of the SQL data type, excluding any length or precision
        information. In other words, if the actual type is `VARCHAR(255)`,
        this property will be VARCHAR.
        
        Value will always be normalized to uppercase
        
    data_length : (int)
        Length for character-ish datatypes.
        
    precision : (int)
        The number of significant digits in the prescribed radix. Not
        very useful for binary data types, but may be used to specify a
        precision for decimal data types (e.g., the `15` in `DECIMAL(15,2)`)
        
    scale : (int)
        The number of digits after the radix point. Not
        very useful for binary data types, but may be used to specify a
        precision for decimal data types (e.g., the `2` in `DECIMAL(15,2)`)
        
    radix : (int)
        The base of the number system used for the internal representation
        of numeric data types. Generally 2 or 10.
        
    data_type : (str) READ ONLY
        Returns the full SQL data type expression, includint the part in
        parentheses (e.g., `VARCHAR(255)`).
        
        Presently this is read-only. Someday we may write a parser smart
        enough that this can accept a SQL data type expression and set the
        basic_type, data_length, precision, scale and radix properties
        appropriately.
        
    is_charish : (bool) READ ONLY
        Whether or not this is one of the data types that, like `CHAR`
        or `VARCHAR`, takes a length in parentheses.
        
    default_value : (str)
        What the server reports as the default value for this type. We do 
        nothing with this information.
        
    nullable : (str)
        Whether or not the server reports this field as nullable. We do
        nothing with this information.
        
    ordinal_position : (int or None)
        The position that this column occupies in the table. When you add
        a column to a TableDesc, the TableDesc will respect this value if
        it is not None. This could be useful for assembling a table whose
        fields should come in a particular order. If this is None, then
        TableDesc will append the new column to the end of its column
        list (and fill in the ordinal_position appropriately).
        
    n_bytes : (int)
        The number of bytes occupied in a row by a field of this type
        
    Methods
    -------
    
    .. automethod:: clone
    
    
        
    
    '''
    def __init__( self, field_name="", basic_type="", data_length=None, precision=None,
                  scale=None, radix=None, default_value=None, nullable=True,
                  ordinal_position=None, alias=None, identity=None ):
        super( FieldSpec, self ).__init__()
        self.field_name = field_name
        self.table = None
        self.basic_type = basic_type
        self.data_length = data_length
        self.precision = precision
        self.scale = scale
        self.radix = radix
        self.default_value = default_value
        self.nullable = nullable
        self.ordinal_position = ordinal_position
        self.alias = alias
        self.identity = identity

    def populate_from_information_schema( self, row ):
        self.field_name = row.column_name
        self.basic_type = row.data_type
        self.data_length = row.character_maximum_length
        self.precision = row.numeric_precision
        self.scale = row.numeric_scale
        self.radix = row.numeric_precision_radix
        self.default_value = row.column_default
        self.nullable = row.is_nullable
        self.ordinal_position = row.ordinal_position

    def setIdentity( self, seed=None, increment=None ):
        if seed == None and increment == None:
            self.identity = None
        else:
            self.identity = ( seed, increment )

    @property
    def field_name( self ):
        return self._field_name

    @field_name.setter
    def field_name( self, value ):
        self._field_name = db_identifier_quote( value )

    @property
    def alias( self ):
        return self._alias

    @alias.setter
    def alias( self, value ):
        self._alias = db_identifier_quote( value )

    @property
    def qualified_name( self ):
        return '.'.join( ( self.table.alias_or_name, self.field_name ) )

    @property
    def name_with_as( self ):
        if self.alias is None:
            return self.qualified_name
        else:
            return self.qualified_name + ' AS ' + self.alias

    @property
    def alias_or_name( self ):
        if self.alias is None:
            return self.field_name
        else:
            return self.alias

    @property
    def basic_type( self ):
        return self._basicType

    @basic_type.setter
    def basic_type( self, value ):
        self._basicType = value.upper()

    @property
    def data_type( self ):
        if self.is_charish:
            if self.data_length != None:
                return "{}({:d})".format( self.basic_type, int( self.data_length ) )
            else:
                return self.basic_type
        elif self.basic_type in _PRECISION_TYPES:
            if self.precision is None:
                return self.basic_type
            elif self.scale is None:
                return "{}({})".format( self.basic_type, self.precision )
            else:
                return "{}({},{})".format( self.basic_type, self.precision, self.scale )
        return self.basic_type

    @property
    def definition( self ):
        phrases = [ self.field_name, self.data_type ]
        if self.default_value != None:
            pass
        if not self.nullable:
            phrases.append( "NOT NULL" )
        if self.identity:
            phrases.append( "IDENTITY({0},{1})".format( *self.identity ) )
        return " ".join( phrases )

    @property
    def is_charish( self ):
        return self.basic_type in _CHARISH_TYPES
    
    @property
    def n_bytes( self ):
        if self.basic_type in ( 'CHAR', 'VARCHAR', 'TEXT', 'BINARY', 'VARBINARY' ):
            return self.data_length
        elif self.basic_type in ( 'NCHAR', 'NVARCHAR', 'NTEXT' ):
            return self.data_length * 2
        elif self.basic_type in ( 'FLOAT', 'BIGINT' ):
            return 8
        elif self.basic_type in ( 'REAL', 'INT' ):
            return 4
        elif self.basic_type == 'SMALLINT':
            return 2
        elif self.basic_type == 'TINYINT':
            return 1
        elif self.basic_type in _PRECISION_TYPES:
            if self.precision <= 9:
                return 5
            elif self.precision <= 19:
                return 9
            elif self.precision <= 28:
                return 13
            elif self.precision <= 38:
                return 17
            else:
                raise ValueError( "{} is not a valid precision for type {}".format( self.precision, self.basic_type ) )
        else:
            raise ValueError( "Don't know the size of type {}".format( self.basic_type ) )
        
    
    def clone(self):
        """A copy of the field data that is not attached to a table
        """
        clone = copy.copy(self)
        clone.table = None
        clone.alias = None
        clone.ordinal_position = None
        return clone

    def __format__( self, spec ):
        if spec == "":
            return self.field_name
        if spec == "qualified":
            return self.qualified_name
        if spec == "with_as":
            return self.name_with_as
        if spec == "alias_or_name":
            return self.alias_or_name
        if spec == "definition":
            return self.definition
        raise ValueError( "Invalid format specification: " + spec )

