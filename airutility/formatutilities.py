'''
Created on Apr 29, 2013

@author: temp_dmenes
'''
import os.path

from arglist import parse_arg_list

def yesno( b ):
    '''"YES" if b is True, else "NO"
    ''' 
    return "YES" if b else "NO"

def yn( b ):
    '''"Y" if b is True, else "N"
    ''' 
    return "Y" if b else "N"

def db_identifier_quote( name ):
    '''The provided name as a quoted SQL Server identifier
    
    The name will be reduced to all-lowercase and wrapped with square brackets.  If the name is already wrapped in
    square brackets, :func:`db_identifier_quote` will do the sensible thing and avoid adding a second set.
    
    If the provided name is unicode, it will be encoded into us-ascii. This step suppresses an issue with pyodbc which
    fails when it is provided with unicode identifiers.
    
    .. note::
    
      This function WILL NOT correctly interpret a qualified identifier (multiple parts separated by periods). Any period
      characters in the name will simply be wrapped inside the square brackets and treated as part of the basic
      identifier. DO NOT include database or schema names (when identifying tables) or database, schema or table names
      (when identifying columns).
    '''
    name = db_identifier_unquote( name )
    if name is None:
        return None
    if "]" in name:
        raise ValueError( "SQL Server identifiers may not contain ']'" )
    
    return '[' + name + ']'

def db_identifier_unquote( name ):
    '''Remove quoting brackets from a SQL Server identifier.
    
    Removes square brackets from an identifier, and renders it as lowercase. If the identifier is unicode, it is
    recoded as us-ascii. If the identifier does not have square brackets, this function behaves sensibly, and simply
    returns the lowercase name. If the identifier has somehow gotten wrapped in multiple sets of square brackets,
    :func:`db_identifier_unquote` will remove them all, provided that they are properly balanced..

    .. note::
    
      This function WILL NOT correctly interpret a qualified identifier (multiple parts separated by periods). In particular, any
      square brackets that are in the interior of the string WILL NOT be removed. This includes a closing bracket before a period, 
      or an opening bracket following a period. DO NOT include database or schema names (when identifying tables) or database,
      schema or table names (when identifying columns).
    '''
    if name is None:
        return None
    if isinstance( name, unicode ):
        name = name.encode('ascii')
    while len(name) > 1 and name[0] == "[" and name[-1] == "]":
        name = name[1:-1]
    return name.lower()

def expand_path( path ):
    if path is None:
        return None
    return os.path.expandvars( os.path.expanduser( path ) )

class Joiner( object ):
    '''Apply format strings iteratively to the contents of a sequence or several sequences
    
    When an instance of this class is provided as a field value to the Python formatter
    (''.format()), special format specifications are available. These specifications will
    be applied iteratively over the items in the wrapped sequences.
    
    Example:
    
    .. doctest::
    
        >>> cols = Joiner( ['column_1', 'column_2', 'column_3'] )
        >>> "SELECT {cols:delimiter=','} FROM {table}".format( cols=cols , table="table_1" )
        'SELECT column_1,column_2,column_3 FROM table_1'
    
    This example is the simplest case: we specify a delimiter to be used to join the contents
    of the wrapped sequence.
    
    You can supply as many sequences as you like to the constructor of the Joiner object.
    They will treat them as a single sequence, with the concatenated contents of all of the
    embedded sequences. Any empty sequences will be ignored::
    
    .. doctest::

        >>> cols = Joiner( [ 'column_1 VARCHAR(255)', 'column_2 VARCHAR(255)', 'column_3 VARCHAR(255)' ], [], [ 'PRIMARY KEY(column_1)' ] )
        >>> "CREATE TABLE {table}({cols:delimiter=','})".format( cols=cols , table="table_1" )
        'CREATE TABLE table_1(column_1 VARCHAR(255),column_2 VARCHAR(255),column_3 VARCHAR(255),PRIMARY KEY(column_1))'
        
    You can also pass a format string that will be used to format the items in the wrapped
    sequences::
    
    .. doctest::

        >>> cols = Joiner( ['column_1', 'column_2'] )
        >>> """SELECT {cols:delimiter=',',
                        item='col',
                        itemfmt='{alias}.{{col}}'} FROM {table} AS {alias}""".format( cols=cols , table="table_1", alias="A" )
        'SELECT A.column_1,A.column_2 FROM table_1 AS A'
        
    Format Specifier Parameters
    ===========================
    
    The contents of the format specifier are treated as the argument list for a Python
    function call. As such, you can specify the arguments either by keyword or by
    ordinal position.
    
    delimiter: str (default ',')
        First positional parameter. A string that will be inserted between successive items,
        as with 'delimiter'.join()
        
    item: str (default None)
        Second positional parameter. The name by which the item should appear in the format
        string specified by the itemfmt parameter.
        
        If this is missing, then the item can be referenced in the itemfmt string with
        empty curly braces (doubled--see the discussion under itemfmt).
        
    index: str (default None)
        A name that can be referenced in the format string to return the ordinal position
        of the current item in the provided list. Index starts at 1 (not very Pythonic, I know). 
        
    itemfmt: str (default None)
        Fourth positional parameter. A format string, following the standard conventions for
        Python format strings. This string will be applied iteratively to format each element
        of the wrapped sequences.
        
        If this parameter is omitted, the default format for the item will be used
    
        Keep in mind that this format string will end up being processed twice: the first time
        as part of the larger format string; the second time when it is applied to the
        Joiner items individually. This requires you to double any curly braces that need to
        be used in the second pass. In the example above, this is set to format='{alias}.{{col}}'.
        After the first processing pass, this has been rewritten as format='A.{col}' which is the
        format specifier that will be used to process the individual items in the list.
    
    '''
    def __init__(self, *args):
        self.args = args
        
    def __format__( self, spec ):
        args, vargs = parse_arg_list( spec )
        return self._format( *args, **vargs )
    
    def __iter__( self ):
        for seq in self.args:
            for item_i in seq:
                yield item_i
        
    def _format( self, delimiter=",", item=None, index=None, itemfmt=None ):
        return delimiter.join( self._formatItems( item, index, itemfmt ) )
    
    def _formatItems( self, item, index, itemfmt ):
        if itemfmt is None:
            for item_i in self:
                yield item_i.__format__('')
        elif item is None:
            d = {}
            i = 1
            for item_i in self:
                d[ index ] = i
                i += 1
                yield itemfmt.format( item_i )
        else:
            d = {}
            i = 1
            for item_i in self:
                d[ item ] = item_i
                d[ index ] = i
                i += 1
                try:
                    yield itemfmt.format( **d )
                except ValueError:
                    raise ValueError( "Did not understand format string '{}'".format( itemfmt ) )
