'''
Created on Apr 29, 2013

@author: temp_dmenes
'''

from tablespec import TableSpec
from formatutilities import db_identifier_unquote, db_identifier_quote, Joiner
from dictlist import DictList
import temptable

__all__ = [ 'get_column_names', 'get_table_names', 'clear_all',
            'table_exists', 'drop_table_if_exists', 'get_temp_table_name',
            'assembly_exists', 'drop_assembly_if_exists', 'dump', 'n_obs',
            'get_table_spec', 'get_temp_table' ]

_GET_COLUMN_NAMES_QUERY = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ? AND table_schema=?"
_GET_TABLE_NAMES_QUERY = "SELECT DISTINCT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema=?"
_GET_FOREIGN_KEYS_QUERY = """
    SELECT constraint_name, table_schema, table_name
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    WHERE constraint_type='FOREIGN KEY'
"""
_GET_ASSEMBLY_FUNCTIONS_QUERY ="""
    SELECT A.name AS object_name, A.type AS type, B.name AS object_schema
    FROM (
        ( sys.objects A
          INNER JOIN sys.schemas B
          ON A.schema_id=B.schema_id
        )
        INNER JOIN sys.assembly_modules C
        ON A.object_id=C.object_id )
    INNER JOIN sys.assemblies D
    ON C.assembly_id=D.assembly_id
    WHERE D.name=?
"""
_DROP_CONSTRAINT_QUERY = "ALTER TABLE [{table_schema}].[{table_name}] DROP CONSTRAINT [{constraint_name}]"
_DROP_TABLE_QUERY = "DROP TABLE {table_schema}.{table}"
_DROP_TABLE_QUERY_2 = "DROP TABLE {table:qualified}"
_DROP_ASSEMBLY_QUERY = "DROP ASSEMBLY [{assembly_name}]"
_DROP_FUNCTION_QUERY = "DROP FUNCTION [{schema}].[{name}]"
_DROP_PROC_QUERY = "DROP PROC [{}].[{name}]"
_TEST_EXISTS_TABLE_QUERY = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?"
_TEST_EXISTS_ASSEMBLY_QUERY = "SELECT 1 FROM sys.assemblies WHERE name=?"
_DUMP_TABLE_QUERY = "SELECT {fields} FROM {table:qualified} {order_by} {where}"
_N_OBS_QUERY = "SELECT COUNT(1) FROM {schema}.{table}"

_TEMP_TABLE_COUNTER = 0

def get_table_spec( table, db_context=None, table_schema=None ):
    """Returns a table_spec object that encodes the supplied table, context and
    schema information.
    
    Unlike the similarly named method of the :class:`DBContext`
    class, this function will never populate the table_spec with the schema
    details from the database.
    
    This method is mainly intended for normalizing the inputs to other methods,
    where a table may be specified either as a string or as a
    :class:`TableSpec` object
    
    Parameters
    ----------
    table : str or :class:`TableSpec`
        If a :class:`TableSpec` object is provided, it is simply returned. If
        a string is provided, it is used to construct a :class:`TableSpec`
        object.
        
    db_context : :class:`DBcontext` (optional if `table` is :class:`TableSpec`)
        If table is specified as a string, this becomes the db_context property
        of the newly constructed :class:`TableSpec`.
        
        If `table` is a :class:`TableSpec` object, this is optional. If
        provided, it must match the `db_context` property of the provided
        :class:`TableSpec`
        
    table_schema : str (optional)
        If table is specified as a string, this becomes the db_context property
        of the newly constructed :class:`TableSpec`. If omitted, the table's
        schema will be the default schema for the `db_context`.
        
        If `table` is a :class:`TableSpec` object, this is optional. If
        provided, it must match the `table_schema` property of the provided
        :class:`TableSpec`
    """
    if isinstance( table, TableSpec ):
        if ( db_context is not None ) and ( table.db_context is not db_context ):
            raise ValueError( "Two different db_contexts were provided: one explicit and one as a property of a table object." )
        if ( table_schema is not None ) and ( table.table_schema != table_schema ):
            raise ValueError( "Two different schemas were provided: one explicit and one as a property of a table object." )
        return table
    if db_context is None:
        raise ValueError("You must provide a db_context either explicitly or as a property of a table object")
    return TableSpec(db_context, table, table_schema)


def get_column_names( table, db_context=None, table_schema=None ):
    """Returns a list of the names of the columns in a table, quoted with []
    
    Parameters
    ==========
    table : str or TableSpec
        The table
        
    db_context : DBContext
        The database context that contains the table. If the table is a TableSpec
        object that specifies a db_context property, you can omit this.
        
    table_schema : str
        The name of the schema in which the table is located. If you omit
        this, the value will be read from the tablespec or from the db_context.
        This will usually lead to the default value of [dbo] being used.
    """
    
    table_spec = get_table_spec( table, db_context, table_schema )
    table_name = db_identifier_unquote( table_spec.table_name )
    table_schema = db_identifier_unquote( table_spec.table_schema )
    db_context = table_spec.db_context
    return [ '['+row.column_name+']' for row in db_context.execute( _GET_COLUMN_NAMES_QUERY, ( table_name, table_schema ) ) ]


def _get_best_schema( db_context, table_schema ):
    """Returns the "best" schema name to use.
    
    Returns table_schema if it is not None, else db_context.schema
    """
    if table_schema is None:
        return db_context.schema
    else:
        return db_identifier_quote( table_schema )

def get_table_names( db_context, table_schema=None ):
    """Returns a list of all of the tables in a database schema
    
    Parameters
    ----------
    db_context : :class:`DBContext`
        The DBContext object defining the database to examine
        
    table_schema : str optional
        The schema within which we will look for tables. If not supplied, will
        search the default schema for the `db_context` (usually `[dbo]`)
    """
    table_schema = db_identifier_unquote( _get_best_schema( db_context, table_schema ) )
    return [ '['+row.table_name+']' for row in db_context.execute( _GET_TABLE_NAMES_QUERY, (table_schema,) ) ]

def clear_all( db_context, table_schema=None ):
    """Drop all of the tables in a schema within a database.
    
    This method is smart enough to drop all of the foreign key constraints
    before dropping the tables.
    
    This method does not drop anything besides tables.
    
    Parameters
    ----------
    db_context : :class:`DBContext`
        The DBContext object defining the database to examine
        
    table_schema : str optional
        The schema within which we will look for tables. If not supplied, will
        search the default schema for the `db_context` (usually `[dbo]`)
    """
    table_schema = _get_best_schema( db_context, table_schema )
    for row in db_context.execute( _GET_FOREIGN_KEYS_QUERY ):
        query = _DROP_CONSTRAINT_QUERY.format(
                table_name=row.table_name,
                table_schema=row.table_schema,
                constraint_name=row.constraint_name )
        db_context.executeNoResults( query )
    tables = get_table_names( db_context )
    for table in tables:
        query = _DROP_TABLE_QUERY.format( table = table, table_schema = table_schema )
        
        db_context.executeNoResults( query )
        
def table_exists( table, db_context=None, table_schema=None):
    """Test whether the identified table exists in the database
    
    Parameters
    ==========
    table : str or TableSpec
        The table
        
    db_context : DBContext
        The database context that contains the table. If the table is a TableSpec
        object that specifies a db_context property, you can omit this.
        
    table_schema : str
        The name of the schema in which the table is located. If you omit
        this, the value will be read from the tablespec or from the db_context.
        This will usually lead to the default value of [dbo] being used.
    """
    table_spec = get_table_spec( table, db_context, table_schema )
    for row in table_spec.db_context.execute( _TEST_EXISTS_TABLE_QUERY,
                ( db_identifier_unquote( table_spec.table_name ),
                  db_identifier_unquote( table_spec.table_schema ) ) ):
        return True
    return False

def n_obs( table, db_context=None, table_schema=None ):
    """Return number of rows in a table
    
    Parameters
    ==========
    table : str or TableSpec
        The table whose rows you need counted
        
    db_context : DBContext
        The database context that contains the table. If the table is a TableSpec
        object that specifies a db_context property, you can omit this.
        
    table_schema : str
        The name of the schema in which the table is located. If you omit
        this, the value will be read from the tablespec or from the db_context.
        This will usually lead to the default value of [dbo] being used.
    """
    table_spec = get_table_spec( table, db_context, table_schema )
    for row in table_spec.db_context.execute( _N_OBS_QUERY.format(
            table = db_identifier_unquote( table_spec.table_name ),
            schema = db_identifier_unquote( table_spec.table_schema ) ) ):
        return row[0]


def drop_table_if_exists( table, db_context=None, table_schema=None ):
    """Drop the identified table if it exists in the database
    """
    table_spec = get_table_spec( table, db_context, table_schema )
    name = db_identifier_unquote( table_spec.table_name )
    schema = db_identifier_unquote( table_spec.table_schema )
    db_context = table_spec.db_context
    
    # We are using "for" as an "if"--ugly, but effective
    for row in table_spec.db_context.execute( _TEST_EXISTS_TABLE_QUERY,
                                              ( name, schema ) ):
        db_context.executeNoResults( _DROP_TABLE_QUERY_2.format(
                table = table_spec ) )
        break
        
def dump( table, order_by='', where='', db_context=None, table_schema=None  ):
    """Dump the contents of a table.
    
    An easy way to get "SELECT * FROM table".
    
    Parameters
    ----------
    table : str or :class:`TableSpec`
        The table whose rows you need returned
        
    order_by : str optional
        An optional "ORDER BY" clause. If it does not begin with the string
        "ORDER BY ", that string will be added before inserting it into the
        query. 

    where : str optional
        An optional "WHERE" clause. If it does not begin with the string
        "WHERE ", that string will be added before inserting it into the
        query. 

    db_context : :class:`DBContext` optional
        The database context that contains the table. If the table is a TableSpec
        object that specifies a db_context property, you can omit this.
        
    table_schema : str
        The name of the schema in which the table is located. If you omit
        this, the value will be read from the tablespec or from the db_context.
        This will usually lead to the default value of [dbo] being used.
    
    Returns
    -------
    A list of row objects like those returned by SafeExcelReader
    """
    table = get_table_spec(table, db_context, table_schema)
    table.populate_from_connection()
    order_by = order_by.strip()
    if order_by != '':
        if order_by[0:10].upper() != 'ORDER BY ':
            order_by = 'ORDER BY ' + order_by
    where = where.strip()
    if where != '':
        if where[0:6].upper() != 'WHERE ':
            where = "WHERE " + where
    query = _DUMP_TABLE_QUERY.format( fields=Joiner( table ), table=table, order_by=order_by, where=where )
    ans = []
    for list_i in table.db_context.execute( query ):
        row_i = Row()
        ans.append( row_i )
        for field_i, value in zip( table, list_i ):
            row_i[field_i.field_name] = value
    return ans

def assembly_exists( assembly_name, db_context ):
    """Test whether a "CLR assembly" exists in the database
    
    A CLR Assembly is an external library containing stored procedures and user-defined
    functions.
    """
    assembly_name = db_identifier_unquote( assembly_name )
    for row in db_context.execute( _TEST_EXISTS_ASSEMBLY_QUERY, ( assembly_name, ) ):
        return True
    return False

def drop_assembly_if_exists( assembly_name, db_context ):
    """Drop a "CLR assembly" from the database
    
    A CLR Assembly is an external library containing stored procedures and user-defined
    functions.
    """
    
    assembly_name = db_identifier_unquote( assembly_name )
    for row in db_context.execute( _TEST_EXISTS_ASSEMBLY_QUERY, ( assembly_name, ) ):
        # First, drop the defined functions and procs that are defined in the assembly
        for row in db_context.execute( _GET_ASSEMBLY_FUNCTIONS_QUERY, ( assembly_name, ) ):
            t = row.type
            object_name = row.object_name
            object_schema = row.object_schema
            if t in ( 'AF', 'FS', 'FT', 'TA' ):
                db_context.executeNoResults( _DROP_FUNCTION_QUERY.format( name=object_name, schema=object_schema ) )
            elif t == 'PC':
                db_context.executeNoResults( _DROP_PROC_QUERY.format( name=object_name, schema=object_schema ) )
        # Now drop the assembly
        db_context.executeNoResults( _DROP_ASSEMBLY_QUERY.format( assembly_name=assembly_name ) )
        break

def get_temp_table_name( db_context ):
    """ Return a name for a temporary table.
    
        By default, the temporary file will be visible only to the current connection, and
        will be deleted when the connection is closed. For debugging purposes, it is useful
        to have temp files that are added permanently to the database schema. To have this
        happen, put the following in your .ini file::
        
            [MISC]
            use_visible_temp_files=True
    
        This routine checks to see if the returned name exists, although it is always possible
        that another table of the same name will be created before you actually use it, if
        you have multi-threaded code
        
        Note that calling this routine does NOT ensure that a table created with this name
        will be deleted when the object leaves scope.  To ensure this behavior, use the
        :func:`get_temp_table` function instead.
        
        :parameter db_context:  The DBContext object defining the database and schema on which this temporary table should be created.
        
        :type db_context: :class:`DBContext`
    """
    name = _get_temp_table_name( db_context )
    while table_exists( name, db_context ):
        name = _get_temp_table_name( db_context )
    return name
        
def _get_temp_table_name( db_context ):
    global _TEMP_TABLE_COUNTER

    _TEMP_TABLE_COUNTER += 1
    
    if db_context.runContext.getConfig( 'MISC', 'use_visible_temp_files', False ):
        prefix = "TEMP_"
    else:
        prefix = "#_"
    return prefix + db_context.runContext.name + '_' + str( _TEMP_TABLE_COUNTER )

def get_temp_table( db_context ):
    """ Return a :class:`TempTable` with a name for a temporary table. NOTE THAT THIS METHOD
        DOES NOT CREATE THE TABLE!
        
        By default, the temporary table will be visible only to the current connection.
        
        By default, the temporary table will be deleted when the :class:`TempTable` goes out of
        scope. The :class:`TempTable` may be used as a context manager if you wish to
        ensure that it is cleaned up as soon as the relevant code has completed.
        
        Note that the table will NOT be deleted if an exception is being handled when the
        cleanup code is called (either because the context block exits, or because the
        object is garbage-collected).
        
        For debugging purposes, you can change the behavior of the object by adding the following
        in your .ini file::
        
            [MISC]
            use_visible_temp_files=True
            delete_temp_files=False
    
        The routine will do its best to return a :class:`TempTable` with a name that does not already exist.
    
        :parameter db_context:  The DBContext object defining the database and schema on which this temporary table should be created.
        
        :type db_context: :class:`DBContext`
    """
        
    return temptable.TempTable( db_context, get_temp_table_name( db_context ) )

def drop_tables(tables, dbContext):
    DROP_TABLES = "{tables:delimiter=' ', item='X', itemfmt=' IF OBJECT_ID(\"dbo.{{X}}\", \"U\") IS NOT NULL drop table {{X}}; '}"
    dbContext.executeNoResults(DROP_TABLES.format(tables=Joiner(tables)).replace('"', "'"))

"""A :class:`DictList` that normalizes keys as if they were database identifiers
"""
class Row( DictList ):
    _normalize_name = staticmethod( db_identifier_unquote )
    _normalize_property = staticmethod( db_identifier_unquote )
    
