'''Context describing a database connection

@author P Lakshmanan


Created on: 2013-04-25

Changes:
========

2013-04-26
  D Menes integrated P Lakshman's original DBContext code with a new RunContext class
  
2013-05-06
  D Menes added ability to specify servers by means of tags in the .ini file
'''
import os
import os.path

import pyodbc as p

from arglist import parse_arg_list
from formatutilities import db_identifier_quote
import tablespec
 
__all__ = ['DBContext']

class DBContext(object):
    """Holds a connection to a database, and metadata describing that connection.
    
    :class:`DBContext` objects should not be created directly, but should be retrieved from
    :meth:`airassessmentreporting.airutility.RunContext.getDBContext`.
    
    Each :class:`DBContext` has a name. By default, :class:`DBContext` objects will be cached,
    so repeated requests for :class:`DBContext` objects with the same name will return the
    same object. This behavior can be overridden, however, for cases where two simultaneous
    connections to the same database are required.
    
    Connection information for the :class:`DBContext` objects is in the :file:`{context}.ini` file.
    For example, a :class:`DBContext` named ``unittest`` would be defined by a section like this::
    
        [DB]
        unittest=server='MYCOMPUTER\SQLEXPRESS', database='unittest_db'
        
    This would configure a connection to a database called ``unittest_db`` on the ``SQLEXPRESS``
    service of ``MYCOMPUTER``
    
    Optionally, a schema for the database may be specified by adding ``, schema='my_schema` to
    the configuration line.
    
    Currently only MS SQL Server connections are supported.
    
    .. attribute:: runContext
    
        The :class:`airassessmentreporting.airutility.RunContext object with which this :class:`DBContext`
        is associated.
        
    .. attribute:: tag
    
        The name of the :class:`DBContext`. 
        If given as a constructor argument, the tag value must be found in the 
        ini file associated with the run_context attribute, specifically located
        in the section "[DB]", and its value must have component settings for
        'server' and 'database'. 
        The default tag value is "default_database".
        
    .. attribute:: server
    
        The name of the server to which to connect.
        
    .. attribute:: db
    
        The name of the database on the server to which to connect.
        
    .. attribute:: db_name
    
        The name of the database on the server to which to connect, quoted using rules appropriate
        for the database software in use. This attribute is read-only.  To change it, change
        the :attr:`db` attribute
        
    .. attribute:: schema
    
        The name of the default schema for this :class:`DBContext`. Defaults to ``[dbo]``. The
        name will always be returned quoted appropriately for the database software in use.
    """
    def __init__(self,run_context,tag):
        self.runContext = run_context
        self.tag=tag
        
        if tag is None:
            self._setConnectParams( database = run_context.getConfig( 'DB', 'default_database' ),
                                             server = run_context.getConfig( 'DB', 'default_server' ),
                                             schema = run_context.getConfig( 'DB', 'default_schema', 'dbo') )
            if self.db is None or self.server is None:
                raise ValueError( "Default database connection parameters must be specified in {name}.ini file".
                                   format( tag=tag, name=run_context.name ) )
            self.logger = run_context.get_sql_logger()
        else:
            server_args = run_context.getConfig( 'DB', tag )
            if server_args is None:
                raise ValueError( "Did not find configuration for db context '{}' in run context ''"
                                  .format( tag, run_context.name ) )
            arg_list, arg_dict = parse_arg_list( server_args )
            self._setConnectParams( *arg_list, **arg_dict )
            if self.db is None or self.server is None:
                raise ValueError( "Database connection parameters for database {tag} must be specified in {name}.ini file".
                                   format( tag=tag, name=run_context.name ) )
            self.logger = run_context.get_logger( 'sql.' + tag )
            
            
        self.conn = self._getDefaultConn()
    
    def __del__(self):
        self.close()
        
    
    def _getDefaultConn(self):
       
        self.logger.debug( 'DBCONTEXT db=%s', self.db )
        connStr = ( r'DRIVER={SQL Server};SERVER=' +
                    self.server + ';DATABASE=' + self.db + ';' +
                    'Trusted_Connection=yes;' + 'unicode_results=False;' + 'CHARSET=UTF8'    )
        
        self.logger.debug( 'Connecting with connection string %s', connStr )
        return p.connect(connStr)
    
    def _setConnectParams( self, database, server, schema="[dbo]" ):
        self.server = server
        self.db = database
        self.schema = schema
    
    def createcur(self):
        return self.conn.cursor()
    
    """This method returns a generator object"""
    def execQuery(self,cursor,arraysize=10000):
        # This relies on the GC to clean up the cursor and iterator.
        
        
        if not isinstance(arraysize,int):
            raise Exception("You must provide an integer as the arraysize parameter.")
            self.logger.debug( 'DBCONTEXT execQuery array size is not an integer, raising exception')
            
        self.logger.debug( ' DBCONTEXT execQuery cursor=%s', cursor )
        
        while True:
            results = cursor.fetchmany(arraysize)
            self.logger.debug( 'DBCONTEXT execQuery cursor=%s',cursor )
            if not results:
                break
            for result in results:
                yield result
                
    def executeBuffered(self,query='',parameters=[], arraysize=10000):
        """ This method executes the query with the given parameters and 
            returns an iterator through the results.
            
            :parameter query: The query you want to run formatted for the parameters passed.
            
            :type query: :func:`str`
                
            :parameter parameters: Parameters that will be formatted into the query.
            
            :type parameters: sequence or None

            :parameter arraysize: Number of rows to have in memory at a time. The default value is 10000.
            
            :type arraysize: :func:`int`
                
            :returns: an iterator over the result set
        """
        cur = self.conn.cursor()
        self.logger.debug( 'DBCONTEXT executeBuffered query=%s', query )
        self.logger.debug( 'DBCONTEXT executeBuffered parameters=%s', parameters )
        
        #make sure arraysize is an integer
        if not isinstance(arraysize,int):
            raise Exception("You must provide an integer as the arraysize parameter.")
            self.logger.debug( 'DBCONTEXT executeBuffered array size is not an integer, raising exception')
        try:
            cur = cur.execute(query,parameters)
            while True:
                self.logger.debug( 'DBCONTEXT executeBuffered fetching %s rows', str(arraysize))
                results = cur.fetchmany(arraysize)
                if not results:
                    break
                for result in results:
                    yield result
        finally:
            self.logger.debug( 'DBCONTEXT executeBuffered ended yielding results, cleaning up cursor')
            cur.close()
                
    def execute(self,query='',parameters=[]):
        """ This method executes the query with the given parameters and 
            returns a list containing all of the results.
            
            :parameter query: The query you want to run formatted for the parameters passed.
            
            :type query: :func:`str`
                
            :parameter parameters: Parameters that will be formatted into the query.
            
            :type parameters: sequence or None

            :returns: a list containing the entire result set
        """
        cur = self.conn.cursor()
        self.logger.debug( 'DBCONTEXT execute query=%s', query )
        self.logger.debug( 'DBCONTEXT execute parameters=%s', parameters )
        result = []
        try:    
            cur.execute(query,parameters)
            result  = cur.fetchall()
        finally:
            self.logger.debug( 'DBCONTEXT execute cursor closed' )
            cur.close()
            
        self.logger.debug( 'DBCONTEXT execute result=%s', result )
        return result
    
    def executemany(self,query='',parameters=[]):
        """ This method executes a query repeatedly with varying parameters. No results
            are returned.
            
            :parameter query: The query you want to run formatted for the parameters passed.
            
            :type query: :func:`str`
                
            :parameter parameters: Parameters that will be formatted into the query.
            
            :type parameters: sequence of sequences

            :returns: None
        """
        cur = self.conn.cursor()
        self.logger.debug( 'DBCONTEXT execute query=%s', query )
        self.logger.debug( 'DBCONTEXT execute parameters=%s', parameters )
        try:    
            cur.executemany(query,parameters)
        except BaseException as e:
                raise e
        finally:
            self.logger.debug( 'DBCONTEXT execute cursor closed' )
            cur.close()
    
    def executeNoResults( self, query='', parameters=[], commit=True ):
        """ Execute a query with the given parameters that returns no results. Unlike the
            other execute methods, :meth:`executeNoResults` by default commits the transaction on
            completion.  This behavior can be overridden with the `commit` parameter.
            
            :parameter query: The query you want to run formatted for the parameters passed.
            
            :type query: :func:`str`
                
            :parameter parameters: Parameters that will be formatted into the query. Optional
            
            :type parameters: sequence
            
            :parameter commit: If True, commit the transaction before returning. Defaults to True.

            :returns: None
        """
        self.logger.debug( 'DBCONTEXT execute query=%s', query )
        self.logger.debug( 'DBCONTEXT execute parameters=%s', parameters )
        try:
            self.conn.execute(query,parameters)
            if commit:
                self.conn.commit()
        except BaseException as e:
            if commit:
                self.conn.rollback()
                raise e
        
    def close(self):
        """ Close the underlying connection and remove this object from the cache.
            
            :returns: None
        """
        if self.conn != None:
            try:
                self.conn.close()
            except ( p.DatabaseError, AttributeError ):
                # Do nothing--probably just means that connection was already closed or
                # object was never fully initialized 
                pass
            
            self.conn = None
            try:
                del self.runContext.DBContextCache[ self.tag ]
            except KeyError:
                # Do nothing--for some reason this item wasn't in the cache
                pass
            
    def commit(self):
        """ Commit any pending transactions on the connection.
            
            :returns: None
        """
        self.conn.commit()
        
    def rollback(self):
        """ Roll back any pending transactions on the connection.
            
            :returns: None
        """
        self.conn.rollback()
        
    def getTableSpec( self, table_name, table_schema=None, field_spec_class=tablespec.FieldSpec ):
        """ Return a :class:`airassessmentreporting.airutility.TableSpec` object describing a table on
            this database.
            
            If the table exists, the :class:`airassessmentreporting.airutility.TableSpec` will be populated
            with the :class:`airassessmentreporting.airutility.FieldSpec`\ s
            representing the columns in the table. Primary key and foreign key information will also be
            read. If the table does not exist, than an empty :class:`airassessmentreporting.airutility.TableSpec`
            with the proper name will be returned.
            
            :parameter table_name: Name of the table
            
            :type table_name: ``str``
            
            :parameter table_schema: Database schema in which the table will be created. Optional Defaults to :const:`None`.
                If :const:`None`, the schema name of the :class:`DBContext` will be used.
            
            :type table_schema: ``str`` or None
            
            :parameter field_spec_class: Class or factory function that will be used to create rows for the :class:`TableSpec`.
                Optional. Defaults to :class:`airassessmentreporting.airutility.FieldSpec`
            
            :type table_schema: ``class`` or None
            
            :returns: :class:`airassessmentreporting.airutility.TableSpec`
        """
        table_spec = tablespec.TableSpec( self, table_name=table_name,
                table_schema=table_schema, field_spec_class=field_spec_class )
        if table_name is not None:
            table_spec.populate_from_connection()
        return table_spec
    
    def executeFile( self, file_or_name, commitBatch=False, commitEach=True ):
        ''' Execute contents of file as SQL on connected server
        
            As of now, this implements an absolutely minimal subset of the
            functionality of Microsoft's SQLCMD. Only the "GO" command is recognized.
            No results are logged or returned.
            
            If anyone wants to provide additional functionality, feel free.
            
            :parameter file_or_name: An open file object or the name of the file to open
            
            :parameter commitBatch:  Whether to perform commit at the end of the script
            
            :parameter commitEach:  Whether to perform commit after each command in the script        
        '''
        if isinstance( file_or_name, ( str, unicode ) ):
            if not os.path.exists( file_or_name ):
                raise IOError( "File %s does not exist", file_or_name )
            if not os.path.isfile( file_or_name ):
                raise IOError( "%s is not a file", file_or_name )
            if not os.access( file_or_name, os.R_OK ):
                raise IOError( "%s is not a file", file_or_name )
            f = open( file_or_name, 'R' )
        else:
            f = file_or_name
        try:
            cmd_buffer =[]
            for line in f:
                line = line.strip()
                self.logger.debug( "SCRIPT FILE: %s", line )
                if len( line ) == 2 and line.upper() == "GO":
                    self.conn.execute( "\n".join( cmd_buffer ) )
                    if commitEach:
                        self.commit()
                    cmd_buffer = []
                elif len( line ) == 0:
                    continue
                else:
                    cmd_buffer.append( line )
                
            if len( cmd_buffer ) > 0:
                self.conn.execute( "\n".join( cmd_buffer ) )
                if commitEach:
                    self.commit()
            if commitBatch:
                self.commit()
        except BaseException as e:
            if commitBatch or commitEach:
                self.rollback()
            raise e
        finally:
            f.close()
            
    @property
    def schema(self):
        return self._schema
    
    @schema.setter
    def schema(self, value):
        self._schema = db_identifier_quote( value )
        
    @property
    def db_name(self):
        return db_identifier_quote( self.db )

