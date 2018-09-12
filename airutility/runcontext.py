'''
Created on Apr 26, 2013

@author: temp_dmenes
'''

import inspect
import os.path
import sys
import logging.config
from ConfigParser import NoOptionError, NoSectionError, DEFAULTSECT


from dbcontext import DBContext
from formatutilities import expand_path
from context_config_parser import ContextConfigParser

__all__ = [ 'RunContext' ]

_CACHE = {}

# _STDIO_HANDLERS = {}
_LOGS_CONFIGURED = set()

# _srcfile logic borrowed from logging package. Used when walking up the stack trace to
# determine when we've reached the effective caller

if hasattr(sys, 'frozen'): #support for py2exe
    _srcfile = "logging%s__init__%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)

class RunContext(object):
    '''
    A context containing configuration information for a data run
    
    This class looks in certain standard locations for a configuration file
    in the usual ".ini" file format. By default, the search path contains
    the current working directory, and the directory "air_python" in the user's
    home directory. These defaults can be overridden at runtime.
    
    In order to support multiple configurations on the same computer, a
    "context name" is provided when you construct the object. This name is just
    the name of the configuration file, minus the :file:`{something}.ini` extension.
    
    For example, if you have created a file :file:`~/test_config.ini`, you can access
    the configuration defined therein with::
    
        run_context = RunContext('test_config')
    
    A sample configuration file is located in the airutility package under the
    name "runcontext.ini_template"
    
    A cache is implemented so that instantiating multiple copies of the same class
    with the same context name will all return the same actual object. Call the
    :meth:`close` method to remove a context from the cache.
    
    .. todo::
       Should throw a IOException when no files are found
    '''


    def __new__( cls, context_name, search_path=None ):
        cache_key = ( cls, context_name )
        if cache_key in _CACHE:
            return _CACHE[ cache_key ]
        new_instance = super( RunContext, cls ).__new__( cls )
        _CACHE[ cache_key ] = new_instance
        return new_instance

    def __init__( self, context_name, search_path=None ):
        '''
        Constructor
        
        context_name
            Name of the context. This is the name of the configuration file without the ".ini" extension
            
        search_path
            A list of directories in which to look for the specified files. Defaults to
            the current directory and the directory ".air_python" in the user's home
            directory.
        '''
        
        if hasattr( self, "initialized" ):
            return
        
        if search_path == None:
            search_path = [
                os.path.abspath( '.' ),
                os.path.normpath( os.path.expanduser('~/air_python/') )
            ]

        
        self.name = context_name
        
        files = [ os.path.join( path, context_name + ".ini" ) for path in search_path ]
        
        self.config = ContextConfigParser()
        self.register_default( 'context_name', context_name )
#         self.register_default('level', 'INFO', 'LOGGING')
        home_dir = os.path.abspath( os.path.expanduser( '~' ) ).replace('\\','/')
        self.register_default( 'home_dir', home_dir )
        self.register_default( 'user_dir', os.path.join( home_dir, 'air_python' ).replace('\\','/') )
        p, f = os.path.split( _srcfile )
        checkout_dir=os.path.normpath( os.path.join( p , '../../..' ) )
        self.register_default( 'checkout_dir', checkout_dir )

        self.config.read( files )
        
        
#         log_level = self.config.get( 'LOGGING', 'level' )
#         self.logger.setLevel( log_level )
#         if context_name not in _STDIO_HANDLERS:
#             handler = logging.StreamHandler( sys.stderr )
#             _STDIO_HANDLERS[ context_name ] = handler
#             self.logger.addHandler( handler )
        old_class = logging.getLoggerClass()
        logging.setLoggerClass( PassThroughLogger )
        self.logger = logging.getLogger( context_name )
        self.sql_logger = logging.getLogger( context_name + '.sql' )
        logging.setLoggerClass( old_class )
        if self.name not in _LOGS_CONFIGURED:
            _LOGS_CONFIGURED.add( self.name )
            log_config_file = self.getConfigFile( 'LOGGING', 'config' )
            if log_config_file is None:
                print "Logging not configured.  Please add a 'config' entry to the [LOGGING] section of {}.ini".format( self.name )
            else:
                try:
                    logging.config.fileConfig( log_config_file, self.config.defaults(), False )
                except BaseException as e:
                    print "Unable to read logging configuration."
                    print "Got exception:" + repr( e )
            
        
        self.DBContextCache = {}
        self.initialized = True
        
    def get_db_context( 
      self, tag=None, instance_name=None, cached=True ):
        """Get a :class:`DBContext` object defined in this run context
        
        :param tag: Identifies which DB context to return. Use `None` or omit to get the default DBContext
        
        :type tag: str
        
        :param instance_name: If you wish to cache multiple instances of the same context, specify different instance_names.
            Ignored if :attr:`cached` if :const:`False`
        
        :type instance_name: str
        
        :param cached: Defaults to :const:`True`. If :const:`True`, :meth:`getDBContext` will look for a cached copy with
            the same ``tag`` and ``instance_name``, and return that if available.  If a new copy needs to be
            created, then it will be cached.  If :const:`False`, then a new copy will always be created, and that copy
            will not be added to the cache.
        
        :type cached: bool
        
        :returns: :class:`DBContext` -- The DB context
        """
        if cached:
            key = ( tag, instance_name )
            if key in self.DBContextCache:
                return self.DBContextCache[ key ]
        new_context = DBContext( self, tag )
        if cached:
            self.DBContextCache[ key ] = new_context
        return new_context
    
    def getDBContext( 
      self, tag=None, instance_name=None, cached=True ):
        return self.get_db_context(tag, instance_name, cached) 
                             
    def get_logger( self, name=None ):
        """Get a logger object
        
        You should use this method rather than :meth:`Logging.getLogger` to get a logger object. This method will return
        a logger which is a descendant of the :class:`RunContext`'s root logger, rather than the overall root logger. The
        root logger for the :class:`RunContext` has the same name as the context name.
        
        Previously, using the context's default logger would result in all of the log messages appearing to come from
        the `db_context.py` module. This is no longer the case. The logger will search up the stack to identify the proper caller.
        
        :param name: Name of the logger to return. If omitted or `None`, then the root logger for the RunContext is returned.
        :type name: str
        :returns: :class:`logging.Logger` -- a logger
        """
        if name == None:
            return self.logger
        return logging.getLogger( '.'.join( ( self.name, name ) ) )

    def get_sql_logger( self, name=None ):
        """Get a logger object for SQL.
        
        The default logger for SQL queries is called :samp:`{context_name}.sql`. It will generally be set to a less verbose
        priority level than the main logger.
        
        :param name: Name of the logger to return. Will be prefixed with :samp:`{context_name}.sql.` If omitted or `None`, the
            default SQL logger for the context will be returned.
        """
        
        if name == None:
            return self.logger
        return self.get_logger( 'sql' )

    def debug(self, msg, *args, **kwargs):
        """Log a "debug" message
        
        Logs a debug message using the default logger for the context. 
        Previously, using the context's default logger would result in all of the log messages appearing to come from
        the `db_context.py` module. This is no longer the case. The logger will search up the stack to identify the proper caller.
        """
        self.logger.debug( msg, *args, **kwargs )

    def info(self, msg, *args, **kwargs):
        """Log an "info" message
        
        Logs a debug message using the default logger for the context. 
        Previously, using the context's default logger would result in all of the log messages appearing to come from
        the `db_context.py` module. This is no longer the case. The logger will search up the stack to identify the proper caller.
        """
        self.logger.info( msg, *args, **kwargs )

    def warning(self, msg, *args, **kwargs):
        """Log a "warning" message
        
        Logs a debug message using the default logger for the context. 
        Previously, using the context's default logger would result in all of the log messages appearing to come from
        the `db_context.py` module. This is no longer the case. The logger will search up the stack to identify the proper caller.
        """
        self.logger.warning( msg, *args, **kwargs )

    def error(self, msg, *args, **kwargs):
        """Log an "error" message
        
        Logs a debug message using the default logger for the context. 
        Previously, using the context's default logger would result in all of the log messages appearing to come from
        the `db_context.py` module. This is no longer the case. The logger will search up the stack to identify the proper caller.
        """
        self.logger.error( msg, *args, **kwargs )

    def critical(self, msg, *args, **kwargs):
        """Log a "critical error" message
        
        Logs a debug message using the default logger for the context. 
        Previously, using the context's default logger would result in all of the log messages appearing to come from
        the `db_context.py` module. This is no longer the case. The logger will search up the stack to identify the proper caller.
        """
        self.logger.critical( msg, *args, **kwargs )
        
    def get_config( self, section, option, default=None ):
        """Get a configured value
        
        Unlike the corresponding method of the :class:`ConfigParser` object, this method will return a default value rather than
        throwing an error if the sepcified option or section is not found.
        
        :param section: The name of the section in which to look for the value
        :type section: str
        :param option: The option name to search for
        :type optiion: str
        :param default: The default value to use if the option is not found. Defaults to `None`
        :returns: The option value. May be `None`, a string, or whatever was passed in as the default
        """
        try:
            return self.config.get( section, option )
        except ( NoOptionError ):
            return self.config.interpolate( default, section, option )
        except ( NoSectionError ):
            return self.config.interpolate( default, DEFAULTSECT, option )

    def getConfig( self, section, option, default = None ):
        return self.get_config(section, option, default)
            
    
    def getConfigFile( self, section, option, default = None ):
        """Get a configured filename
        
        This is similar to :meth:`getConfig`, but environment variables and `~` will be expanded appropriately.
        
        :param section: The name of the section in which to look for the value
        :type section: str
        :param option: The option name to search for
        :type optiion: str
        :param default: The default value to use if the option is not found. Defaults to `None`
        :returns: The option value. May be `None`, a string, or whatever was passed in as the default
        """
        return expand_path( self.getConfig( section, option, default ) )
    
    def interpolate( self, s, section, name="<variable>" ):
        return self.config.interpolate( s, section, name )
    
    def defaultToConfig( self, value, section, option ):
        """Return value, unless it is None, in which case the configured value is returned.
        
        If value is `None`, and no default is configured, then `None` is returned.
        
        :param section: The name of the section in which to look for the value
        :type section: str
        :param option: The option name to search for
        :type optiion: str
        :returns: The value, the configured value (a string), or `None`
        """
        return value if value is not None else self.getConfig( section, option )
    
    def register_default( self, option, value, section=None ):
        """Register a default value to be used when parsing configuration files.
        
        :param option: The option name to search for
        :type optiion: str
        :param value: The default value
        :type value: str
        :param section: The name of the section in which to look for the value. If omitted, `None`, or `"DEFAULT"`, then the
            value will be added to the default section and will be available for macro substitution anywhere in the file. See
            :class:`ConfigParser`
        :type section: str
        :returns: The value, the configured value (a string), or `None`
        """
        if section is None or section == 'DEFAULT':
            d = self.config.defaults()
            if option not in d:
                d[option] = value
        else:
            if not self.config.has_section( section ):
                self.config.add_section( section )
            if not self.config.has_option( section, option ):
                self.config.set( section, option, value )

    def close( self ):
        '''Remove this RunContext instance from the cache and close all open
        :class:`DBContext` objects
        
        Next call to constructor will return a new object rather than a cached object.
        '''
        contexts = self.DBContextCache.values()[:]
        for context in contexts:
            context.close()
        cache_key = ( self.__class__, self.name)
        del _CACHE[ cache_key ]

class PassThroughLogger( logging.Logger ):
    """A logger which will report the caller as the method that called the method on RunContext.
    """
    def findCaller(self):
        """
        Override the original search function .
        """
        f = inspect.currentframe()
        #On some versions of IronPython, currentframe() returns None if
        #IronPython isn't run with -X:Frames.
        if f is not None:
            f = f.f_back
        rv = "(unknown file)", 0, "(unknown function)"
        while hasattr(f, "f_code"):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            codename = co.co_name
            if ( filename == logging._srcfile
                 or ( filename == _srcfile
                      and codename in ('debug', 'info', 'warning', 'error', 'critical') ) ):
                f = f.f_back
                continue
            rv = (co.co_filename, f.f_lineno, co.co_name)
            break
        return rv
