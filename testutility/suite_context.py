
import pyodbc
import os
import logging
from airassessmentreporting.airutility import RunContext
import airassessmentreporting

class SuiteContext(RunContext):
    def __init__(self, config_filename_prefix):
        """
        Make a SuiteContext with test.ini file, and make persistent connection.
        
        Extended summary
        ----------------
        
        SuiteContext("my_ini_file_prefix") is an environment to support unit testing and potentially production execution. It is derived from RunContext(). 
        
        SuiteContext() provides, through member "conn" an initialized persistent database connection to the database and server defined in the "TESTS" section by values default_database and default_server. 
        
        SuiteContext() adds the log_file that is defined by .ini to the logger so you can capture all log output in a flat file for examination or comparison purposes either manually or by unit testing code.
        
        A SuiteContext() may be used as the "run_context" initial argument to our Python functions and methods to test.
        
        A SuiteContext() may be initialized in a unittest "setUpClass()" to open the
        database connection for a series of tests and provide handy access to execution and testing parameters like log file name and location, location of input and output test buffer_. 
         
        
        PARAMETERS 
        ----------
        
        Parameters are identified below by ini file section name and variable name.
        They are all of type string, however, in an ini file you do not enclose the
        value in single or double quotes.
        
        The parameters are stored in the SuiteContext object, as are handy derived values for your tests to use to find test input or to place output file and logs. For example, logs_file_name is a member of SuiteContext, but so is log_file, which is the simple concatenation of users' home_dir (computed internally), logs_subdir, and logs_file_name. 
        
        These derivations are provided as a convenience. One could derive other values for custom purposes. 
        
        [DEFAULT]
        =========
        user_dir : string
        
        This is typically set to  ~/air_python, and it is the 'parent' directory of other settings including the tests_subdir and logs_subdir.
        
        If not provided, this will default to ~/air_python
        
        checkout_dir : string
        
        This is your local checkout workspace directory of the Mercurial python project.
        It is used as a basis for identifying your checkout workspace, under where it can find test buffer_ that you have checked into the project, somewhere under a 'test' directory. For example, where you have checked in some 'xls' files for testing your code.
        If you omit this setting from the file, context will guess based on the __file__ attribute of the airassessmentreporting module
        
        
        [TESTS] 
        =======
        
        default_server and default_database: string
        
        Identifies your test database. Using this may make it handy to use a single 'ini' file so we can store both a db server in the DB section for some environments and in the TESTS section for others.
        
        [LOGGING]
        =========
        level : string
        standard python logger - logging level value
        
        logs_subdir : string 
        Useful to use %(user_dir)s as the lead component.
        
        logs_file_name: string
        Just a file name, not a path name. 
        On init, member log_dir is created by prefixing this with the value in logs_subdir. If this variable is present, init() will construct a log file handler and tack it on to the standard logger in RunContext.        
        
        Returns
        -------
        A SuiteContext object initialized with your ini file contents.
        
        It is suitable for use in any function that takes a RunContext object by inserting a few lines of code at the top of your function like:
        
        Code sample
        +++++++++++
        
        
            # If context has member "conn" reuse the TEST db connection
            if (hasattr(context, "conn")):
                cnxn = context.conn
                context.info("export(): DB connection2 string='%s'" % context.cxs)
            else:
            
                # basic RunContext() here. Must connect to DB.
                now = str(datetime.now())
                context.info("export(): DB Connecting at time '%s':" % now)
                cnxn = context.getDBContext()._getDefaultConn()
                
        Notes:
        ======
        No checks are done here for validation or presence of variables in the ini file. 
        
        20130509 NOTES: (1) The current only way to open a non-default db  connection named in an ini file without using pyodbc is as follows, but lets assume we will discuss and revise this workflow.
        
        NOTE 2: getDBContext() now ignores the arguments server and db. It always,  values from the xxx.ini file [DB]default_server and [DB]default_db, but it does create a db context.
                
                dbc = getDBContext( self, server=None, db=None ):
                dbc.server = self.server
                dbc.db = self.db
                conn = dbc._getDefaultConn()
        
              
         end doc
         +++++++++++

        """
        super(SuiteContext,self).__init__(config_filename_prefix)
        self.server = self.getConfig('TESTS', 'default_server')
        self.db = self.getConfig('TESTS', 'default_database')
        
        """             
        # Pending a smoother DBcontext workflow, open the conn directly 
        # with ini params and pyodbc.
        """
        
        
        self.cxs = ('DRIVER={SQL Server};;SERVER=%s;'
            'dataBASE=%s;Trusted_connection=yes' % (self.server, self.db))
        self.conn = pyodbc.connect(self.cxs)
        
        # Register default values
        # A safe directory in which secure buffer_ may be placed
        self.tests_safe_dir = self.config.get('DEFAULT', 'tests_safe_dir')
        
        # Read the properties
        self.home_dir = self.getConfigFile('DEFAULT', 'home_dir')
        self.tests_dir = self.getConfigFile('TESTS', 'tests_dir')
        self.logs_dir = self.getConfigFile('LOGGING', 'logs_dir')
        if not os.path.exists( self.logs_dir ):
            os.makedirs( self.logs_dir )
        self.log_file_name = self.getConfig('LOGGING', 'log_file_name')
        self.log_file = os.path.join( self.logs_dir, self.log_file_name )
        if ( self.log_file ):
            fh = logging.FileHandler(self.log_file)
            fh.setLevel (logging.DEBUG)
            self.logger.addHandler(fh)         
                 
        self.tests_safe_dir = self.getConfigFile('DEFAULT','tests_safe_dir')
        self.user_dir = self.getConfigFile('DEFAULT', 'user_dir')
        self.checkout_dir = self.config.get('DEFAULT', 'checkout_dir')

def __del__(self):
    if self.conn:
        self.conn.close()
        
    
