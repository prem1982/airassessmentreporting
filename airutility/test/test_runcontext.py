'''
Created on Apr 26, 2013

@author: temp_dmenes
'''
import unittest
import os.path
import logging
import re

from airassessmentreporting.airutility import RunContext
from airassessmentreporting.testutility import ListHandler, get_list


_THIS_DIR = os.path.split( __file__ )[0]

class Test(unittest.TestCase):


    def setUp(self):
        self.saveDir = os.path.abspath('.')
        os.chdir( _THIS_DIR )
        self.OUT = RunContext( 'runcontexttest' )
        self.log_main = get_list( 'main' )
        del self.log_main[:]
        self.log_root = get_list( 'root' )
        del self.log_root[:]
        self.log_sql = get_list( 'sql' )
        del self.log_sql[:]
        

    def tearDown(self):
        os.chdir( self.saveDir )

    def test_config_read(self):
        config = self.OUT.config
        self.assertEqual( 'THIS IS NOT A REAL SERVER',
                      config.get( 'DB', 'default_server' ),
                      'Wrong default server value' )
        self.assertEqual( 'NOTADB',
                          config.get( 'DB', 'default_database'),
                          'Wrong default db value' )
        level = logging.getLevelName( self.OUT.get_logger().level )
        self.assertEqual( 'DEBUG',
                          level,
                          'Wrong logging level: found {}'.format( level ) )
    
    def test_logger_setup(self):
        logger = self.OUT.logger
        self.assertEquals( 'DEBUG',
                           logging.getLevelName( logger.level ),
                           'Wrong logging level on logger' )
        self.assertEqual( 'runcontexttest',
                          logger.name,
                          'Wrong logger name' )
        
    def test_logger_main1(self):
        self.OUT.debug( 'Message 1' )
        self.OUT.info( 'Message 2' )
        self.OUT.warning( 'Message 3' )
        self.OUT.error( 'This error is only a drill. In the event of a real error, the unit test would fail.' )
        expected_list = [
            r'DEBUG runcontexttest ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 1',
            r'INFO  runcontexttest ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 2',
            r'WARNING runcontexttest ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 3',
            r'ERROR runcontexttest ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: This error is only a drill\. In the event of a real error, the unit test would fail\.',
        ]
        
        for expected, actual in zip( expected_list, get_list( 'main' ) ):
            if not re.match(expected, actual):
                self.fail( "Expected message \"{}\", found \"{}\"".format( expected, actual ) )

    def test_logger_SQL1(self):
        logger = self.OUT.get_sql_logger()
        logger.debug( 'Message 1' )
        logger.info( 'Message 2' )
        logger.warning( 'Message 3' )
        logger.error( 'This error is only a drill. In the event of a real error, the unit test would fail.' )
        expected_list = [
            r'ERROR runcontexttest\.sql ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: This error is only a drill\. In the event of a real error, the unit test would fail\.',
        ]
        
        for expected, actual in zip( expected_list, get_list( 'sql' ) ):
            if not re.match(expected, actual):
                self.fail( "Expected message \"{}\", found \"{}\"".format( expected, actual ) )

    def test_logger_another(self):
        logger = self.OUT.get_logger( 'some.class' )
        self.assertEqual( 'runcontexttest.some.class', logger.name,
                          "Expected logger named runcontexttest.some.class, found {}".format( logger.name ) )
        logger.debug( 'Message 1' )
        logger.info( 'Message 2' )
        logger.warning( 'Message 3' )
        logger.error( 'This error is only a drill. In the event of a real error, the unit test would fail.' )
        expected_list = [
            r'DEBUG runcontexttest\.some\.class ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 1',
            r'INFO  runcontexttest\.some\.class ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 2',
            r'WARNING runcontexttest\.some\.class ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 3',
            r'ERROR runcontexttest\.some\.class ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: This error is only a drill\. In the event of a real error, the unit test would fail\.',
        ]
        
        for expected, actual in zip( expected_list, get_list( 'main' ) ):
            if not re.match(expected, actual):
                self.fail( "Expected message \"{}\", found \"{}\"".format( expected, actual ) )

    def test_logger_isolation(self):
        rc_independent = RunContext( 'independent' )
        self.OUT.info( "Message 1" )
        rc_independent.info( "Message 2" )
        expected_list = [
            r'INFO  runcontexttest ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 1',
        ]
        
        for expected, actual in zip( expected_list, get_list( 'main' ) ):
            if not re.match(expected, actual):
                self.fail( "Expected message \"{}\", found \"{}\"".format( expected, actual ) )

        expected_list = [
            r'INFO  independent ....-..-.. ..:..:..,... test_runcontext.py      \[...  ]: Message 2',
        ]
        
        for expected, actual in zip( expected_list, get_list( 'independent' ) ):
            if not re.match(expected, actual):
                self.fail( "Expected message \"{}\", found \"{}\"".format( expected, actual ) )
                
    def test_db_context_cache(self):
        rc = RunContext( 'unittest' )
        dbc1 = rc.getDBContext( 'unittest' )
        dbc2 = rc.getDBContext( 'unittest', 'second' )
        dbc3 = rc.getDBContext( 'unittest', cached=False )
        
        self.assertIsNot( dbc1, dbc2, 'Returned same dbContext in spite of different instance name' )
        self.assertIsNot( dbc1, dbc3, 'Returned same dbContext in spite of different instance name' )
        self.assertIsNot( dbc2, dbc3, 'Returned same dbContext in spite of different instance name' )
        
        self.assertIs( rc.getDBContext( 'unittest' ), dbc1, 'Did not return same dbContext' )
        self.assertIs( rc.getDBContext( 'unittest', 'second' ), dbc2, 'Did not return same dbContext' )
        self.assertIsNot( rc.getDBContext( 'unittest', cached=False ), dbc1, 'Returned same dbContext even though cached=False' )
        self.assertIsNot( rc.getDBContext( 'unittest', cached=False ), dbc2, 'Returned same dbContext even though cached=False' )
        self.assertIsNot( rc.getDBContext( 'unittest', cached=False ), dbc3, 'Returned same dbContext even though cached=False' )
        self.assertIsNot( rc.getDBContext( 'unittest', 'second', cached=False ), dbc1, 'Returned same dbContext even though cached=False' )
        self.assertIsNot( rc.getDBContext( 'unittest', 'second', cached=False ), dbc2, 'Returned same dbContext even though cached=False' )
        self.assertIsNot( rc.getDBContext( 'unittest', 'second', cached=False ), dbc3, 'Returned same dbContext even though cached=False' )

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()