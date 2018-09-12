'''
Created on Jun 26, 2013

@author: temp_dmenes
'''
import unittest
import cProfile
import pstats
import os

from airassessmentreporting.airutility import FieldSpec, TableSpec, drop_table_if_exists, get_temp_table, FastTableStream
from airassessmentreporting.airutility.fasttablestream_processors import NVarcharProcessor
from airassessmentreporting.testutility import XMLTest, SuiteContext

class TestFastTableStream( unittest.TestCase ):
    
    def setUp(self):
        
        self.run_context = SuiteContext( 'unittest' )
        self.db_context = self.run_context.getDBContext( 'unittest' )
        self.LOGGER = self.run_context.get_logger()
        
        # Set up a test table definition
        self.table = get_temp_table( self.db_context )
        ( self.table
            .add( FieldSpec( "col1", "NVARCHAR", 8 ) )
            .add( FieldSpec( "col2", "FLOAT" ) )
            .add( FieldSpec( "col3", "TINYINT" ) )
            .add( FieldSpec( "col4", "INT" ) )
            .add( FieldSpec( "col5", "BIGINT" ) )
        )
        
    def tearDown(self):
        self.table.drop()
        
    def runTest(self):
        pass

    def testWriteUnicodeCharacters(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'NVARCHAR', 8 ) )
            
        fts = FastTableStream( self.table, use_names=False )
        with fts:
            for j in xrange( 10000 ):
                fts.write( [ 100 * j + i for i in xrange( 100 ) ] )
        self.LOGGER.info( "Table name {}".format( self.table ) )
    
    def testWriteUnicodeMany(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'NVARCHAR', 8 ) )
            
        fts = FastTableStream( self.table, use_names=False )
        data = [ [ 100 * j + i for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
        
        def do_write( ):
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
        
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_unicode_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )

    def testWriteUnicodeRaw(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'NVARCHAR', 8 ) )
            
        fts = FastTableStream( self.table, use_names=False, raw=True )
        data = [ [ unicode( 100 * j + i ) for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
        
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
        
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_unicode_raw_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )

    
    def testWriteUnicodeNoNull(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'NVARCHAR', 8, nullable=False ) )
            
        fts = FastTableStream( self.table, use_names=False, raw=True )
        data = [ [ unicode( 100 * j + i ) for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
        
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
        
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_unicode_nonull_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )

    
    def testWriteCharacters(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'VARCHAR', 8 ) )
             
        fts = FastTableStream( self.table, use_names=False )
        with fts:
            for j in xrange( 10000 ):
                fts.write( [ str( 100 * j + i )[:8] for i in xrange( 100 ) ] )
        self.LOGGER.info( "Table name {}".format( self.table ) )
     
    def testWriteMany(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'VARCHAR', 8 ) )
             
        fts = FastTableStream( self.table, use_names=False )
        data = [ [ 100 * j + i for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
         
        def do_write( ):
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_char_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )
 
    def testWriteRaw(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'VARCHAR', 8 ) )
             
        fts = FastTableStream( self.table, use_names=False, raw=True )
        data = [ [ str( 100 * j + i ) for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
         
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_char_raw_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )
 
     
    def testWriteIntegerMany(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'INT' ) )
             
        fts = FastTableStream( self.table, use_names=False )
        data = [ [ 100 * j + i for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
         
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_int_with_checks_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )
 
    def testWriteIntegerRaw(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'INT' ) )
             
        fts = FastTableStream( self.table, use_names=False, raw=True )
        data = [ [ 100 * j + i for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
         
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_int_raw_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )
 
     
    def testWriteNoNull(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'VARCHAR', 8, nullable=False ) )
             
        fts = FastTableStream( self.table, use_names=False, raw=True )
        data = [ [ str( 100 * j + i ) for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
         
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_char_nonull_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )

    
    def testWriteFloatNoNull(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'FLOAT', nullable=False ) )
             
        fts = FastTableStream( self.table, use_names=False, raw=True, dumpfile="C:\\Scratch\\float_no_null.dat" )
        data = [ [ 100.0 * j + i for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
         
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_float_nonull_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )
 
     
    def testWriteFloatMany(self):
        del self.table[:]
        for i in xrange( 100 ):
            self.table.add( FieldSpec( "col_{}".format(i), 'FLOAT' ) )
             
        fts = FastTableStream( self.table, use_names=False, dumpfile="C:\\Scratch\\float.dat"  )
        data = [ [ 100.0 * j + i for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
         
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_float_nonull_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )
 
     
    def testWriteMixed(self):
        del self.table[:]
        for i in xrange( 33 ):
            self.table.add( FieldSpec( "float_{}".format(i), 'FLOAT' ) )
            self.table.add( FieldSpec( "int_{}".format(i), 'INT' ) )
            self.table.add( FieldSpec( "str_{}".format(i), 'VARCHAR', 6 ) )
             
        fts = FastTableStream( self.table, use_names=False )
        data = []
        for j in xrange( 1000 ):
            row = []
            for i in xrange( 33 ):
                k = 100 * j + i
                row.extend( ( int(k), float(k), str(k)  ) )
         
        def do_write():
            with fts:
                for j in xrange( 20 ):
                    fts.write_many( data )
        pr = cProfile.Profile()
        pr.enable()
        pr.runcall( do_write )
        pr.disable()
         
        filename = os.path.join( self.run_context.logs_dir, 'fast_table_write_mixed_profile.txt' )
        with open( filename, 'w' ) as f:
            stats = pstats.Stats( pr, stream=f )
            stats.print_stats()
        self.LOGGER.info( "Table name {}".format( self.table ) )
 
     
    def testValidateNewTable(self):
        drop_table_if_exists( self.table )
        table_stream = FastTableStream( self.table )
        table_stream.validate_write_inputs()
        
    def testValidateFailNewEmptyTable(self):
        drop_table_if_exists( self.table )
        del self.table[:]
        table_stream = FastTableStream( self.table )
        try:
            table_stream.validate_write_inputs()
        except ValueError:
            # Expected error
            return
        self.fail( "Expected a ValueError if called on to create a new table with no fields" )

    def testValidateExistingTable(self):
        drop_table_if_exists( self.table )
        self.db_context.executeNoResults( self.table.definition )
        table_stream = FastTableStream( self.table )
        table_stream.validate_write_inputs()
        
    def testValidateExistingTableAgainstEmptySpec(self):
        drop_table_if_exists( self.table )
        self.db_context.executeNoResults( self.table.definition )
        del self.table[:]
        table_stream = FastTableStream( self.table )
        table_stream.validate_write_inputs()
        
    def testValidateFailWrongColumnCount(self):
        drop_table_if_exists( self.table )
        self.db_context.executeNoResults( self.table.definition )
        self.table.add( FieldSpec( 'col7', 'NVARCHAR', 15 ) )
        table_stream = FastTableStream( self.table )
        try:
            table_stream.validate_write_inputs()
        except ValueError:
            # Expected error
            return
        self.fail( "Expected a ValueError if TableSpec has different column count from db" )

    def testValidateFailWrongColumnName(self):
        drop_table_if_exists( self.table )
        self.db_context.executeNoResults( self.table.definition )
        col1 = self.table.pop( 'col1' )
        col1.field_name = 'col_new'
        self.table.add( col1 )
        table_stream = FastTableStream( self.table )
        try:
            table_stream.validate_write_inputs()
        except ValueError:
            # Expected error
            return
        self.fail( "Expected a ValueError if column names do not match" )

    def testValidateFailWrongColumnType(self):
        drop_table_if_exists( self.table )
        self.db_context.executeNoResults( self.table.definition )
        self.table[ 'col1' ].basic_type = 'VARCHAR'
        table_stream = FastTableStream( self.table )
        try:
            table_stream.validate_write_inputs()
        except ValueError:
            # Expected error
            return
        self.fail( "Expected a ValueError if db column type is different from TableSpec column type" )

    def testValidateFailColumnTooShort(self):
        drop_table_if_exists( self.table )
        self.db_context.executeNoResults( self.table.definition )
        self.table[ 'col1' ].data_length = 100
        table_stream = FastTableStream( self.table )
        try:
            table_stream.validate_write_inputs()
        except ValueError:
            # Expected error
            return
        self.fail( "Expected a ValueError if db column is shorter than TableSpec column" )


    def testValidateColumnLonger(self):
        drop_table_if_exists( self.table )
        self.db_context.executeNoResults( self.table.definition )
        self.table[ 'col1' ].data_length = 1
        table_stream = FastTableStream( self.table )
        table_stream.validate_write_inputs()
        
class TestRemoteConnection( unittest.TestCase ):

    def test1(self):
        run_context = SuiteContext( 'unittest' )
        db_context = run_context.getDBContext( 'remote' )
        self.LOGGER = run_context.get_logger()
        
        # Set up a test table definition
        with get_temp_table( db_context ) as table:
            for i in xrange( 100 ):
                table.add( FieldSpec( "col_{}".format(i), 'NVARCHAR', 8 ) )
                
            fts = FastTableStream( table, use_names=False, raw=True )
            data = [ [ unicode( 100 * j + i ) for i in xrange( 100 ) ] for j in xrange( 1000 ) ]
            
            def do_write():
                with fts:
                    for j in xrange( 5 ):
                        fts.write_many( data )
            pr = cProfile.Profile()
            pr.enable()
            pr.runcall( do_write )
            pr.disable()
            
            filename = os.path.join( run_context.logs_dir, 'fast_table_write_remote_unicode_raw_profile.txt' )
            with open( filename, 'w' ) as f:
                stats = pstats.Stats( pr, stream=f )
                stats.print_stats()
            self.LOGGER.info( "Table name {}".format( table ) )

class TestNVarcharProcessor( XMLTest ):
    
    def setUp(self):
        # Set up a field to define
        table = TableSpec( "my_table", None )
        self.field = FieldSpec( "col1", "NVARCHAR", 8 )
        table.add( self.field )
        
        # Set up the type processor
        self.proc = NVarcharProcessor( 1, self.field, False )

    def test_write_unicode( self ):
        row = { 1: u'abcdefgh' }
        buffer_ = bytearray( 20 )
        ptr = 0
        ptr = self.proc.process( row, buffer_, ptr )
        self.assertEquals( ptr, 18 )
        self.assertEquals( buffer_[0:2], bytearray( ( 16, 0, ) ) )
        self.assertEquals( buffer_[2:18], bytearray( u'abcdefgh', 'UTF-16LE' ) )

    def test_write_string( self ):
        row = { 1: 'abcdefgh' }
        buffer_ = bytearray( 20 )
        ptr = 0
        ptr = self.proc.process( row, buffer_, ptr )
        self.assertEquals( ptr, 18 )
        self.assertEquals( buffer_[0:2], bytearray( ( 16, 0 ) ) )
        self.assertEquals( buffer_[2:18], b'a\0b\0c\0d\0e\0f\0g\0h\0' )

    def test_write_unicode_too_long( self ):
        row = { 1: u'abcdefghi' }
        buffer_ = bytearray( 20 )
        ptr = 0
        try:
            ptr = self.proc.process( row, buffer_, ptr )
        except ValueError:
            return
        self.fail( "Should have raised ValueError for string too long" )

    def test_write_integer( self ):
        row = { 1: 1234 }
        buffer_ = bytearray( 20 )
        ptr = 0
        ptr = self.proc.process( row, buffer_, ptr )
        self.assertEquals( ptr, 10 )
        self.assertEquals( buffer_[0:2], bytearray( ( 8, 0, ) ) )
        self.assertEquals( buffer_[2:10], b'1\x002\x003\x004\x00' )

    def test_write_none( self ):
        row = { 1: None }
        buffer_ = bytearray( 20 )
        ptr = 0
        ptr = self.proc.process( row, buffer_, ptr )
        self.assertEquals( ptr, 2 )
        self.assertEquals( buffer_[0:2], b'\xff\xff' )

    
    