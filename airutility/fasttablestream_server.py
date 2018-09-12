'''
Created on Jun 26, 2013

@author: temp_dmenes
'''
import os
from threading import Thread, Semaphore
import traceback
import win32pipe, win32file
import uuid
import socket

MAX_THREADS = 2

def get_pipe_name( ext ):
    base_name = str( uuid.uuid4() )
    return ( "\\\\" + socket.gethostname() + "\\pipe\\" + base_name + "." + ext,
             "\\\\.\\pipe\\" + base_name + "." + ext )

class WriterContext( object ):
    
    def __init__( self, run_context, buffer_, n_to_write, dumpfile ):
        self.logger = run_context.get_logger( 'fasttablestream' )
        self.buffer = buffer_
        self.data_pipe_name, self.local_data_pipe_name = get_pipe_name( "dat" )
        self.n_to_write = n_to_write
        self.dumpfile = dumpfile
        
    def __enter__( self ):
        
        # We start multiple identical threads to feed the buffer into the pipe. At least two threads
        # are needed because SQL server opens the file, reads a few bytes, and closes it again, before
        # opening it "for real" to read the entire buffer. When the file is closed the first time,
        # that instance of the Windows pipe is destroyed.  We keep a second pipe waiting in the wings
        # so that we don't have a race with SQL Server to reinitialize the pipe before SQL Server tries
        # to read from it.
        #
        # Sometimes we create a third thread in testing, in order to make sure that leftover pipes are
        # properly destroyed
        self.data_servers = []
        for i in range( MAX_THREADS ):
            thread_i = WriterThread( self.logger, self.local_data_pipe_name, self.buffer, self.n_to_write,
                                     "Data pipe {}".format( i+1 ), None )
            self.data_servers.append( thread_i )
            thread_i.start()
            
            # To avoid a race condition, wait until the server thread is actually running before we go on.
            thread_i.semaphore.acquire()

    def __exit__( self, exc_class, exc_value, tb ):
        
        # Consume any pipes served by threads that are still running.
        try:
            while True:
                # When we've used up all of the threads, the pipe will no longer exist. At that
                # point, open() will throw an error, which we use to exit the loop.
                with open( self.data_pipe_name, 'r' ) as fh:
                    fh.read(1)
        except:
            pass
        
        del self.buffer
        if not any( thread_i.all_written for thread_i in self.data_servers ):
            if exc_class is None:
                raise IOError( "Failed to write entire contents of buffer to pipe!" )
            else:
                self.logger.error( 'Failure to write entire contents of buffer to pipe probably related to underlying error' )
        del self.data_servers[:]


class WriterThread( Thread ):
    
    def __init__( self, logger, pipe_name, buffer_, n_to_write, name, dumpfile ):
        try:
            super( WriterThread, self ).__init__(name=name)
            self.logger = logger
            self.logger.debug( "Initializing server thread {}".format( name ) )
            self.pipe_name = pipe_name
            self.view = buffer( buffer_, 0, n_to_write )
            self.all_written = False
            self.semaphore = Semaphore( 1 )
            self.semaphore.acquire()
            self.dumpfile = dumpfile
        except:
            self.logger.error( traceback.format_exc() )

    def run(self):
        try:
            self.logger.debug( "{} creating pipe".format( self.name ) )
            p = win32pipe.CreateNamedPipe( self.pipe_name,
                    win32pipe.PIPE_ACCESS_DUPLEX | win32file.FILE_FLAG_WRITE_THROUGH,
                    win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_WAIT,
                    MAX_THREADS, 0, 0, 30, None )
            try:
                self.logger.debug("{} connecting to pipe".format( self.name ) )
                self.semaphore.release()
                win32pipe.ConnectNamedPipe( p, None )
                self.logger.debug("{} Writing buffer to pipe".format( self.name ) )
                win32file.WriteFile( p, self.view )
                self.all_written = True
            finally:
                win32file.CloseHandle(p)
        except:
            self.logger.debug("{} got exception".format( self.name ) )
            self.logger.debug( traceback.format_exc() )
        finally:
            self.logger.debug("{} leaving".format( self.name ) )
        if self.dumpfile is not None:
            with open( self.dumpfile, "a" ) as f:
                f.write( self.view )
    