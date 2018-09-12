'''
Created on Jun 27, 2013

@author: temp_dmenes
'''

import sys

import tablespec
import dbutilities

__all__ = [ 'TempTable' ]

class TempTable( tablespec.TableSpec ):
    """ A :class:`TableSpec` that deletes itself when it goes out of scope.
    
        Most of the interface is identical to :class:`TableSpec`, and is not
        repeated here.
        
        :class:`TempTable` objects can be used as context managers with
        :ref:`with`\ . The :meth:`drop` method will be called when
        the context exits.
        
        .. attribute:: keep
        
            (`bool`) If :const:`False`, drop the underlying table when the object
            is no longer being used. Defaults to :const:`False`
    """
    def __init__( self, *args, **kwargs ):
        super( TempTable, self ).__init__( *args, **kwargs )
        self.keep = False
        
    def __del__( self ):
        self.drop()
    
    def __enter__( self ):
        return self
    
    def __exit__( self, exc_class, exc_value, tb ):
        self.drop()

    def drop(self):
        """ (Usually) drop the database table.
        
            The table will not be dropped if the :attr:`keep` attribute is true, if
            an exception is pending at the time of the call, or if the
            file :file:`{context}.ini` has the option ``delete_temp_files`` in the
            section ``[MISC]`` set to any value other than ``TRUE``
        """
        if self.keep:
            return
        if sys.exc_info()[0] is not None:
            return
        if self.db_context is None:
            return
        rc = self.db_context.runContext
        if rc is None:
            return
        delete_files = rc.getConfig( 'MISC', 'delete_temp_files', 'TRUE' ).upper()
        if delete_files == 'TRUE':
            rc.get_logger().debug( 'Dropping temp table {:qualified}'.format( self ) )
            dbutilities.drop_table_if_exists( self )
