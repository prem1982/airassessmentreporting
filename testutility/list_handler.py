from logging import Handler

__all__ = [ 'ListHandler', 'get_list' ]

_LISTS = {}

class ListHandler( Handler ):
    """A simple log handler for testing
    
    This adds each emitted message to a list of messages
    
    Not meant for production. No effort made at thread safety
    
    Parameters
    ==========
    name : str
        A name by which the list can be retrieved
    """
    def __init__( self, name ):
        super( ListHandler, self ).__init__()
        if name in _LISTS:
            self.log_list = _LISTS[ name ]
        else:
            self.log_list = []
            _LISTS[ name ] = self.log_list
        
    def emit( self, record ):
        msg = self.format(record)
        self.log_list.append( msg )

def get_list( name ):
    """Get a list of logged messages
    
    Parameters
    ==========
    name : str
        The name used when creating the list handler
    """
    return _LISTS[ name ]
