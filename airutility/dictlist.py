class DictList( object ):
    """A dictionary which remembers the insert order AND has fancy indexing.
    
    Similarly to a :class:`collections.OrderedDict`, the DictList remembers the order in which
    items have been added. On top of that, the items can be accessed by three different methods:
    a name, a numeric index, or via properties of the :class:`DictList` that match the item names.
    
    Item access and deletion using slice notation is supported.
    
    The ``in`` operator works as it does on a :class:`dict`: it checks if the supplied argument
    appears in the keys.
    
    By default, you cannot use numeric or slice indices on the left side of an assignemnt, as the
    class will not know what name to assign to the inserted values.  However, if a subclass of :class:`DictList`
    implements the :meth:`_get_name` method, then numeric and slice indices can be used on the left
    side of an assignment.
    
    Subclasses can further refine the behavior of the class by implementing :meth:`_set_ordinal`,
    :meth:`_normalize_name`, :meth:`_normalize_property`, :meth:`_connect` and :meth:`_disconnect` methods.
    """
    def __init__( self ):
        self._dict={}
        self._list=[]

    @staticmethod
    def _set_ordinal( x, i ):
        """Set an ordinal position property for a newly added member
        
        Subclasses may implement this method. It is called whenever a new member is added in order
        to set a property representing the member's ordinal position in the list. The default implementation
        does nothing.
        """
        pass
    
    @staticmethod
    def _get_name( x ):
        """Retrieve the value that should be the name for a member.
        
        Called whenever a member is inserted by assigning to an index, if the index value is a number
        or a slice. It must return a name to use for this member. The name must not be shared by
        any other members of the :class:`DictList` (except a member that is being replaced).
        
        The default implementation raises a :except:`KeyError`, indicating that numeric and slice
        indices cannot be used on the left side of an assignment
        """
        raise KeyError( "In order to insert using numeric indices, you must implement _get_name()" )
    
    @staticmethod
    def _normalize_name( x ):
        """Called to normalize a name before adding an item to the collection.
        
        Note that _normalize_name is only called on names that are provided as indices. It is `not`
        called on names returned by :meth:`_get_name`. If you implement :meth:`_get_name`, you must
        do any required normalization within that method.
        
        :meth:`_normalize_name` will be called for any object that is not an integer or a slice.
        The method can therefore be used to convert non-string objects into strings
        
        The default implementation returns the name unchanged.
        """
        return x
    
    @staticmethod
    def _normalize_property( x ):
        """Called to normalize a property name before adding it to the class dictionary.
        
        Unlike :meth:`_normalize_name`, this method is called whenever :meth:`__setitem__` updates
        a property--even if the name was retrieved from the object by :meth:`_get_name`.
        
        The default implementation returns the name unchanged.
        """
        return x
    
    def _connect( self, x ):
        """Called whenever an item is added, to inform the new member that it is now a child of the
        :class:`DictList`
        
        The default implementation does nothing.
        """
        pass
    
    def _disconnect( self, x ):
        """Called whenever an item is removed, to inform the item that it is no longer a child of the
        :class:`DictList`
        
        The default implementation does nothing.
        """
        pass
    
    def keys( self ):
        '''The usual :meth:`keys` method for mappings'''
        return [ x[0] for x in self._list ]

    def values( self ):
        '''The usual :meth:`values` method for mappings'''
        return [ x[1] for x in self._list ]

    def items( self ):
        '''The usual :meth:`items` method for mappings'''
        return self._list[:]

    def iterkeys( self ):
        '''The usual :meth:`iterkeys` method for mappings'''
        for x in self._list:
            yield x[0]

    def itervalues( self ):
        '''The usual :meth:`itervalues` method for mappings'''
        for x in self._list:
            yield x[1]

    def iteritems( self ):
        '''The usual :meth:`iteritems` method for mappings'''
        for x in self._list:
            yield x
    
    def has_key( self, name ):
        '''The usual :meth:`has_key` method for mappings'''
        return name in self

    def get( self, name, default=None ):
        '''The usual :meth:`get` method for mappings'''
        if name in self:
            return self[ name ]
        return default

    def clear( self ):
        '''The usual :meth:`clear` method for mappings'''
        for item in self._list:
            self._disconnect( item[1] )
        self._dict.clear()
        del self._list[:]

    def copy( self ):
        '''We haven't implemented copy. A shallow copy won't work for :class:`TableSpec`, and we would need
        to know about a clone method in order to implement a deep copy properly.'''
        raise NotImplementedError

    def fromkeys( self ):
        '''We haven't implemented fromkeys. Lots of circumstances where it won't work right because of key conflicts'''
        raise NotImplementedError

    def pop( self, idx=None, *args ):
        '''When called with a string or unicode index, behave like :meth:`dict.pop`. When called with an integer
        index or no index, behaves like :meth:`list.pop`
        '''
        if idx is None or isinstance( idx, int ):
            return self.popitem(idx)[1]
        else:
            idx = self._normalize_name( idx )
            try:
                i, value = self._dict.pop( idx )
            except KeyError as e:
                if len( args ) > 0:
                    return args[0]
                raise e
            del self._list[i]
            self._disconnect( value )
            self._fixup_ordinals()
            return value

    def popitem( self, i=None ):
        '''When called with no index, behave like :meth:`dict.popitem`. When called with an integer
        index it returns the key-value pair with the designated ordinal position.
        '''
        if i is None:
            item = self._list.pop()
        else:
            item = self._list.pop( i )
        del self._dict[ item[0] ]
        del self.__dict__[ self._normalize_property( item[0] ) ]
        self._disconnect( item[1] )
        self._fixup_ordinals
        return item

    def update( self, m ):
        '''The usual :meth:`update` method for mappings'''
        for name, value in m.iteritems():
            self[name] = value
            
    def append( self, value ):
        '''The usual :meth:`append` method for mutable sequences'''
        name = self._get_name( value )
        if name in self._dict:
            raise KeyError( "Appending item would create duplicate name" )
        self[ name ] = value
        
    def count( self, value ):
        '''The usual :meth:`count` method for sequences'''
        self.values().count( value )
        
    def index( self, *args ):
        '''The usual :meth:`index` method for sequences'''
        return self.values().index( *args )
    
    def extend( self, values ):
        '''The usual :meth:`extend` method for mutable sequences'''
        n = len( self._list )
        self[ n:n ] = values
        
    def insert( self, i, value ):
        '''The usual :meth:`insert` method for mutable sequences'''
        self[ i:i ] = (value,)
        
    def remove( self, value ):
        '''The usual :meth:`remove` method for mutable sequences'''
        for name, x in self._list:
            if value == x:
                del self[ name ]
                break
            
    def reverse( self ):
        '''The usual :meth:`reverse` method for mutable sequences'''
        self._list.reverse()
        self.update_ordinals()
    
    def sort( self, cmp=None, key=None, reverse=False ):
        '''The usual :meth:`sort` method for mutable sequences'''
        if key is None:
            my_key = lambda x : x[1]
        else:
            my_key = lambda x : key( x[ 1 ] )
        self._list.sort( cmp, my_key, reverse )
        self.update_ordinals()

    def __iadd__( self, what ):
        self.extend( what )

    def __getitem__( self, idx ):
        if isinstance( idx, slice ):
            return [ x[1] for x in self._list[idx] ]
        elif isinstance( idx, int ):
            return self._list[ idx ][ 1 ]
        else:
            idx = self._normalize_name( idx )
            return self._dict[ idx ][ 1 ]
        
    def __delitem__( self, idx ):
        if isinstance( idx, slice ):
            items = self._list[ idx ]
            del self._list[ idx ]
            for name, value in items:
                del self._dict[ name ]
                del self.__dict__[ self._normalize_property( name ) ]
                self._disconnect( value )
        elif isinstance( idx, int ):
            name, value = self._list[ idx ]
            del self._dict[ name ]
            del self.__dict__[ self._normalize_property( name ) ]
            del self._list[ idx ]
            self._disconnect( value )
        else:
            idx = self._normalize_name( idx )
            i, value = self._dict[ idx ]
            del self._dict[ idx ]
            del self.__dict__[ self._normalize_property( idx ) ]
            del self._list[ i ]
            self._disconnect( value )
        self._fixup_ordinals()

    def __setitem__( self, idx, value ):
        if isinstance( idx, slice ):
            # Check that we won't create any duplicate names!
            names = [ x[0] for x in self._list ]
            del names[ idx ]
            new_items = [ ( self._get_name( x ), x ) for x in value ]
            for item in new_items:
                name = item[ 0 ]
                if name in names:
                    raise KeyError( "Slice insert would create entry with duplicate name" )
                names.add( name )
            # Remove the old items
            for name, value in self._list[ idx ]:
                self._disconnect( value )
                del self._dict[ name ]
                del self.__dict__[ self._normalize_property( name ) ]
            # Do the insert
            self._list[ idx ] = new_items
            for name, value in new_items:
                self._connect( value )
                self._dict[ name ] = ( 0, value )
                self.__dict__[ self._normalize_property( name ) ] = value
            self._fixup_ordinals()
        elif isinstance( idx, int ):
            # Check for name conflicts
            new_name = self._get_name( value )
            try:
                i, some_value = self._dict[ new_name ]
                if i != idx:
                    raise KeyError( "Insert at index would create entry with duplicate name" )
            except KeyError:
                # Great, the name doesn't exist, so we can't have a key error!
                pass
            # Remove the old value
            old_name, old_value = self._list[ idx ]
            del self._dict[ old_name ]
            del self.__dict__[ self._normalize_property( old_name ) ]
            self._disconnect( old_value )
            # Connect the new value
            self._list[ idx ] = ( new_name, value )
            self._dict[ new_name ] = ( idx, value )
            self.__dict__[ self._normalize_property( new_name ) ] = value
            self._connect( value )
            self._set_ordinal( value, idx )
        else:
            idx = self._normalize_name( idx )
            if idx in self._dict:
                i, old_value = self._dict[ idx ]
                self._disconnect( old_value )
                self._dict[ idx ] = ( i, value )
                self.__dict__[ self._normalize_property( idx ) ] = value
                self._list[ i ] = ( idx, value )
                self._set_ordinal( value, i )
                self._connect( value )
            else:
                i = len( self._list )
                self._list.append( ( idx, value ) )
                self._dict[ idx ] = ( i, value )
                self.__dict__[ self._normalize_property( idx ) ] = value
                self._set_ordinal( value, i )
                self._connect( value )
    
    def __len__( self ):
        return len( self._list )

    def __iter__( self ):
        return self.itervalues()

    def __contains__( self, item ):
        item = self._normalize_name( item )
        return item in self._dict
    
    
    def _fixup_ordinals( self ):
        i = 0
        for ( name, value ) in self._list:
            self._set_ordinal( value, i )
            self._dict[ name ] = ( i, value )
            i += 1
