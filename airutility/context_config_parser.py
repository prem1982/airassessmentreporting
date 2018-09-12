'''
Created on May 31, 2013

@author: temp_dmenes
'''

from ConfigParser import SafeConfigParser, DEFAULTSECT, _Chainmap, NoSectionError

class ContextConfigParser( SafeConfigParser ):
    '''
    Modified config file parser for RunContext objects
    
    This class adds to SafeConfigParser additional methods for expanding macros in strings using the values in the config file
    '''

    def interpolate( self, s, section=DEFAULTSECT, name="<variable>" ):
        """Interpolates config variable references in the string
        
        Any reference of the form :samp:`%({variable-name})s` will be replaced with the appropriate value form the configuration
        file.
        
        :param s: The string into which values should be interpolated
        :type s: str
        :returns: str -- The value of `s` after interpolation
        """
        ### This code is copied wholesale from the ConfigParser class. It makes use of that class's non-public
        ### _interpolate() method
        sectiondict = {}
        try:
            sectiondict = self._sections[section]
        except KeyError:
            pass
            # By doing nothing, we will interpolate from the defaults
            
        # Update with the entry specific variables
        d = _Chainmap(sectiondict, self._defaults)
        name = self.optionxform( name )
        if s is None:
            return s
        else:
            return self._interpolate(section, name, s, d)

    