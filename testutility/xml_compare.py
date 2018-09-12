'''
Created on Jun 26, 2013

@author: temp_dmenes
'''

import unittest

__all__ = ['XMLTest']

class XMLTest( unittest.TestCase ):
    
    def assertXMLEquals( self, xml1, xml2, msg='XML not equal', path='/' ):
        
        # Tag names must be equal
        self.assertEquals( xml1.tag, xml2.tag,
                '{msg}\nDifferent tags found at [{path}]: <{xml1.tag}> and <{xml2.tag}>'.format( **locals() ) )
        
        # Attribute dictionaries must match
        for key, value in xml1.items():
            self.assertTrue( key in xml2.attrib,
                    '{msg}\nMissing attribute at [{path}] on element <{xml1.tag}>: @{key} found only on left'.format( **locals() ) )
            value2 = xml2.attrib[ key ]
            self.assertEquals( value, value2,
                    '{msg}\nUnequal values at [{path}] on element <{xml1.tag}> for attribute @{key}: "{value}" and "{value2}"'.format( **locals() ) )
        for key in xml2.keys():
            self.assertTrue( key in xml1.attrib,
                    '{msg}\nMissing attribute at [{path}] on element <{xml1.tag}>: @{key} found only on right'.format( **locals() ) )
        
        # Text following start tag must match
        if xml1.text is None:
            text1 = ''
        else:
            text1 = " ".join( xml1.text.split() )
        if xml2.text is None:
            text2 = ''
        else:
            text2 = " ".join( xml2.text.split() )
        if text1 and not text2:
            self.fail( '{msg}\nMissing text at [{path}] on element <{xml1.tag}>: Text "{text1}" found after start element on left only'.format( **locals() ) )
        if text2 and not text1:
            self.fail( '{msg}\nMissing text at [{path}] on element <{xml1.tag}>: Text "{text2}" found after start element on right only'.format( **locals() ) )
        self.assertEquals( text1, text2,
                '{msg}\nUnequal text after start tag at [{path}] on element <{xml1.tag}>: "{text1}" and "{text2}"'.format( **locals() ) )
        
        # Text following end tag must match
        if xml1.tail is None:
            tail1 = ''
        else:
            tail1 = " ".join( xml1.tail.split() )
        if xml2.tail is None:
            tail2 = ''
        else:
            tail2 = " ".join( xml2.tail.split() )
        if tail1 and not tail2:
            self.fail( '{msg}\nMissing text at [{path}] after element <{xml1.tag}>: Text "{tail1}" found on left only'.format( **locals() ) )
        if tail2 and not tail1:
            self.fail( '{msg}\nMissing text at [{path}] after element <{xml1.tag}>: Text "{tail2}" found on right only'.format( **locals() ) )
        self.assertEquals( tail1, tail2,
                '{msg}\nUnequal text at [{path}] after element <{xml1.tag}>: "{tail1}" and "{tail2}"'.format( **locals() ) )
        
        # Children must match by the same rules
        n1 = len( xml1 )
        n2 = len( xml2 )
        self.assertEquals( n1, n2,
                '{msg}\nUnequal numbers of children at [{path}] on element <{xml1.tag}>: {n1} and {n2}'.format( **locals() ) )
        child_path = '/'.join( ( path, xml1.tag ) )
        for child1, child2 in zip( xml1, xml2 ):
            self.assertXMLEquals( child1, child2, msg, child_path )
        
