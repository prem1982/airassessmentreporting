'''
Created on May 31, 2013

@author: temp_dmenes
'''

import unittest

from airassessmentreporting.airutility import FieldSpec

class FieldSpecTest( unittest.TestCase ):
    def test_identity(self):
        o_u_t = FieldSpec('my_field', 'int', identity=(1,1) )
        self.assertEqual( o_u_t.definition, '[my_field] INT IDENTITY(1,1)', "Did not get correct field definiton" )


