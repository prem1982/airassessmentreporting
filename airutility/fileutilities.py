'''
Created on May 8, 2013

@author: temp_plakshmanan
'''
"""This module will contains all utilities related to files"""
import os
import string

def external_file_check(filename=''):
    """This module will check if a file is available or not a"""
    filename = filename.strip('\'"' + string.whitespace)
    if not os.access(filename, os.F_OK):
        raise IOError("File not found: {filename}".format(filename=filename))
    if not os.access(filename, os.R_OK):
        raise IOError("Cannot read file: {filename}".format(filename=filename))
        
class FileTransformer( object ):
    """Transform the contents of a file by expanding %-style format expressions
    
    Parameters
    ==========
    filename:
        Name of a file to open
        
    values: dict or tuple
        Value that will be used to expand %-style format expressions in each line of file
    """
    
    def __init__( self, filename, values ):
        self.values = values
        self.fp = open( filename, 'r' )
        
    def readline( self, size=-1 ):
        line = self.fp.readline()
        if not line:
            return line
        return line % self.values
            
        












# external_file_check(filename='C:\CVS Projects\CSSC Score Reporting\OGT Spring 2012\Input Layout\OGT_SP12_Op_DataLayout_IntakeLayout1.xls')    