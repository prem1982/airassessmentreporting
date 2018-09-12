'''
These are utilities for manipulating COM objects--especially MS Excel
'''
import os

import win32com.client

__all__=['get_workbook_from_file']

def get_workbook_from_file( filename, excel = None ):
    '''Return the Workbook object corresponding to the given file, opening
    it if necessary
    
    Parameters
    ----------
    filename : str
        Name of file to open
        
    excel : Excel.Application
        Optional instance of Excel com object. If not provided, one will be
        retrieved from win32com.client
        
    Returns
    -------
    ( Workbook workbook, bool was_closed )
    
    Raises
    ------
    IOError
        if file was not found or could not be read
        
    COMError
        if some problem was encountered running Excel
    
    '''
    if not os.path.isfile( filename ):
        raise IOError("File not found: {}".format( filename ))
    if not os.access(filename, os.R_OK):
        raise IOError("Cannot read file: {}".format())

    # Get a handle to Excel
    # self.excel = win32com.client.GetObject(Class="Excel.Application")
    if excel is None:
        excel = win32com.client.Dispatch('Excel.Application')
    
    # Find the open worksheet, or open it (read-only) if it is not yet open
    found = None
    was_closed = True
    looking_for = os.path.realpath( filename ).upper()
    for workbook in excel.Workbooks:
        if looking_for == os.path.realpath( workbook.FullName ).upper():
            found = workbook
            was_closed = False
            break
        
    if found is None:
        found = excel.Workbooks.open( looking_for, False, True )
        was_closed = True
    
    return ( found, was_closed, excel )
    