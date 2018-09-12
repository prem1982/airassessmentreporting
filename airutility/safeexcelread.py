'''Import an Excel spreadsheet into an MS-SQL table

@author Daniel Menes <temp_dmenes@air.org>
@author Zachary Schroeder <zscroeder@air.org>
'''

import os.path
import string
import time
import datetime
import re
from numbers import Number

import win32api
import win32com.client
from win32com.client import pywintypes
import pythoncom

from comutilities import get_workbook_from_file
from dbutilities import ( drop_table_if_exists, get_table_spec, Row )
from formatutilities import Joiner, db_identifier_unquote, db_identifier_quote
from tablespec import FieldSpec,TableSpec
from dictlist import DictList


__all__ = [ 'XLSX_FILE_TYPE', 'XLSB_FILE_TYPE', 'XLSM_FILE_TYPE', 'XLS_FILE_TYPE', 'CSV_FILE_TYPE', 'PROVIDER',
            'SafeExcelReader' ]

XLSX_FILE_TYPE = "Excel 12.0 Xml"
XLSB_FILE_TYPE = "Excel 12.0"
XLSM_FILE_TYPE = "Excel 12.0 Macro"
XLS_FILE_TYPE = "Excel 8.0"
CSV_FILE_TYPE = "Text;FMT=Delimited({delimiter})"
PROVIDER = "Microsoft.ACE.OLEDB.12.0"
_CONN_STR_PATTERN = "Data Source={srcName};Extended Properties=\"{properties}\""
_CREATE_ID_QUERY_PATTERN = "ALTER TABLE {outputTable:qualified} ADD {importOrder} INT IDENTITY PRIMARY KEY"
_POPULATE_TABLE_QUERY_PATTERN = "INSERT INTO {outputTable:qualified} SELECT * FROM OPENDATASOURCE('{provider}','{srcConnStr}')...[{sheetName}]"
_DELETE_MISSING_QUERY_PATTERN = """DELETE FROM {outputTable:qualified} WHERE {columnNames: " AND ", itemfmt="(ISNULL({{}},'') = '')" }"""
_INSERT_INTO_TABLE_QUERY = "INSERT INTO {outputTable:qualified} ({fields}) VALUES ({fields:',',itemfmt='?'})"
_ADD_PK_QUERY = "ALTER TABLE {outputTable:qualified} ADD PRIMARY KEY ({importOrder})"
_INT_TYPES = [int,float]
_STR_TYPES = [str,unicode]
_DATE_TYPES = [datetime,type(pywintypes.Time(time.time()))] # ignore error, function Time() does exist
        
class SafeExcelReader( object ):
    '''Class implementing "safe" Excel reading strategy
    
    The functionality is designed to be as close as possible to the SAS macro SafeExcelImport.
    
    Properties
    ----------

    runContext : RunContext
        The run context to use for logging
        
    filename : str
        Name of Excel file or text file to import
        
    sheetName : str or None or int
        Name or index of Excel sheet to import
        
    outputTable : str
        Name of table to create or :class:`TableSpec` object defining table to
        create.
        
    db_context : DBContext
        A db_context object representing a database connection on which to
        create the table

    getNames : boolean
        If true, the spreadsheet begins with a row of column names. If false,
        invented column names will be used.
        
    delimieter : str
        Delimiter character to use when importing text files
        
    importOrder : str
        Name of an integer column that will be added to the table to indicate
        the order in which the rows appeared in the source document. Defaults
        to [import_order].
            
    provider : str
        Name of the ODBC provider to use. Defaults to "Microsoft.ACE.OLEDB.12.0"
        
    skip : int
        Number of rows to skip at beginning of file. This is only supported
        with the getRows method--don't try it with createTable
    
    buffer_size : int
        Number of rows to hold in the memory in buffer at a time in the _getValues() function
        
    scan_all : bool
        True if we should scan the entire file to get the type of a column, or False if we should just scan
        the number of rows in buffer_size. Default is False.
    
    range : 4-tuple (row_start,column_start,row_end,column_end) or a String range "A1:M15"
        Optional. This can be a 4-tuple or a string representation of a range in excel. This should describe the 
        rectangle of cells to get from the spreadsheet. These values should be 0-based. These values are inclusive. 
        If None is specified then it gets the entire sheet. Default is None.
    '''
    # We are not now implementing err_var_name
    
    def __init__( self, run_context, filename=None, sheet_name=None, output_table=None, db_context=None,
                  get_names=True, delimiter=',', import_order='import_order', skip=0, buffer_size=1000,
                  scan_all=False, range_=None):
        '''Constructor
        
        Parameters
        ==========
        
        run_context : RunContext
            Required. A run context, used mainly for logging.
            
        filename : str
            Name of table to create. If not specified to the constructor, must be set before calling "createTable()"
            
        sheet_name : str or None or int
            Name of worksheet to import. This must be a string name of the worksheet, None to get the first worksheet by default, 
            or an integer index of a worksheet. Default is None. 
            
        output_table : str or TableSpec object
            Name of table to create. Table will be dropped first if it already exists. If not specified to the
            constructor, must be set before calling "createTable()"
            
        db_context : DBContext
            Database connection to be used for transaction
            
        get_names : boolean
            If true, the spreadsheet begins with a row of column names. If false, invented column names will
            be used.
            
        delimieter : str
            Delimiter character to use when importing text files
            
        import_order : str
            Name of an integer column that will be added to the table to indicate the order in which the rows appeared
            in the source document. Defaults to [import_order].
            
        skip : int
            Number of rows to skip at beginning of file. This is only supported with the
            getRows method--don't try it with createTable
            
        buffer_size : int
            Number of rows to hold in the memory in buffer at a time in the _getValues() function
            
        scan_all : bool
            True if we should scan the entire file to get the type of a column, or False if we should just scan
            the number of rows in buffer_size. Default is False.
        
        range_ : 4-tuple (row_start,column_start,row_end,column_end) or a String range "A1:M15"
            Optional. This can be a 4-tuple or a string representation of a range in excel. This should describe the 
            rectangle of cells to get from the spreadsheet. These values should be 0-based. These values are inclusive. 
            If None is specified then it gets the entire sheet. Default is None.
        '''
        
        self.runContext = run_context
        self.filename = filename
        self.sheetName = sheet_name
        self.outputTable = output_table
        self.db_context = db_context
        self.getNames = get_names
        self.delimiter = delimiter
        self.importOrder = import_order
        self.excel = None
        self.skip = skip
        self.buffer_size = buffer_size
        self.scan_all = scan_all
        self.range = range_
        
    def _import_text( self ):
        """ This function imports a delimited .txt file into excel.
        """
        excel = win32com.client.Dispatch('Excel.Application')
        wb = excel.Workbooks.add
        self.to_close = True
        sheet = wb.Sheets[0]
        sheet.QueryTables.Add( "TEXT;" + os.path.realpath( self.filename ),sheet.Range( "A1" ) );
        sheet.QueryTables[0].TextFileCommaDelimiter = (self.delimiter == ',') 
        sheet.QueryTables[0].TextFileOtherDelimiter = self.delimiter
        sheet.QueryTables[0].TextFileStartRow = self.skip + 1
        # we must find how many columns are in the text file so we can manually set them all to string type, so
        # we check the first line that holds data (we skip the first `self.skip` rows)
        f = open( os.path.realpath( self.filename ), "r" )
        linecnt = 0
        numcols = 0
        try:
            for line in f:
                if linecnt < self.skip:
                    linecnt += 1
                    continue
                stripped_line = line.strip('\r\n')
                # this line gets a list of each word between the specified delimiter, and considers quotes ( " ) to
                # be text qualifiers. A string encased in quotes can include a delimiter without being split.
                cols = [x[0] + x[1] for x in re.findall('[' + self.delimiter + '](?: *"(.*?)" *|(.*?))(?=[' + self.delimiter + '])',self.delimiter + stripped_line + self.delimiter)]
                numcols = len(cols)
                linecnt += 1
                # after the first line we dont want to waste time so we get out of the file
                break
        finally:
            f.close()
        if numcols == 0:
            raise ValueError( "Empty file given" )
        # setting all columns to string type
        sheet.QueryTables[0].TextFileColumnDataTypes = [2] * numcols
        sheet.QueryTables[0].Refresh();
        return wb,sheet,excel

    def createTable( self ):
        """ Create a SQL table from a spreadsheet.
        
        Extended Overview
        ---------------------
        We call the getRows() method, and read in `buffer_size` rows into memory at a time, calling
        executemany() to insert them a chunk at a time. We do this in a loop until all rows are read in.
        
        TODO: How to handle if they have too many columns in the file? Can't add more than 1024
        """
        self._validate_create_table_inputs()
     
        # Delete table if it exists
        drop_table_if_exists( self.outputTable, self.db_context )
        
        #get the row iterator and update one row to get it to populate self.outputTable
        rowiter = self.getRows( self._table_list_factory )
        values = [ next( rowiter, None ) ]
        if values[0] is None:
            raise ValueError( "Empty file given" )
        
        # get column names for table before adding the import order column
        names = self.outputTable[:]
        
        # add import_order column
        self.outputTable.add( FieldSpec( self.importOrder,'bigint',identity=(1,1) ) )
        
        #create the table
        query = self.outputTable.definition
        self.db_context.executeNoResults( query )
        
        _insert_query = _INSERT_INTO_TABLE_QUERY.format( outputTable=self.outputTable,fields=Joiner( names ) )
        
        # now go through the file and get `buffer_size` arrays of values and insert them in chunks
        while ( True ):
            # fill buffer
            while ( len(values) < self.buffer_size ):
                row = next( rowiter, None )
                if row is None:
                    break
                values.append( row )
            if values is None or len( values ) == 0:
                break
            #insert into table
            self.db_context.executemany( _insert_query, values )
            values = []
        
        # commit changes
        self.db_context.commit()

        # Drop all completely blank rows
        query = _DELETE_MISSING_QUERY_PATTERN.format( outputTable=self.outputTable,columnNames = Joiner( names ) )
        self.db_context.executeNoResults( query )
        
        # Now add the import_order column to the primiary key
        query = _ADD_PK_QUERY.format( outputTable=self.outputTable, importOrder = db_identifier_quote( self.importOrder ) )
        self.db_context.executeNoResults( query )
            
    
    def getRows( self, factory = None ):
        """The spreadsheet contents in Python, as a sequence of "row" objects
        
        By default, the row objects will be dictionaries if getNames is True,
        otherwise they will be lists. The default behavior can be overridden by
        explicitly providing a callable parameter to serve as the row factory
        
        This method is a generator. The row objects are created as needed when
        you iterate through the return values. If you need to create all of the
        rows at once, do something like::
          
            values = [ x for x in reader.getRows() ]
        
        Arguments
        =========
        
        factory : callable
            A function, method or constructor that will be used to generate the row
            objects. This callable must accept two arguments: a list of names and a
            tuple of values.  See :ref:`dictFactory` and :ref:`listFactory`
        """
        self._validate_shared_inputs()
        
        if os.path.splitext( self.filename )[1].lower() == '.txt':
            found,sheet,excel = self._import_text()
        else:
            found,sheet,excel = self._get_worksheet_from_file()
        try:
            names = self._get_colnames( excel, sheet )
            if db_identifier_unquote( self.importOrder ) in names:
                raise ValueError( "Error: import_order column '" + self.importOrder + "' already exists in table. It must be a new column." )
            
            #create self.outputTable as a tablespec here
            if self.outputTable is None or isinstance(self.outputTable, (str,unicode)):
                # Even if we are importing into Python, we use a TableSpec to store type information
                self.outputTable = TableSpec( self.outputTable, None )
            del self.outputTable[:]
            for colname in names:
                self.outputTable.add( FieldSpec( colname,'int',data_length=1 ) )

            # Determine correct column types
            row_counter = self.skip      
            if self.getNames:
                row_counter += 1
            self._determine_column_types( excel, sheet, row_counter )
            
            # Create a row factory based on the found column types
            if factory is None:
                factory = row_factory( self.outputTable )
                    
            # Read the data
            while ( True ):
                values = self._getValues( excel, sheet, row_counter, self.buffer_size + row_counter - 1 )
                row_counter += self.buffer_size
                if values is None or len( values ) == 0:
                    break
                for row in values:
                    if any( row ):
                        yield factory( names, row )
        
        finally:
            self._close_workbook( found,excel )
            
    def _getValues( self, excel,sheet, row_start=0, row_end=1000 ):
        """ This function gets the row buffer from the file specified.
        
            Parameters
            --------------
            excel : excel object
                The excel object we use for the Intersect() function.
                
            sheet : Worksheet object
                This is the worksheet we will grab the rows from.
                
            row_start : int
                This is the offset to get our buffer from. The buffer will start at this row.
                
            row_end : int
                This is the last row to get. So if row_start= 10 and row_end=1000, we will grab "10:1000" (rows 10 to 1000).
                
            returns:
                Two arrays: one of the values, and one of the text representations of the cells in the range
        """
        if self.range is not None:
            try:
                from_range = excel.Intersect( sheet.UsedRange, sheet.Range( self.range ) )
            except pythoncom.com_error as e: # ignore eclipse error, this does exist
                #cleanup, then throw error
                self._close_workbook( sheet,excel )
                raise RuntimeError( win32api.FormatMessage( e.excepinfo[5] ) )
        else: 
            from_range = sheet.UsedRange

        # If there is no intersection, Excel nicely returns None
        if from_range is None:
            return []

        # Check limits
        first_row = max( row_start, 0 )
        last_row = min( row_end, from_range.rows.count-1 )
        if first_row > last_row:
            return []
        
        try:
            values = sheet.Range( from_range.rows[first_row], from_range.rows[last_row] ).value
            # if the user gets only one value as a result then it will break everything. We need to turn it into
            # a list containing a list of the one value.
            if not isinstance(values, (list,tuple)):
                values = [[values]]
            return values
        except pythoncom.com_error as e: # ignore eclipse error, this does exist
            #cleanup, then throw error
            self._close_workbook( sheet,excel )
            raise RuntimeError( win32api.FormatMessage( e.excepinfo[5] ) )
        
    def _get_col_types(self, row_list, tablespec=None):
        """ This function takes in a list of lists and a list of column names, and searches through
            the entire row_list to determine the types of data that appear in the columns.
            
            Parameters
            --------------
            row_list : list of lists
                This is the output from _getValues(). It should be a list of lists of items.
            
            tablespec : TableSpec object
                This is the TableSpec object that contains FieldSpecs for all columns in the row_list dataset.
            
            Returns
            ------------
            Nothing, it only edits the FieldSpecs contained in the tablespec.
            
            Note
            ---------
            This function sets the fields has_str,has_date,has_int, and has_float to the FieldSpec, and it sets the data_length property.
        """
        if len(row_list) == 0 or len(tablespec) == 0:
            return {}
        if len(row_list[0]) != len(tablespec):
            raise ValueError("Error: The number of columns does not match in the lists passed to _get_col_types")
        
        for row in row_list:
            for item,fieldspec in zip(row,tablespec):
                # skip missing values
                if item is None or item == '':
                    continue
                type_ = type(item)
                itemlen = len(str(item))
                if itemlen > fieldspec.data_length: 
                    fieldspec.data_length = itemlen
                if type_ in _DATE_TYPES:
                    fieldspec.has_date = True
                elif type_ is int or ( type_ is float and item == int( item ) ):
                    fieldspec.has_int = True
                    fieldspec.max_int = max( fieldspec.max_int, item )
                    fieldspec.min_int = min( fieldspec.min_int, item )
                elif type_ is float:
                    fieldspec.has_float = True
                else: # assume everything else is a string
                    fieldspec.has_str = True
    
    def _determine_column_types( self,excel,sheet,row_counter ):
        # adding the properties we need to the fieldspecs we just added
        for fieldspec in self.outputTable:
            fieldspec.has_str = False
            fieldspec.has_int = False
            fieldspec.has_float = False
            fieldspec.has_date = False
            fieldspec.max_int = 0
            fieldspec.min_int = 0
        if self.scan_all:
            while ( True ):
                #get new buffer
                values = self._getValues( excel,sheet,row_counter,self.buffer_size + row_counter-1 )
                row_counter += self.buffer_size
                if values is None or len( values ) == 0:
                    break
                self._get_col_types( values, self.outputTable )
        else:
            #get new buffer
            values = self._getValues( excel,sheet,row_counter,self.buffer_size + row_counter-1 )
            self._get_col_types( values,self.outputTable )
        for fieldspec in self.outputTable:
            # if there is a string value then change the column type to string
            if fieldspec.has_str or (fieldspec.has_date and (fieldspec.has_int or fieldspec.has_float)):
                fieldspec.basic_type = 'nvarchar'
            # change int to float
            elif fieldspec.has_float and not (fieldspec.has_str or fieldspec.has_date):
                fieldspec.basic_type = 'float'
            # if we haven't gotten any other types for this column and it is a datetime then
            # change the type to datetime.
            elif fieldspec.has_date and not (fieldspec.has_int or fieldspec.has_float or fieldspec.has_str):
                fieldspec.basic_type = 'datetime'
            elif fieldspec.has_int and not (fieldspec.has_date or fieldspec.has_float or fieldspec.has_str):
                if fieldspec.min_int >= 0 and fieldspec.max_int <= 255:
                    fieldspec.basic_type = 'tinyint'
                elif fieldspec.min_int >= -32768 and fieldspec.max_int <= 32767:
                    fieldspec.basic_type = 'smallint'
                elif fieldspec.min_int >= -2147483648 and fieldspec.max_int <= 2147483647:
                    fieldspec.basic_type = 'int'
                elif fieldspec.min_int >= -9223372036854775808 and fieldspec.max_int <= 9223372036854775807:
                    fieldspec.basic_type = 'bigint'
                else:
                    fieldspec.basic_type = 'float'
            else: 
                # this means no data was found (or it was all unrecognized types. They will have to be added if that happens)
                # so we set the data_length here to 1 if it is 0
                fieldspec.basic_type = 'nvarchar'
                fieldspec.data_length = max(fieldspec.data_length,1)
            
    def _get_colnames( self, excel, sheet ):
        """ This function gets the column names from the file. If the file doesn't contain
            column names (specified in self.getNames) then we create generic column names, 
            i.e. Column_1, Column_2, etc.
            
            Returns
            ----------
            A list of column names where each name is enclosed in square brackets [ ]
        """
        values = self._getValues( excel, sheet, self.skip, self.skip )
        if self.getNames:
            i = 0
            names = []
            for x in values[0]:
                if x in names or x is None:
                    names.append( db_identifier_unquote( 'Column_{}'.format( i ) ) )
                else:
                    names.append( db_identifier_unquote( str( x ) ) )
                i += 1
        else:
            names = [ db_identifier_unquote( 'Column_{}'.format( i ) ) for i in xrange( len( values[0] ) ) ]
        return names

    def _validate_shared_inputs( self ):
        # Check input validity for DB table create
        # File name
        self.filename = self.filename.strip('\'"' + string.whitespace)
        if not os.path.isfile( self.filename ):
            raise IOError("File not found: {filename}".format(**self.__dict__))
        if not os.access(self.filename, os.R_OK):
            raise IOError("Cannot read file: {filename}".format(**self.__dict__))
        
        # Worksheet name
        if self.sheetName is not None:
            if isinstance(self.sheetName, tuple(_STR_TYPES)):
                self.sheetName = self.sheetName.strip('\'"' + string.whitespace)
            elif not isinstance(self.sheetName, int):
                raise ValueError( "Error: sheetname must be an integer index, None, or a the name of the sheet as a string" )
        
        if self.skip < 0:
            raise ValueError("Error: Skip must be >= 0")
        
        # if importing a text file make sure a valid delimiter is specified
        if os.path.splitext( self.filename )[1].lower() == '.txt':
            if not isinstance(self.delimiter, tuple(_STR_TYPES)) or len(self.delimiter) == 0:
                raise ValueError("Error: A valid delimiter must be specified when importing text files")
        
        # range - change from a tuple to a string if necessary, and make sure the string is valid
        if isinstance(self.range,tuple):
            for x in self.range:
                if not isinstance(x, int):
                    raise ValueError( "Error: The numbers in the 4-tuple of range must be integers." )
            # we allow the tuple values to be zero-based, so we add 1 to row numbers make them 1-based for excel
            self.range = self._xlcol( self.range[1] ) + str( self.range[0] + 1 ) + ":" + self._xlcol( self.range[3] ) \
                         + str( self.range[2] + 1 )
        if self.range is not None and not isinstance(self.range, tuple(_STR_TYPES)):
            raise ValueError( "Error: Range must be a 4-tuple, None, or a valid range in excel (i.e. A1:D6)" )
        
    def _validate_create_table_inputs( self ):
        if self.outputTable is None:
            raise ValueError("Error: You must specify a tablespec or a table name for the outputTable parameter")
        # allow user to specify a valid tablespec with a reference to RunContext and DBContext instead of specifying them
        self.outputTable = get_table_spec( self.outputTable, self.db_context )
        self.db_context = self.outputTable.db_context
        self.runContext = self.db_context.runContext
        
    def _get_worksheet_from_file ( self ):
        found, self.to_close, excel = get_workbook_from_file( self.filename, excel = None )
        if found.Sheets is None or len(found.Sheets) == 0:
            raise Exception("There are no worksheets in excel file '" + self.filename + "'")
        if self.sheetName is None:
            return found,found.Sheets[ 0 ],excel
        else:
            #if they provided a sheetname then check that it exists, or 
            # if they are getting the sheet by index make sure that index exists
            if (isinstance(self.sheetName, (unicode,str)) and self.sheetName.lower() not in [sheet.name.lower() for sheet in found.Sheets]) or \
            (isinstance(self.sheetName,(int,float)) and len(found.Sheets) <= self.sheetName):
                raise Exception("Worksheet '" + str(self.sheetName) + "' does not exist in excel file '" + self.filename + "'")
            return found,found.Sheets[ self.sheetName ],excel
    
    def _close_workbook( self,found,excel ):
        # Close the worksheet only if it was not open before the call
        if self.to_close:
            excel.DisplayAlerts = False
            found.Close()
            excel.DisplayAlerts = True
            self.to_close = False
    def _xlcol( self,n ):
        '''Converts a zero-based column index to an Excel column identifier
        '''
        if n < 0:
            raise ValueError( 'Excel column number cannot be negative' )
        address = ''
        while n >= 0:
            n,r = divmod( n,26 )
            address = string.ascii_uppercase[r] + address
            n = n - 1 # This line compensates for the fact that, on the first pass, each digit has 27 repeats, including None. 
        return address
    
    def _xlcolnumber( self,s ):
        '''Converts an Excel column identifier to a zero-based index
        '''
        n = 0
        for c in s:
            n = n * 26 + ( ord(c.upper()) - 64 )
        return n-1
    
    def _table_list_factory( self, names, values ):
        ret_list = []
        for value,spec in zip(values,self.outputTable):
            if spec.basic_type == 'NVARCHAR' and value is not None:
                ret_list.append(str(value))
            else: 
                ret_list.append(value)
        return ret_list

def row_factory( table_spec ):
    """ This factory class will transform all values to of columns that are determined to be strings to be strings. 
        Pass transform_list or transform_dict to getRows().
    """
    if table_spec is None or not isinstance(table_spec, TableSpec):
        raise ValueError( "Error: You must pass a valid TableSpec to the constructor for tableFactory" )
    f_vector = [ _f_maker( field ) for field in table_spec  ]
    
    def factory( names, values ):
        row = Row(  )
        [ f( row, x ) for x, f in zip( values, f_vector ) ]
        return row
    
    return factory

# Create the appropriate type transformation functions
def _f_maker( field ):
    name = db_identifier_unquote( field.field_name )
    if field.basic_type == "NVARCHAR":
        if field.has_float:
            def f( row, val ):
                if val is None:
                    row.__setitem__( name, None )
                else:
                    row.__setitem__( name, str( val ) )
        elif field.has_int:
            def f( row, val ):
                if val is None:
                    row.__setitem__( name, None )
                elif isinstance( val, Number ):
                    row.__setitem__( name, str( int( val ) ) )
                else:
                    row.__setitem__( name, str( val ) )
        else:
            def f( row, val ):
                if val is None:
                    row.__setitem__( name, None )
                else:
                    row.__setitem__( name, str( val ) )
    elif field.basic_type in ( "TINYINT", "SMALLINT", "INT", "BIGINT" ):
        if field.has_str:
            def f( row, val ):
                if val is None:
                    row.__setitem__( name, None )
                elif isinstance( val, ( str, unicode ) ):
                    if val in ( '', '.' ):
                        row.__setitem__( name, None )
                    else:
                        val = val.split('.')[0]
                        row.__setitem__( name, int( val ) )
                else:
                    row.__setitem__( name, int( val ) )
        else:
            def f( row, val ):
                if val is None:
                    row.__setitem__( name, None )
                else:
                    row.__setitem__( name, int( val ) )
    elif field.basic_type == "FLOAT":
        if field.has_str:
            def f( row, val ):
                if val is None:
                    row.__setitem__( name, None )
                elif isinstance( val, ( str, unicode ) ):
                    if val in ( '', '.' ):
                        row.__setitem__( name, None )
                else:
                    row.__setitem__( name, float( val ) )
        else:
            def f( row, val ):
                if val is None:
                    row.__setitem__( name, None )
                else:
                    row.__setitem__( name, float( val ) )    
    else:
        def f( row, val ):
            row.__setitem__( name, val )
    return f

    