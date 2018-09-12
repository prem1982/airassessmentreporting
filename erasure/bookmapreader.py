'''
Created on Mar 13, 2013

@author: zschroeder 
'''

from airassessmentreporting.airutility import *

__all__ = [ 'Item', 'BookMap', 'cols_to_item', 'read_book_map', 'read_book_map_from_db', 'BookMapReader',
           '_LOCATIONS_COLUMNS','_BM_COLUMNS' ]

_LOCATIONS_COLUMNS = {"location":"location",
                      "grade_values":"grade_values",
                      "subject":"subject",
                      "subject_values":"subject_values",
                      "form_values":"form_values"}

_BM_COLUMNS = {"item position":"item position",
                  "book position":"book position",
                  "its id":"its id",
                  "grade":"grade",
                  "subject":"subject",
                  "form":"form",
                  "session":"session",
                  "reporting subscore":"reporting subscore",
                  "role":"role",
                  "item format":"item format",
                  "point value":"point value",
                  "answer key":"answer key",
                  "numeric key":"numeric key",
                  "weight":"weight",
                  "test":"test",
                  "tagged for release":"tagged for release",
                  "content standard":"content standard",
                  "grade level":"grade level"}
_INSERT_QUERY = """
                INSERT INTO {table:qualified}
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
"""
_ADD_PK_QUERY = "ALTER TABLE {table:qualified} ADD PRIMARY KEY ({column})"

class Item:
    "This class represents a single row (item) in a BookMap"
    position = -1
    book_position = -1
    its_id = -1
    grade = -1
    subject = ''
    form = ''
    session = ''
    report_subscore = ''
    role = ''
    format = ''
    point_value = -1
    answer_key = ''
    numeric_key = ''
    weight = -1
    test = ''
    release = False
    content_standard = ''
    grade_level = ''
    def __init__(self):
        self.position = -1
        self.book_position = -1
        self.its_id = -1
        self.grade = -1
        self.subject = ''
        self.form = ''
        self.session = ''
        self.report_subscore = ''
        self.role = ''
        self.format = ''
        self.point_value = -1
        self.answer_key = ''
        self.numeric_key = ''
        self.weight = -1
        self.test = ''
        self.release = False
        self.content_standard = ''
        self.grade_level = ''

class BookMap:
    "This class represents a BookMap and contains a list of items"
    grade_values = ''
    subject=''
    subject_values = ''
    form_values = ''
    items = []
    def __init__(self):
        self.grade_values = ''
        self.subject=''
        self.subject_values = ''
        self.form_values = ''
        self.items = []

def create_bm_table(db_context,outputTable):
    """ Creating a table to hold all bookmaps. The table name is specified by outputTable parameter
    """
    tablespec = TableSpec(db_context,outputTable)
    #add columns for bookmap information - these will be primary keys
    tablespec.add(FieldSpec('grade_values', 'int', nullable=False))
    tablespec.add(FieldSpec('subject', 'nvarchar', 64, nullable=False))
    tablespec.add(FieldSpec('subject_values', 'nvarchar', 2, nullable=False))
    tablespec.add(FieldSpec('form_values', 'nvarchar', 2, nullable=False))
    #now add columns for item information
    tablespec.add(FieldSpec('item position', 'int'))
    tablespec.add(FieldSpec('book position', 'int'))
    tablespec.add(FieldSpec('its id', 'bigint', nullable=False))
    tablespec.add(FieldSpec('grade', 'int'))
    tablespec.add(FieldSpec('item_subject', 'nvarchar', 64))
    tablespec.add(FieldSpec('form', 'nvarchar', 256))
    tablespec.add(FieldSpec('session', 'nvarchar', 256))
    tablespec.add(FieldSpec('reporting subscore', 'nvarchar', 64))
    tablespec.add(FieldSpec('role', 'nvarchar', 64))
    tablespec.add(FieldSpec('item format', 'nvarchar', 2))
    tablespec.add(FieldSpec('point value', 'int'))
    tablespec.add(FieldSpec('answer key', 'nvarchar', 64))
    tablespec.add(FieldSpec('numeric key', 'nvarchar', 64))
    tablespec.add(FieldSpec('weight', 'int'))
    tablespec.add(FieldSpec('tagged for release', 'nvarchar', 3))
    tablespec.add(FieldSpec('test', 'nvarchar', 256))
    tablespec.add(FieldSpec('content standard', 'nvarchar', 2))
    tablespec.add(FieldSpec('grade level', 'nvarchar', 10))
    #drop the table if it already exists
    drop_table_if_exists(outputTable, db_context)
    # create the table
    db_context.executeNoResults(tablespec.definition)
    return tablespec

def cols_to_item(cols,bookmap_cols):
    cur_item = Item()
    cur_item.position = cols[bookmap_cols["item position"]]
    cur_item.book_position = cols[bookmap_cols["book position"]]
    cur_item.its_id = cols[bookmap_cols["its id"]]
    cur_item.grade = cols[bookmap_cols["grade"]]
    cur_item.subject = cols[bookmap_cols["subject"]]
    cur_item.form = cols[bookmap_cols["form"]]
    cur_item.session = cols[bookmap_cols["session"]]
    cur_item.report_subscore = cols[bookmap_cols["reporting subscore"]]
    cur_item.role = cols[bookmap_cols["role"]]
    cur_item.format = cols[bookmap_cols["item format"]]
    cur_item.point_value = cols[bookmap_cols["point value"]]
    cur_item.answer_key = cols[bookmap_cols["answer key"]]
    cur_item.numeric_key = cols[bookmap_cols["numeric key"]]
    cur_item.weight = cols[bookmap_cols["weight"]]
    cur_item.test = cols[bookmap_cols["test"]]
    cur_item.release = cols[bookmap_cols["tagged for release"]]
    cur_item.content_standard = cols[bookmap_cols["content standard"]]
    cur_item.grade_level = cols[bookmap_cols["grade level"]]
    return cur_item
    
def read_book_map(cols, loc_cols, bookmap_cols,db_context):
    """This class reads a Bookmap Excel file and returns a BookMap object

        Extended summary
        -------------------------
        This class reads a Bookmap Excel file given the current line in the book map locations file.
        
        Parameters
        --------------
        cols : Dictionary of {colname : value}
            This should be the dictionary of column names to values in the current line of the 
            bookmap locations file.
            
        loc_cols : dictionary {String : String}
            This should be a dictionary of the column names for the bookmap locations file. The dictionary key
            should be the same keys as _LOCATIONS_COLUMNS, defined above. The dictionary value should be the
            associated column name in the locations file. For example if 'grade_locations' is named 'glocations'
            in the file then the key value pair in the dictionary would be {"grade_locations":"glocations"}.
        
        bookmap_cols : dictionary {String : String}
            This should be a dictionary of the column names for the bookmap files. The dictionary key
            should be the same keys as _BM_COLUMNS, defined above. The dictionary value should be the
            associated column name in the bookmaps file. For example if "item position" is named "position"
            in the file then the key value pair in the dictionary would be {"item position":"position"}.

        Returns
        ----------
        BookMap object. This represents the entire BookMap excel file contents.       
        
        Notes
        --------
        This is a helper function for the BookMapReader function.
    """
    #assuming we already checked for bad column mapping
    bm = BookMap()
    #bm.items = []
    fileloc = cols[loc_cols["location"]]
    bm.subject = cols[loc_cols["subject"]]
    bm.grade_values = cols[loc_cols["grade_values"]]
    bm.subject_values = cols[loc_cols["subject_values"]]
    bm.form_values = cols[loc_cols["form_values"]]
    runcontext = None
    if db_context is not None:
        runcontext = db_context.runContext
    reader = SafeExcelReader( run_context=runcontext,db_context=db_context,filename=fileloc, 
                              output_table=bm.subject + '_' + bm.form_values )   
    #setup column mapping for book map
    colmapping = {x:-1 for x in map(lambda y:y.lower(), bookmap_cols.values())}
    rownum = 0
    for row in reader.getRows():
        #only check headers on first row
        if rownum == 0:
            #now set the column number in mapping based on excel column names
            for item in row.keys():
                if item in colmapping:
                    colmapping[item] = 1
        if -1 in colmapping.values():
            raise Exception("Not all required columns contained in book map locations file. Required columns are: " + 
                            ",".join(_BM_COLUMNS.keys()))
        #currently no error checking done...
        bm.items.append(cols_to_item(row,bookmap_cols))
        rownum += 1
    return bm

def read_book_map_from_db(cols,loc_cols,in_colmapping,db_context, tablename, bookmap_cols):
    """This class reads a Bookmap SQL table and returns a BookMap object.

        Extended summary
        -------------------------
        This class reads a Bookmap SQL table given the current line in the book map locations SQL table 
        and the column mapping for that table.
        
        Parameters
        --------------
        cols : List of items
            This should be the list of column values in the current row of the bookmap locations table
            
        loc_cols : dictionary {String : String}
            This should be a dictionary of the column names for the bookmap locations file. The dictionary key
            should be the same keys as _LOCATIONS_COLUMNS, defined above. The dictionary value should be the
            associated column name in the locations file. For example if 'grade_locations' is named 'glocations'
            in the file then the key value pair in the dictionary would be {"grade_locations":"glocations"}.
        
        in_colmapping : Dictionary<String,int>
            This should be the Dictionary column mapping for the BookMap locations file. The keys
            are the column names, and the values are the column index.
        
        db_context : DBContext object
            This is the connection to the database we will use for queries.
            
        tablename : String
            This should be the name of the table to query for the BookMap.

        bookmap_cols : dictionary {String : String}
            This should be a dictionary of the column names for the bookmap files. The dictionary key
            should be the same keys as _BM_COLUMNS, defined above. The dictionary value should be the
            associated column name in the bookmaps file. For example if "item position" is named "position"
            in the file then the key value pair in the dictionary would be {"item position":"position"}.
        
        Returns
        ----------
        BookMap object. This represents the entire BookMap SQL table contents.       
        
        Notes
        --------
        This is a helper function for the BookMapReader function.
    """
    #assuming we already checked for bad column mapping
    bm = BookMap()
    #bm.items = []
    tablename = cols[in_colmapping[loc_cols["location"]]]
    bm.subject = cols[in_colmapping[loc_cols["subject"]]]
    bm.grade_values = cols[in_colmapping[loc_cols["grade_values"]]]
    bm.subject_values = cols[in_colmapping[loc_cols["subject_values"]]]
    bm.form_values = cols[in_colmapping[loc_cols["form_values"]]]
    #setup column mapping for book map
    colmapping = {x:-1 for x in map(lambda y:y.lower(), bookmap_cols.values())}
    #check table exists
    if not table_exists(tablename,db_context):
        raise Exception("Table \"" + tablename + "\" does not exist")
    #select only the columns we want
    select_query = "SELECT [" + "],[".join(colmapping.keys()) + "] FROM " + tablename
    #make sure all columns we want exist
    tablespec = db_context.getTableSpec(tablename)
    for column in tablespec:
        colname = db_identifier_unquote(column.field_name)
        if colname in colmapping:
            colmapping[colname] = column.ordinal_position - 1
    if -1 in colmapping.values():
        raise Exception("ERROR: Table '" + tablename +"' is missing a required column. The required columns are: " + 
                        ",".join(_BM_COLUMNS.keys()))
    #reset column mapping because we only selected the columns we need in the order we wanted
    cnt = 0
    for key in colmapping:
        colmapping[key] = cnt
        cnt+=1
    #now go through and get the data
    for row in db_context.executeBuffered(select_query):
        #currently no error checking done...
        rowdict = {name:row[colmapping[name]] for name in colmapping.keys()}
        bm.items.append(cols_to_item(rowdict,bookmap_cols))
    return bm
            
def BookMapReader(excel='Y',
                  inputds='',
                  db_context=None,
                  inputfile='',
                  inputsheet='',
                  locations_columns=_LOCATIONS_COLUMNS,
                  bookmap_columns=_BM_COLUMNS,
                  read_to_db=False,
                  outputTable=''
                  ):
    """This class reads a book map locations file and returns a list of BookMap objects
        
        Extended summary
        -------------------------
        This class takes a BookMap Locations file and returns a list of the bookmaps that are contained in it.
        A bookmaps locations file is an excel file (or SQL table) where each row contains a location of the
        bookmap, subject, grade_values, subject_values, and form_values.
        
        Parameters
        --------------
        excel : String ('Y' or 'N')
            This must be either 'Y' or 'N' and indicates whether the bookmaps are in an excel file or a
            SQL table. If excel='N' we assume it is a SQL table.
            
        inputds : String
            If excel='Y' this is not used. If excel='N' then this must be the name of the table that 
            contains the bookmap locations.
        
        db_context : DBContext object
            This is the DBContext within which all processing will be done. This specifies the DB connection.
            
        inputfile : String
            If excel='Y' this should be the location of the excel file that holds the bookmap locations. If 
            excel = 'N' then this is not used.
            
        inputsheet : String
            If excel='Y' this should be the name of the sheet the excel file that holds the bookmap locations. If 
            excel = 'N' then this is not used.

        locations_columns : dictionary {String : String}
            This should be a dictionary of the column names for the bookmap locations file. The dictionary key
            should be the same keys as _LOCATIONS_COLUMNS, defined above. The dictionary value should be the
            associated column name in the locations file. For example if 'grade_locations' is named 'glocations'
            in the file then the key value pair in the dictionary would be {"grade_locations":"glocations"}.
            The default value is _LOCATIONS_COLUMNS defined above.
            
        bookmap_columns : dictionary {String : String}
            This should be a dictionary of the column names for the bookmap files. The dictionary key
            should be the same keys as _BM_COLUMNS, defined above. The dictionary value should be the
            associated column name in the bookmaps file. For example if "item position" is named "position"
            in the file then the key value pair in the dictionary would be {"item position":"position"}.
            The default value is _BM_COLUMNS defined above.
        
        read_to_db : bool
            If true then the results will be placed in one table, with primary keys on the bookmap information.
            As well as still returning the list of bookmap objects. If false then this function will only return 
            a list of bookmap objects, without creating tables.
            
        outputTable : string
            If read_to_db is true then this must be specified. This will be the name of the table to create.
        
        Returns
        ----------
        List of BookMap objects. This represents the list of the bookmaps contained in the BookMap locations
        file.       
        
        Notes
        --------
        This is used in the Erasure function.
    """
    Yes = ['Y','YES']
    No = ['N','NO']
    YesOrNo = Yes + No
    if excel.strip().upper() in Yes and (inputfile.strip() == '' or inputsheet.strip() == '' ):
        raise Exception("Error: You must enter an inputfile and inputsheet when excel = 'Y'")
    if db_context is None and read_to_db:
        raise ValueError( "Error: You must specify a db_context when read_to_db is true" )
    if read_to_db and outputTable.strip() == '':
        raise ValueError( "Error: You must specify a table name in outputTable when read_to_db is true" )
    if excel.strip().upper() in No and (inputds.strip() == '' or db_context is None):
        raise Exception("Error: You must enter a DBContext and inputds when excel = 'N'")
    if excel.strip().upper() not in YesOrNo:
        raise Exception("Error: excel parameter must be one of these: " + ",".join(YesOrNo))
    if locations_columns.keys() != _LOCATIONS_COLUMNS.keys():
        raise Exception("Error: 'locations_columns' dictionary parameter's keys do not match the required keys in _LOCATIONS_COLUMNS")
    if bookmap_columns.keys() != _BM_COLUMNS.keys():
        raise Exception("Error: 'bookmap_columns' dictionary parameter's keys do not match the required keys in _BM_COLUMNS")
    # setting loc_colmapping to be {name of column in locations file in lowercase : -1 }  
    loc_colmapping = {x:-1 for x in map(lambda y:y.lower(), locations_columns.values())}
    list_of_bookmaps = []
    if excel.strip().upper() == 'Y': #input is in an excel file
        runcontext = None
        if db_context is not None:
            runcontext = db_context.runContext
        reader = SafeExcelReader(run_context=runcontext,db_context=db_context,filename=inputfile,sheet_name=inputsheet)
        rownum = 0
        for row in reader.getRows():
            #check headers only on first row
            if rownum == 0:
                #get column names as lowercase list
                columns = row.keys()
                #now set the column number in mapping based on excel column names
                for item in columns:
                    if item in loc_colmapping.keys():
                        loc_colmapping[item] = columns.index(item)
            if -1 in loc_colmapping.values():
                raise Exception("Not all required columns contained in book map locations file. Required columns are: " + 
                                ",".join(_LOCATIONS_COLUMNS.keys()))
            bm = read_book_map(row,locations_columns,bookmap_columns,db_context)
            list_of_bookmaps.append(bm)
            rownum += 1
    #otherwise it is a SQL Database we connect to
    else:
        if not table_exists(inputds,db_context):
            raise Exception("Table \"" + inputds + "\" does not exist")
        #check all required table columns exist
        tablespec = db_context.getTableSpec(inputds)
        for column in tablespec:
            colname = db_identifier_unquote(column.field_name)
            if colname in loc_colmapping:
                loc_colmapping[colname] = column.ordinal_position - 1
        if -1 in loc_colmapping.values():
            raise Exception("ERROR: The agg_file is missing a required column. The required columns are: " + 
                            ",".join(_LOCATIONS_COLUMNS.keys()))
        select_query = 'SELECT ['
        #add all columns we need
        select_query += "],[".join(loc_colmapping.keys())
        select_query += r"] from [" + inputds + "]"
        #reset column mapping because we only selected the columns we need in the order we wanted
        cnt = 0
        for key in loc_colmapping:
            loc_colmapping[key] = cnt
            cnt+=1
        for row in db_context.executeBuffered(select_query):
            bm = read_book_map_from_db(row,locations_columns,loc_colmapping,db_context, inputds,bookmap_columns)
            list_of_bookmaps.append(bm)
    #now we have the list of bookmaps and we return it..should we do data checking or anything?
    # add to table if user specified to
    if read_to_db:
        # create table to hold the bookmaps if we are supposed to
        tablespec = create_bm_table(db_context, outputTable)
        values = []
        for bookmap in list_of_bookmaps:
            for item in bookmap.items:
                row = []
                # add bookmap info to list (these will be primary keys)
                row.append(bookmap.grade_values)
                row.append(bookmap.subject)
                row.append(bookmap.subject_values)
                row.append(bookmap.form_values)
                # now add the item information
                row.append(item.position)
                row.append(item.book_position)
                row.append(item.its_id)
                row.append(item.grade)
                row.append(item.subject)
                row.append(item.form)
                row.append(item.session)
                row.append(item.report_subscore)
                row.append(item.role)
                row.append(item.format)
                row.append(item.point_value)
                row.append(item.answer_key)
                row.append(item.numeric_key)
                row.append(item.weight)
                if item.release:
                    row.append("yes")
                else: row.append("no")
                row.append(item.test)
                row.append(item.content_standard)
                row.append(item.grade_level)
                values.append(row)
        # add all values at once
        db_context.executemany(_INSERT_QUERY.format(table=tablespec),values)
        # now add primary keys to table. We set the bookmap information to be the primary keys, along with its id.
        # This is the first 4 fieldspecs in the tablespec or first 4 columns in the table, along with the 6th column.
        column_names = [ spec.field_name for spec in tablespec[:4] ] + [ tablespec[6].field_name ]
        db_context.executeNoResults( _ADD_PK_QUERY.format( table=tablespec,column=",".join( column_names ) ) )
    return list_of_bookmaps
        