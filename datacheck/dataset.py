import os
import csv 
import pyodbc
import xlrd
import inspect
# import xlwt
from collections import OrderedDict
from airassessmentreporting.airutility import Joiner

class SheetDictReader(object):
    """ 
    Create and return a csv.DictReader style of iterable to read an excel sheet.
    """
    
    def __init__(self, sheet=None,nskip=0):
        """
        Create and return a csv.DictReader style of iterable to read an excel 
        sheet.

Extended Summary
================
Create and return a csv.DictReader style of iterable to read an excel sheet, where the
column names are in row1 of the sheet, and where the sheet object is returned by xlrd 
method workbook.get_sheet_by_name() or workbook.get_sheet_by_index.

Params
======
sheet: sheet object ala package xlrd
This is the excel worksheet to read.

nskip: integer
Integer number of initial sheet rows to skip, afterwhich is a row of column names, 
after which are all of the data rows.

Notes
======
        """
        if sheet is None:
            # Future, maybe default to sheet 0 if no name, but error for now.
            raise ValueError("sheet is None")
        self.sheet = sheet
        self.odict = OrderedDict()
        
        # Also make member fieldnames[] semi-compatible with csv DictReader.
        # User may query them but should not change them. Column names are 'normalized'
        # below, so there is very likely no need to change them.
        self.fieldnames = []
        for col_idx in range(sheet.ncols):
            column_name =  str(self.sheet.cell(nskip,col_idx).value)
            column_name = column_name.lower().replace(' ','_')
            self.fieldnames.append(column_name)
            self.odict[column_name] = ""
        
    def __getitem__(self, index):
        # Populate the dict with the sheet's next row of column values 
        # (stripped)and return the dict.
        # We skip idx 0 (row 1) because it has the column names, already 
        # read in by init.
        # So, be aware that index 0 really returns the row 
        # labeled as 'row 2' of this type of excel spreadsheet. 
        #print "SheetReader: getitem: index=%d, nrows=%d" % (index, self.sheet.nrows)
        if index >= self.sheet.nrows -1  or index < 0:
            raise IndexError
        for idx_col, key in enumerate(self.odict):
            ctype = self.sheet.cell_type(index+1,idx_col)
            cvalue = self.sheet.cell(index+1,idx_col).value
            if ctype == 1:
                # Excel 'text' type of value.
                # Only if a text value, convert to str and strip it. 
                # Otherwise unicode conversion errors
                # on attempt to convert "excel float" values.
                # Future: could do user function, eg,  to put in lower case.
                cvalue = cvalue.strip()
            self.odict[key] = cvalue
        return self.odict  
    
    def __repr__(self):
        return ("%s: nrows=%s, ncols=%s, fieldnames=%s." 
          % (self.__class__.__name__, 
             repr(self.sheet.nrows),
             repr(self.sheet.ncols), 
             repr(self.fieldnames),
             ))
        
#end class SheetDictReader

    
class PyodbcReader(object):
    """ 
    Create and return a csv.DictReader style of iterable to read pyodbc query results.
    """
    def __init__(self, conn=None, cursor=None, query=None, qparams=None, 
      table=None, column_names=None, od_column_type=None, verbosity=None):
        """ 
        Initialize a reader for a db table via pyodbc.

        Initialize a reader for a db table via pyodbc. 
        (1) Set the instance parameters, and 
        (2) if parameter 'table' was used, get column names/types from db
        (3) submit the db query to get result rows
        
        Parameters:
        ===========
        
        conn: pyodbc connection 
        ------------------------
        -- see pyodbc docs
           
        cursor: pyodbc connection cursor
        --------------------------------
        -- pyodbc cursor
        
        table: string
        -------------
        -- A database table name to read.
        
        query: string
        -------------
        -- A database query to submit to the database to get result rows.
        -- It can be any DB query, for example a simple or multi-table join.
        -- The syntax of the query is not checked, but any exceptions from 
           the database will be provided. 
           
        qparams: list of strings
        ------------------------
        -- If the query uses the '?' style of placeholders, then the qparams
           are used to pair with the question marks.
             
        column_names: list of strings
        -----------------------------
        -- These are the column names to use if the data source is a query 
           because this code does not have a method to glean column names
           from a query.
        -- If this is None and datasource is a query, then od_column_type must
           be given, and it is used to glean column names.
        
        od_column_type: dictionary(key=column_name: value = db_data_type)
        ------------------------------------------------------------------
        -- Optional:
        -- If column_names is given, a key must be in columns_names.
        -- The type is typically a database-understandable type, though the
           syntax is not checked.
        -- Can be to register the data type of query result columns.
         
        Notes:
        ======

        -- This is called by Dataset.DictReader(), which has passed through its
        'query=' parameter value (or other parameter values)
         or created a query for all rows of a table.
        """
        iam = inspect.stack()[0][3]
        if conn is None:
            raise ValueError("Parameter conn is None.")
        self.conn = conn
        if query is None and table is None:
            raise ValueError("Parameter query is None.")
        self.query = query
        if cursor is None:
            raise ValueError("Parameter cursor is None")
        
        if verbosity:
            print ("%s: Got table='%s',\n\tquery='%s'" % (iam,table,query))
        # Also make member fieldnames[] semi-compatible with csv DictReader.
        # User may provide them but they should be of equal number as the 
        # columns in a result row. 
        self.cursor = cursor
        self.fieldnames=[]
        self.table = table
        self.od_column_type = od_column_type
        if column_names is None and od_column_type is None:
            raise ValueError(
              "Both column_names and od_column_type are None.")
        if column_names is None and od_column_type is not None:
            # use od_column_type keys for column names
            column_names = [x for x in od_column_type.keys()]
        self.column_names = column_names
            
        if not self.od_column_type and self.table:
            # Get column types for a table from the db.
            self.get_od_column_type()
        self.odict = OrderedDict()
        
        if self.column_names:
            self.fieldnames = self.column_names
        else:
            # This is a query for the whole table and no column_names were 
            # given, so use all its database-stored column names.
            # Todo: may get this from new od_column_type sometimes
            for col in self.cursor.columns(table=table):
                # print "PyodbcReader: table column='%s'" % col.column_name 
                self.fieldnames.append(col.column_name)
        for column_name in self.fieldnames:
            self.odict[column_name] = ""
        # Create cursor for executed query/results
        # See pyodbc docs for details cursor.execute()
        # print("PyodbcReader: running Query='%s'" % (query))
        if qparams is not None:
            try:
                cursor = cursor.execute(query, qparams)
            except:
                raise ValueError("Query failed='%s'" % (query))
        else:
            cursor = cursor.execute(query)
    #end def __init__()      
            
    def __getitem__(self, index):
        """
        Support caller's use of "for row in dr" where dr is an instance.
        
        Populate the result dict with the result's next row of column values
        (stripped) and return the result dict 'row'.
        """
        #print "PyodbcReader: getitem: index=%d, nrows=%d" % (index, self.sheet.nrows)
        row = self.cursor.fetchone()
        if row is None:
            raise IndexError
        # Create dict of column_name:values from row with lower cased values.
        # All data values are normalized to string. 
        # If needed, code here can be applied to convert specific columns 
        # to int, float, etc, as required.
        for idx_col, key in enumerate(self.odict):
            if row[idx_col] is None:
                self.odict[key] = None
            else:
                self.odict[key] = str(row[idx_col]).strip()
        return self.odict 
    # end def __getitem__() 
    
    def __repr__(self):
        return ("%s: query=%s, column_names=%s." 
          % (self.__class__.__name__, 
             repr(self.query),
             repr(self.column_names), 
            ))
        
    def get_od_column_type(self):
        """
        Set dict of column name and composed type.
        """
        if not self.table or not self.cursor:
            raise ValueError("pyodbc cursor and table name must be given.")
            
        self.od_column_type = OrderedDict()
        sized_types=["nvarchar","varchar"]
        
        for result in self.cursor.columns(table=self.table):
            #TODO: add support for decimal types here too if ever needed.
            tspec = result.type_name
            if tspec in sized_types:
                tspec = tspec + ("(%d)" % result.column_size)
            if result.is_nullable != "YES":
                tspec =  tspec + " not null"
            else:
                tspec = tspec + " null"
            self.od_column_type[result.column_name] = tspec
        return   
    #end get_od_column_type
    
#end class PyodbcReader()
 

class PyodbcWriter(object):
    def __init__(self,
      conn=None, cursor=None, table_name=None, 
      column_names=None, default_column_spec=None,
      od_column_type=None,
      replace=None,
      nvarchar_fixed_size=128,
      verbosity=None):
        """
Create a table with pyodbc and return a duck-instance of a DictWriter for it.

Consider: rather than here, create the table in Dataset's __init__ and allow 
multiple writers to exist that can open their own cursors to race or operate 
in parallel to insert their own rows into the table. 

Along the same lines, but independent issue; consider allowing an option to 
specify appending to an extant table.
For example, copy the SAS param name "replace", and if 1 then create else 
assume table exists for appending...

Parameters:
===========

conn : pyodbc connection object
-------------------------------

cursor: pyodbc cursor object
----------------------------

table_name: string 
------------------

- Name of database table to which to write.

column_names: list of column_name strings
-----------------------------------------

- list of column names of the table. 
- Now the columns are all of type nvarchar().

(DEPRECATED) nvarchar_fixed_size: integer 
------------------------------------------
    -- (to be deprecated. Use default_column_spec instead.)
    -- the size of the nvarchar field for each column
    -- default is 128
    
od_column_type: OrderedDict() (Optional)
------------------------------

- Key is column name, value is type spec
- If parameter column_names is also given, then it is
  the authoritative list of column names, and 
  each key in this dict must exist in the list of
  parameter column names. 
- The writer will create a table with default spec for each column from parameter
  column_names that is not in dict od_column_type, and use the spec
  in this dict for column names that are found in it.

default_column_spec: string (Optional)

- Defaults to 'nvarchar(128)' 
- Provides the default column spec to use with columns for a database-based dataset.
- This spec value is overridden for columns named in parameter od_column_type.
- Examples: "int", "float", "int not null"
        """
        self.conn = conn
        self.cursor = cursor
        self.verbosity = verbosity
        
        if table_name is None:
            raise ValueError("Table_name must be given.")
        self.table_name = table_name
    
        self.odict = OrderedDict()
        self.replace = False if replace is None else replace
        
        self.nvarchar_fixed_size = (
          128 if nvarchar_fixed_size is None else nvarchar_fixed_size)
        
        if column_names is None and od_column_type is None:
            raise ValueError(
              "Parameter column_names or od_column_type must be given.")
            
        # Delete useless empty trailing column name if extant. SAS does this.
        if column_names is not None and (
            column_names[-1] is None or column_names[-1] == "" ):
            del column_names[-1]

        self.column_names = column_names
        if od_column_type is None:
            self.od_column_type = {}
        else:
            self.od_column_type = od_column_type
            
        if (default_column_spec is None):
            # Per various SQL SERVER 2012 docs, nvarchar < 8000 all use 
            # exact same storage for most fields, so use 7999 here. 
            # Update: use 3999 or fewer. (1)  try 3999 because SQL
            # local server is SQL Express, max is 4000. No, It seems slow. 
            # (2) Stick with 128. Caller can override it anyway.
            self.default_column_spec = "nvarchar(128)"
        else:
            self.default_column_spec = default_column_spec
        # Maybe remove this self.fieldnames member...? it is set by dr, but
        # maybe not used here for dataset.
        self.fieldnames = column_names
        
        # Create/initialize writable output table and columns.
        self._init_table()

    def _init_table(self):
        # Setup the storage vessel (table) with the 'header' columns based on 
        # the parameters column_names and dict self.od_column_type
        
        # Set up initial column order 
        if self.column_names is None:
            self.column_names = [x for x in self.od_column_type.keys()]
            
        if self.column_names is None:
            raise ValueError("Parameter column_names is missing.")
            
   
        # Create a new table for given column names of given spec type.
        column_specs = ""
        columns_string = ""
        qmarks = ""
        delim = ""

        for cn in self.column_names:
            if self.od_column_type.get(cn, None) is None:
                spec = self.default_column_spec
            else:
                spec = self.od_column_type[cn]
            cs = "%s%s %s" % (delim, cn, spec)
            column_specs += cs
            columns_string += delim + cn
            qmarks +=  delim + "?"
            delim = ", "
        sql_create = (
            "create table %s ( %s )" % (self.table_name, column_specs))
        self.sql_create = sql_create
        if self.verbosity:
            print ("sql_create='%s'" % sql_create)
        
        self.cursor = self.conn.cursor()
        # Create the table.
        if self.replace == True:
            # First, try to delete the table. If cannot delete, no worries.
            try:
                sql_delete = "drop table %s" % self.table_name
                self.cursor.execute(sql_delete)
                self.conn.commit()
            except: 
                pass
            
        # CREATE the table to write to.
        self.cursor.execute(sql_create)
        self.conn.commit()
        
        # Create the row insertion sql for later use.
        self.insert_sql = (
          "insert into %s(%s) values (%s)" %
          (self.table_name, columns_string, qmarks)
          )
        self.insert_sql2 = (
          " insert into %s(%s) values ({values:item='X',itemfmt='\"{{X}}\"'}) ; \n" %
          (self.table_name, columns_string)
          )
        self.insert_sql3 = (
          " insert into %s(%s) values " %
          (self.table_name, columns_string)
          )
        #print ("insert_sql= '%s'" % self.insert_sql)

    def writeheader(self, column_names=None):
        # No header action needed because __init__() already created the table, but 
        # must maintain this stub to support generic Dataset() interface.
        return
    
    def writerow(self, row=None, row_columns=None):
        """ Write a row of data to a table.

Parameters:
===========
row: dictionary {column_name : string_value}

   - A dictionary whose keys are column names and values are string values to 
     write to a new row in the table.
   - Either this parameter or parameter row_columns must be provided, but not both.
   - An error is raised if a key in this dictionary is not in self.column_names. 
   - If not all columns in self.column_names exist in the dictionary, 
     the result of the operation is undefined or an exception may be raised.

row_columns: list

    - A list of string values to write a new row to the table. 
    - The column order given in self.column_names is written.
      An error is raised if too many or too few string values exist in the list.
        """
        iam = inspect.stack()[0][3]

        if row_columns is not None:
            self.cursor.execute(self.insert_sql, *row_columns)
        elif row is not None:
            # Must use order in self.column_names
            row_columns = [ row[x] for x in self.column_names]
            # print "row_columns='%s'" % repr(row_columns)
            self.cursor.execute(self.insert_sql, *row_columns)
        else:
            raise ValueError(
              "%s:Either row or row_columns parameter must be given" % iam)
        return
    
    def copyreader_unsafe(self, reader):
        sql = ''
        sql_values = """{v:item='X',itemfmt="'{{X}}'"}); \n"""
        j = 0
        for i,row in enumerate(reader):
            values = [val.replace("'","''") for key,val in row.iteritems()]
            sql += self.insert_sql3 + sql_values.format(v=Joiner(values))
            j += 1
            if j == 1000:
                self.cursor.execute(sql)
                sql = ''
                j = 0
        self.cursor.execute(sql)

    
# end class PyodbcWriter 


import types

def hvp_writeheader( self ): 
    """
    DictWriter.writer writeheader() for dbms == 'hvp' 
    
    NB: This function should be cast to a method assigned to an instance
    Hence, the use of "self" applies to the host instance.
    """  
    dsw = self.dsw  
    columns_line = dsw.delimiter.join(self.fieldnames)
    if self.verbosity and self.verbosity is not None:
        print ("hvp_writeheader:header_name=%s, Columns='%s'" 
           % (dsw.header_name,repr(columns_line)))
           
    with open(self.dsw.header_name, 'wb') as fh:
        fh.write(columns_line)
# end def hvp_writeheader()

def tvp_writeheader( self ): 
    """
    DictWriter.writer writeheader() for dbms == 'tvp' 
    tvp = type-value-pair: two file types
    (1) xxx.cty: is tab separated column type file (column name, tab, type),
        with exactly one column mentioned per file line.
    (2) xxx.txv: is the file of rows/lines of tab separated column values, 
        where one data row's column values are output per file line.
        
    NB: Here, "self" is supposed to be a Dataset.DictWriter(dbms='tvp') 
    instance. 
    """ 
    dsw = self.dsw
          
    if self.verbosity and self.verbosity is not None:
        print ("tvp_writeheader:header_name=%s, fieldnames='%s'" 
           % (dsw.header_name, repr(self.fieldnames)))
    # fieldnames is authoritative ordered list of colnames
    # because in future od_column_type will not be required to have 
    # all columns       
    with open(dsw.header_name, 'wb') as fh:
        for colname in self.fieldnames:
            # add logic to allow missing entry and use a default spec/type 
            coltype = dsw.od_column_type[colname]
            line = "%s\t%s\n" % (colname, coltype)
            fh.write(line)

            
# end ----------------  def tvp_writeheader() ------------------

class Dataset(object):
    
    def __init__(
      self, dbms="csv", name=None, columns=None,
      ds_layout=None,lspec=None,  delimiter=None,    
      workbook_file=None, sheet_name=None, 
      server=None, db=None, conn=None, table=None, query=None, 
      db_context=None, db_table_spec=None, replace=None,
      column_names=None, od_column_type=None,
      open_mode=None, fieldnames=None,
      lengths=None, verbosity=None):

        """
        Create Dataset object and validate parameters and interactions.
        
Extended Summary:
=================
Create Dataset object to write to or read a relational data store.

The Dataset object provides a generic uniform interface to read and write data, 
regardless of whether it exists in csv files, excel spreadsheets, or database 
tables.

The 'dbms' and 'open_mode' parameters control whether reading or writing 
may be done, and the database-management-style (dbms) of reading and writing.


Common Parameters:
==================
dbms : String
-------------
    -- csv, excel_srcn, pyodbc, hvp, tvp, fts (under construction), 
    -- future possible: postgresql, sqllite3, mysql, etc...

Parameters Where dbms='csv'(or None) and open_mode='rb'
========================================================
name : String
-------------
    -- File system's full pathname of the xxx.csv file to read.
    -- The column names are assumed to be in the first row of the input file, 
       comma-separated.

Where dbms='csv' and open_mode='wb'
===================================

name : String 
-------------
    -- File system's full pathname of the xxx.csv file to write.
    
columns : String
----------------
    -- String of comma-separated column names, in order, to write output rows.
    -- Not needed if parameter od_column_type is given.
     
od_column_type: dictionary
--------------------------
    -- Key is column name, value is a column type string. 
    -- Code that uses the reader may use this info to decide whether to cast
       the string column value to a float or int, and so on, if needed.
    -- If parameter columns or column_names is provided, the key must exist
       as a name listed there.
    -- If parameter columns or column_names is not provided, this dictionary
       is the authoritative source of column names and types
       
Where dbms='excel_srcn' and open_mode='rb'
==========================================

workbook_file : String
----------------------
    -- Full pathname of the workbook file.

sheet_name: String (optional)
-----------------------------
    -- Sheetname to read within the workbook file. 
    -- If none is given, the first sheet of the workbook will be read.


Where dbms='pyodbc' and open_mode='rb':
=======================================

server : String
---------------
    -- String representing the SQL Server to use. 
    -- Examples: server="DCSMITHJ1\SQLEXRESS", server="38.118.83.61"
    -- Either (1) server and db are required or (2) conn is required

db : String
-----------
    -- Database to connect to. 
    -- Examples: db="testdb", db="ScoreReportingTestData"
        
conn: Pyodbc connection
-----------------------
    -- pre-created pyodbc connection to the server and database of choice.
    
query : String
--------------
    -- A select statement that will produce the rows that a 
       dataset.DictReader() can read.
    -- The query may be simple or a complex query joining multiple tables.
    -- Either a query or a 'table' param must be specified, but not both.

table : String
--------------
    -- The name of a database table in the default schema of the database.
    -- Either a table or a query must be specified, but not both.

columns or column_names: [] List of strings 
--------------------------------------------
    -- Optional when given parameter "table", and required for "query"
    -- Example: column_names=["id", "name", "score" ]
    -- If the table param is given and this param not given, the column names of the 
       table in the database will be used.
    -- This parameter is useful when multiple data tables have the same semantic
       column contents, but the names are not constant. This can provide
       a constant list of names that the code can use regardless of the table's
       column names used in its database. 
    -- use of parameter name 'columns' is a candidate for deprecation
    
od_column_type: OrderedDict()
-----------------------------
    -- Initial implementation. Not Allowed. Pyodbc will retrieve this from 
       the db.
    -- Future1: Optional. If NOT given, the column names and types from the 
       pyodbc table will be used.
    -- Future1: If given, this dictionary will completely override the 
       pyodbc column information. 
       Future2: allow option for this information to change only
       the pyodbc column type information for column names that appear 
       in this dictionary.

Where dbms="pyodbc" and open_mode='wb':
========================================
server : String
---------------
    -- String representing the SQL Server to use. 
    -- Examples: server="DCSMITHJ1\SQLEXRESS", server="38.118.83.61"

db : String
-----------
    -- Database to connect to. 
    -- Examples: db="testdb", db="ScoreReportingTestData"

table : String
--------------
    -- The name of a database table in the default schema of the database.
    -- If it exists and the replace parameter is not given or false, then an attempt 
       to write to the table will raise an error.

column_names : [] (Optional)
-----------------------------
    -- List of column names in order that are to be used to create the table and
       the order to write data rows' columns to the table.
    -- Example: column_names=["id", "name", "score" ]
    -- The column_names parameter is optional if it will be specified 
       specified in a call to dataset.DictWriter(), but either way it is required
       before an attempt to call the DictWriter's witerow() method is made.

od_column_type: OrderedDictionary() (optional)
----------------------------------------------    
    -- Parameter column_names or od_column_type, but not both, must be given.
    -- Key is column name and value 'spec' is a pyodbc destination database
       server's column type. 
       Eg, SQL Server may be integer, float, nvarchar(128), etc.
    -- Ex: { "id":"integer", "name":"nvarchar(128)", "score":"float" }
        
Where dbms='fts' and open_mode='wb':(Under construction)
--------------------------------------------------------
open_mode: 'wb'
    -- Param open_mode must always be 'wb' for dbms='fts'.

db_table_spec: TableSpec
    -- A TableSpec object defining the table to which to write.

Where dbms='tvp' and open_mode='rb'
===================================

Parameter od_column_type : OrderedDictionary()
----------------------------------------------
-- Initial Implementation: Not Allowed. This info is retrieved from the 'cty' 
   file.
-- Future1:    key is column name
 
-- Future1: value is column type, not syntactically checked, but if using an 
   SQL Server Database for data conversions, it can be useful
   to conform to that syntax. 
   Future2: may implement parameter 'dialect' to specify
   a desired syntax for the type info here.
   
-- Note: function data(), when reading a dataset, will apply the readable
   dataset's od_column_type info to the output dataset, if both datasets
   use  column types via member od_column_type.
   
-- Each key-value pair is stored in order on a single line in a file {name}.cty
   where name is given by parameter 'name' and cty stands for "column type".
   Column and type fields are separated by a tab character.
   Reminder: the data values are stored in {file}.tsv, tab separated values.
   
-- 
Where dbms='tvp' and open_mode='wb'
=================================== 
  
Parameter od_column_type : OrderedDictionary()
----------------------------------------------
-- Optional. However, the paramater must be given to DictWriter() if it is 
   not given here.

Notes:
======

Read a csv file: dsr = Dataset(dbms='csv',name='path_to-some_csv_file', open_mode='rb')
-------------------------------------------------------------------------------------
-- Object dsr does not read
-- Object dsr supports method dict_reader() to create a reader.  

rows = dsr.dict_reader()
-----------------------
- Creates iterator "rows" to enable iterate to produce a row dictionary 
  for the current (next) row in the dataset where key is the column name and
  value is the string value.
- For the dataset, creates and initializes a dictionary style "reader" 
    - this reads row1 (fieldnames)from the csv file named in the 'ds' 
      initialization 
    - and creates and returns the iterable object, here named 'rows'.
    - Now rows.fieldnames is a dictionary with keys (fieldnames) of the 
      csv file, given in the first row of the csv file.
    - One can now inspect rows.fieldnames[] to see the fieldnames that were 
      in the csv file. 
    - Now, before getting a row of data, one can optionally change the 
      fieldnames, say making the names all lowercase and turning spaces 
       into underscores.
        - rows.fieldnames = [field.replace(' ','_').lower() for field in dr.fieldnames]

- Now one reads the next row simply by iterating on rows in a for loop, 
  where each item retrieved (here 'row') is a dictionary filled with data 
  from all the column values in the next line in the input file:
        
- Example to read:

column_name = "name_of_an_interesting_column"
for row in rows:
    print "Intesting column=", column_name, ", value=", row[column_name]

Example to write to a csv file:
-------------------------------
dsw = Dataset(dbms='csv', open_mode='wb', 
  name='C:/users/temp_rphillips/phone2.csv')

dw = dsw.DictWriter(column_names=["id","name","score"])
dw.writeheader()

rows = [{'id':"1", 'name':"Rod",'score':'35'}, 
  {'id':'2','name':"Jane",'score':'55'}, 
  {'id':'61','name':"Fido",'score':'28'}]
for row in rows:
    print "writing row=%s" % repr(row)
    dw.writerow(drow)
del dsw,dw
     
        """
        iam = inspect.stack()[0][3]

        self.verbosity = verbosity
        if (verbosity):
            print "\nDataset() starting...\n"
        dbmses=['csv','hvp','tvp','pyodbc','fixed','fixed2','excel_srcn']
        if dbms is None or dbms not in dbmses:
            raise ValueError(
              "Parameter dbms must be defined from %s" % repr(dbmses))
            
        self.dbms = dbms
        need_name=['csv','hvp','tvp']
        if name is None and dbms in need_name:
            raise ValueError(
              "Parameter 'name' must be a pathname to a dataset value file.")
        self.name = name
        if not open_mode or open_mode not in ('rb','wb'):
            raise ValueError(
                "open_mode = %s is not in ('rb','wb')" % open_mode)
        self.open_mode = open_mode
        self.replace = replace
        self.column_names = (
            column_names if column_names is not None else columns)
        self.od_column_type = od_column_type
        self.csvfile = None
        self.query = query
        self.conn = conn 
        self.param_conn = False
        self.verbosity = verbosity
        self.delimiter = delimiter
        
        if self.dbms == 'csv' or self.dbms == 'hvp' or self.dbms =='tvp':
            if delimiter is None:
                # For hvp and tvp(header in separate file than values), default
                # delimiter '\t' matches default of 'bcp' and other
                # utilities.
                self.delimiter = ',' if self.dbms == 'csv' else '\t'
            self.dialect = 'excel' if self.dbms == 'csv' else 'excel_tab'
            # For csv style data files, open it here, but for hvp and tvp,
            # open the 'header' file in DictReader() 
            self.csvfile =  open(self.name,self.open_mode)
            self.od_column_type = od_column_type
            
        elif self.dbms == 'fts':
            if (open_mode != 'wb'):
                raise ValueError(
                  "dbms='%s', open_mode='%s' not supported."
                  % (self.dbms, open_mode))
            #if (db_context is None):
            #    raise ValueError("DictWriter dbms='fts' requires db_context")
            self.db_context = db_context
            # start with mere db_table_spec - it has db_context embedded.
            if db_table_spec is None:
                raise ValueError(
                  "DictWriter dbms='fts' requires db_table_spec")
            self.db_table_spec = db_table_spec
                    
        elif self.dbms == 'pyodbc':
            self.table = table
            self.query = query

            if query is None and table is None:
                raise ValueError(
                  "When self.dbms = '%s', either table "
                  "or query must be given"  % self.dbms)
            if (table is not None and query is not None):
                raise ValueError(
                  "When self.dbms = '%s', both table('%s') and "
                  "query('%s') may not be given" 
                  % (self.dbms, table, query))
            if (query is not None):
                # The query requires column names
                if (not self.column_names and not self.od_column_type):
                    raise ValueError(
                      "When query is given, column_names or "
                      "od_column_type must be given.")
            if verbosity:
                print ("Dataset(): pyodbc parameters are ok" ) 
                       
            if conn is not None:
                # We should have an extant connection
                # Member param_conn is whether the self.conn was set
                # via a function parameter
                self.param_conn = True
                self.conn = conn
            else:
                # Param conn not given, so open the connection now    
                if ( server is None or db is None ):
                    raise ValueError(
                      "When self.dbms = '%s', either conn or server and db "
                      "must be given"  % self.dbms)
                self.server = server
                self.db = db
                self.param_conn = False
                self.cxs = ('DRIVER={SQL Server};;SERVER=%s;'
                  'dataBASE=%s;Trusted_connection=yes' 
                  % (self.server, self.db))
                self.conn = pyodbc.connect(self.cxs)
                # Open the primary cursor for this connection. 
                if self.conn is None:
                    raise ValueError(
                      "Cannot connect using pyodbc connect string='%s'"
                      % (self.cxs) )
            # end else: Param conn not given, so open the connection now    
          
            self.cursor = self.conn.cursor()
            if self.cursor is None:
                raise ValueError(
                  "%s: ERROR - Cannot open cursor." % repr(self))
                
            # Now that we have self.cursor, if need to get columns, we can.
            if table is not None and open_mode == 'rb':
                # For given table, make a query of all columns in order
                # Set query to get all columns from the table, in order.
                # First, get those colum names.
                if self.column_names is None :
                    # Column names not given, so use table's column names
                    tcnames = []
                    for col in self.cursor.columns(table=table):
                        tcnames.append(col.column_name)
                    if len(tcnames) == 0:
                        raise ValueError(
                          "Table %s not in database or has no column names!"
                           % table)
                    self.column_names = tcnames
                columns_string = ",".join(self.column_names)
                self.query = (
                    'select %s from %s' % (columns_string, self.table))
            # if table and open_mode=='rb'
            if self.verbosity:
                print("%s: dbms='%s',open_mode='%s',table=%s, query='%s'"
                  % (iam, dbms, open_mode, repr(self.table), 
                     repr(self.query)))
        # end elif self.dbms = 'pyodbc'
        elif self.dbms == 'pyodbc_conn':
            # NB: may retire this dbms value. dbms 'pyodbc' has the
            # optional "conn" parameter that does what is needed to speed
            # connections.
            if table is None:
                raise ValueError(
                   "When self.dbms = '%s', table must be given" % self.dbms)
        elif self.dbms == 'fixed':
            if ds_layout is None:
                raise ValueError(
                  "When dbms=%s, ds_layout must be given" % self.dbms)
            self.ds_layout = ds_layout
            if name is None:
                raise ValueError(
                  "When dbms=%s, name (filename) must be given" % self.dbms)
            self.name = self.name
            self.lspec = 0 if lspec is None else lspec
            
        elif self.dbms == 'fixed2':
            self.lengths = lengths
            self.fieldnames = fieldnames
            self.lspec = 0 if lspec is None else lspec

        elif self.dbms == 'excel_srcn':
            # excel sheet SkipRowColumnNames ("excel_srcn"). 
            # That is, skip the first N of rows in param nskip,
            # and the next row is the column names, and all the following 
            # rows are data rows.
            # Usually we skip 0 row because the column names are in 
            # the first row of most project excel sheets.
            if workbook_file is None:
                raise ValueError(
                  "When self.dbms = '%s', workbook must be given" % self.dbms)
            # Open the workbook file
            self.workbook_file = workbook_file
            self.wb = xlrd.open_workbook(workbook_file)    
            
            # open the sheet
            self.sheet_name = sheet_name
            if sheet_name is None:
                # relax and default to: self.wb.sheet_by_index(0)
                self.sheet = self.wb.sheet_by_index(0)
                self.sheet_name = self.sheet.name
            else:
                # open the named sheet
                try:
                    self.sheet = self.wb.sheet_by_name(sheet_name)
                except:
                    # Maybe too lax? Ignore bad sheetname and open
                    # the first one.
                    self.sheet = self.wb.sheet_by_index(0)
                    self.sheet_name = self.sheet.name
        else:
            raise ValueError("dbms=%s is not implemented" % dbms)
    # end def __init__()
               
    def __repr__(self):
        if ( self.dbms == 'csv' or self.dbms == 'tvp' or self.dbms == 'hvp'): 
            return (
              "%s:dbms=%s, name=%s, open_mode=%s." 
              % (self.__class__.__name__, repr(self.dbms), 
                 repr(self.name), repr(self.open_mode)))
        elif (self.dbms == 'excel_srcn'):
            return (
              "%s:dbms=%s, workbook_filename=%s, sheetname=%s, open_mode=%s." 
              % (self.__class__.__name__, repr(self.dbms), 
                 repr(self.workbook_file),
                 repr(self.sheet_name), repr(self.open_mode),
                 ))
        elif (self.dbms == 'pyodbc'):
            return ("\n%s:dbms=%s, \n\tquery=%s\n\ttable=%s\n\topen_mode=%s." 
              % (self.__class__.__name__, repr(self.dbms), 
                 "" if self.query is None else repr(self.query),
                 "" if self.table is None else repr(self.table),
                 repr(self.open_mode),
                 ))
    def close(self): 
        """
        Close resources for reading/writing to this Dataset.
        """ 
        if self.csvfile is not None:
                try:
                    self.csvfile.close()
                except:
                    pass 
        elif self.dbms == 'pyodbc':
            if (not self.param_conn and self.conn):
                # Do not close if conn pre-existed via param>conn
                self.conn.commit()
                self.conn.close()
        elif self.dbms == 'excel':
            if (self.wb):
                self.wb.release_resources()
    # end def close()
              
    def __del__(self):
        try:
            self.close()
        except:
            pass
 
    def DictReader(self):
        """ 
        Create and return a DictReader object for filename, open_mode.

        Extended Summary:
        =================
        Create and return a DictReader object based on self.filename, mode 
        data.

        Also, normalize the fieldnames by changing blanks to underscores 
        and using all lowercase.

        Return:
        =======
        Return an iterator, where each iteration is a dictionary with key 
        of field name and value of the input field value in the next row 
        in the csv input file file.

        Notes:
        ======
        The returned object, say dr, may be procesed like: "for row in dr:", 
        where row is a dictionary keyed by column names with values of 
        the next row in the dataset. 

        See Collector.collect() for a sample usage.

        - csvfile is closed when Dataset is deleted or goes out of scope.
        """
        iam = inspect.stack()[0][3]
        
        if self.open_mode != 'rb':
            raise ValueError(
              "Dataset name=%s, mode is '%s', cannot read it."
              % (self.name, self.open_mode))
        
        if self.dbms == 'csv':
            self.csvfile = open(self.name,self.open_mode)
            dr = csv.DictReader(self.csvfile, delimiter=self.delimiter)
            # On dr instantiation, the first row from csvfile is read in, so 
            # dr.fieldnames[] is now populated.
            # Normalize the fieldnames.
            # Convert field/column names to lower case and convert 
            # whitespace if any to underbars.
            dr.fieldnames = [field.replace(' ','_').lower() 
                for field in dr.fieldnames]
            self.column_names = dr.fieldnames
           
        elif self.dbms == 'hvp':
            # Open the data values file, like xxx.tsv,  usually.
            self.csvfile = open(self.name, self.open_mode)
            
            # Compose the 'header file name' and read the fieldnames.
            base = os.path.splitext(self.name)[0]
            header_name = base + ".hdr" 
            with open(header_name, 'rb') as fh:
                columns_line = next(fh).decode()
            
            fieldnames = columns_line.split(self.delimiter)
            # SAS csv dumps have trailing null fieldname to delete
            if fieldnames[-1] is None or fieldnames[-1] == "" :
                del fieldnames[-1]
            
            # The header column names were read from the header_name
            # file, so give parameter fieldnames to the DictReader().
            # The data file name should have only data rows, no initial
            # row of column names.
            dr = csv.DictReader(self.csvfile, delimiter=self.delimiter
              , fieldnames=fieldnames)
              
            dr.fieldnames = [field.replace(' ','_').lower() 
                for field in dr.fieldnames]
            # save fieldnames to Dataset
            self.column_names = dr.fieldnames
        elif self.dbms == 'tvp':
            # We will create an augmented instance of a standard 'csv' DictReader,
            # So do special setting of self and dr members here.
            # open the value file, 
            self.csvfile = open(self.name, self.open_mode)
            # Compose the 'column names types  file name' and read it.
            # Note: could do this in DataSet __init__() instead, but
            # keep here until a need arises.
            base = os.path.splitext(self.name)[0]
            # We ingest the header info here, so no real need to keep 
            # header_name, but probably good for future development.
            header_name = base + ".cty" 
            self.header_name = header_name
            fieldnames = []
            od_column_type = OrderedDict()
            with open(header_name, 'rb') as fh:
                # get one column name and type per ".cty" file line
                for line in fh:
                    line = line.split('\n')[0]
                    typefields=line.split(self.delimiter)
                    fieldnames.append(typefields[0])
                    od_column_type[typefields[0]] = typefields[1]
                    
            # Promote the column type info to the Dataset
            self.od_column_type = od_column_type
            if self.verbosity:
                print ("%s:dbms=%s,od_column_type=%s,fieldnames=%s" 
                % (iam,self.dbms,repr(od_column_type),repr(fieldnames)))
                
            # The header column names were read from the header_name
            # file, so give parameter 'fieldnames' to the DictReader().
            # Note that the data file name should have only data rows, 
            # no initial row of column names.
            fieldnames = [field.replace(' ','_').lower() 
                for field in fieldnames]
            dr = csv.DictReader(self.csvfile, delimiter=self.delimiter
              , fieldnames=fieldnames)
              
            # tvp_writeheader uses dr.dsw to access some dsw members.
            dr.dsw = self
            dr.od_column_type = od_column_type
            # save fieldnames to Dataset column_names
            self.column_names = fieldnames
           
            # To the csv.DictReader, dr,  register an extra member od_column_type.
            # Compare PyodbcReader. It also maintains od_column_type.
            # Function data() will use od_column_type in the dsr reader, if
            # extant, and pass its types along to the dsw dataset.
            dr.od_column_type = self.od_column_type

        elif self.dbms == 'fixed':
            dr = FixedReader(name=self.name, ds_layout=self.ds_layout,
                 lspec=self.lspec)
            
        elif self.dbms == 'fixed2':
            dr = FixedReader2(self.name, self.lengths, self.fieldnames, 
                 lspec=self.lspec)

        elif self.dbms == 'excel_srcn':
            dr = SheetDictReader(self.sheet)
        elif self.dbms == 'pyodbc':
            dr = PyodbcReader(conn=self.conn, cursor=self.cursor, 
              query=self.query, table=self.table, 
              column_names=self.column_names, 
              od_column_type=self.od_column_type,
              verbosity=self.verbosity)
        else:
            raise ValueError("dbms=%s is not supported" % self.dbms)
        # Could add check here on dr that its column names match 
        # self.column_names if we require self.column_names someday for 
        # init of dict readers... or perhaps 
        # to override the column names in the csv file.
        return dr
    
    def dict_reader(self):
        """ Name dict_reader() to be deprecated and replaced by DictReader().
            DictReader() is a more appropriate name, as it really creates an 
            object, so it is like a class constructor.
        """
        return(self.DictReader())
    
    def DictWriter(self, column_names=None, od_column_type=None, 
        default_column_spec=None, replace=None, db_context=None, 
        db_table_spec=None, verbosity=None):
        """
        Create and return dict writer object that writes rows of data to 
        the Dataset's file.
        
        Extended Summary:
        =================
        
        Parameters:
        ===========
        column_names: list of strings
        -----------------------------
        -- Optional. If given, the strings are all column names, in order, 
           for output data rows.
        -- If this is not given, then od_column_type must be given.
        
        od_column_type: ordered dictionary
        ----------------------------------
        . . .
        See more documentation details in the clause below for each 
        self.dbms type that initializes its style of DictWriter.
        
        Return:
        =======
        Returns a DictWriter object that has two methods, writeheader() and 
        writerow() that work like those in  "csv" style DictWriter objects.
        The writeheader(columns=['colname1','colname2'...] )
        (1) Records in self.column_names then names and order they will be 
            written per output row
        (2) It might create a table or write an initial output line of the 
            columns.
        
        The writerow(row=my_dict) method
        (1) accepts  parameter row, a dictionary of key(column_name)-value
            (column_value) pairs and, 
        (2) for each of the self.column_names in order, it looks up the 
            column_name in the parameter "row" and 
        (3) writes the row dictionary value found there (or nothing if not 
            found) to the output file. 
        (4) if it is an ordinary output file, it writes a newline to the 
            file after it has looped through every column name in the 
            initial list. 
        - If the user provides a "row" dictionary with key names that are 
          not in self.column_names, those entries will simply be ignored 
          when writerow() writes 
          a row.
        """
        iam = inspect.stack()[0][3]
        
        verbosity = (
          verbosity if verbosity is not None else self.verbosity)
        if verbosity:
            print ("%s: dsw (self) =%s" % (iam, repr(self)) )
        if self.open_mode != 'wb':
            raise ValueError(
              "%s: Dataset name=%s, mode is '%s', cannot write it."
              % (iam, self.name, self.open_mode))
        if replace == None:
            # Defer to self.replace value, if given, otherwise
            # like SAS, False is chosen for safety.
            replace = (
              self.replace if self.replace is not None else False)
        if default_column_spec is None:
            default_column_spec = 'nvarchar(128)'
            
        csv_dbmses = ('csv', 'hvp', 'tvp')
        
        if (self.dbms in csv_dbmses ):
            # csv-style DictWriter object. See python docs for csv package.
            column_names = ( 
              self.column_names if column_names is None 
              else column_names)
            od_column_type =(
               self.od_column_type if od_column_type is None 
               else od_column_type)
                        
            if (column_names is None):
                if od_column_type is None:
                    raise ValueError(
                      "dbms='%s', requires parameter column_names "
                      "or od_column_type" % self.dbms )
                else:
                    #glean column_names from od_column_type
                    column_names = []
                    for cn in od_column_type.keys():
                        column_names.append(cn)
            if (self.dbms == 'tvp'):
                if od_column_type is None:
                    # we populate od_column_type from default_column_spec
                    if (default_column_spec is None):
                        raise ValueError(
                          "dbms='%s', requires parameters column_names and "
                          "default_column_spec "
                          "or parameter od_column_type" % self.dbms )
                    else:
                        # build od_column_type 
                        od_column_type = OrderedDict()
                        for cn in column_names:
                            od_column_type[cn] = default_column_spec
                        self.od_column_type = od_column_type
                else:
                    # We have od_column_type. If we also have column_names,
                    # then create a new od_column_type2 dictionary using
                    # the column_names order and use default_column_spec 
                    # for column_names not listed in given od_column_type.
                    # Here we merely regard od_column_type as a dictionary,
                    # ignoring any order that it has.
                    if column_names is not None:
                        od_column_type2 = OrderedDict()
                        for cn in column_names:
                            ct = od_column_type.get(cn, default_column_spec)
                            od_column_type2[cn] = ct
                    #Overwrite the od_column_type OrderedDict
                    od_column_type = od_column_type2
                # end else -- here od_column_type is not none
            # end block: if (dbms == 'tvp')                        
            self.column_names = column_names
            self.od_column_type = od_column_type
                
            if verbosity:
                print ("%s: dbms='%s', using column_names='%s'" 
                    % (iam, self.dbms, repr(self.column_names)))
                print("%s: using od_column_type='%s'" 
                       % (iam, repr(self.od_column_type)))
                print ("%s: using csvfile='%s'" % (iam,repr(self.csvfile)))

            # May insert 'normalizing loop' for all column names here, 
            # as done by DictReader, if need arises. For now, caller can 
            # do it, but if it becomes a frequent need, then provide 
            # it here as a service, perhaps conditional on
            # the setting of a new boolean parameter.
            dw = csv.DictWriter(self.csvfile, 
              delimiter=self.delimiter,
              fieldnames=column_names, extrasaction="ignore")
            
            dw.dsw = self
            self.header_name="Not Used"
            # save header file name
            if (self.dbms == 'hvp'):
                # Compose the header file name
                base = os.path.splitext(self.name)[0]
                self.header_name = base + ".hdr" 
                dw.verbosity = verbosity
                dw.writeheader = types.MethodType(hvp_writeheader, dw)
            elif (self.dbms == 'tvp'):
                # Compose the column types filename
                base = os.path.splitext(self.name)[0]
                self.header_name = base + ".cty" 
                dw.verbosity = verbosity
                # tvp_writeheader uses od_column_type to write header
                dw.writeheader = types.MethodType(tvp_writeheader, dw)
                                    
            if verbosity and self.dbms in ['hvp','tvp']:
                print ("%s: dbms='%s', set header_name=%s" 
                    % (iam, self.dbms, self.header_name))
                print (
                    "%s:csv dict writer: column_names='%s'" 
                    % (iam,repr(column_names)))
        # end block: if self.dbms in ('csv','hvp',tvp')

            """    
            elif self.dbms == 'fts':
            dw = FastTableStreamWriter(      
              db_context = self.db_context, db_table_spec=self.db_table_spec, 
              schema=None, 
              buffer_size=None, use_names=False, raw=None)
            """
        elif self.dbms == 'pyodbc':
            dw = PyodbcWriter(
              conn=self.conn, cursor=self.cursor, table_name=self.table,
              column_names=column_names, od_column_type=od_column_type, 
              default_column_spec=default_column_spec, replace=replace
              ,verbosity=verbosity)
        else:
            self.dbms = "" if self.dbms is None else self.dbms
            raise ValueError(
              "Dataset.DictWriter(): dbms='%s' is not supported" % self.dbms)
            return None
        return dw
    
    def dict_writer(self, column_names):
        """ dict_writer name to be deprecated in favor of DictWriter """
        return(self.DictWriter(column_names))
    # end def DictWriter
    
#end ================ class DataSet() =======================================

        
def data(dsr=None, dsw=None, id_new_name=None, dict_col_name=None
      ,default_column_spec=None, od_column_type=None, unsafecopy=False
      ,rows_chunk=None, verbosity=None):
    """ Copies the dsr (readable) input dataset to output dsw (writable).
    Parameters:
    ===========
    dsr : Dataset(...open_mode='rb'...)
    -----------------------------------
    - Readable dataset with data that will be copied to the output dataset
    
    dsw : Dataset(...open_mode='wb'...)
    -----------------------------------
    - Writable dataset to which the data from dsr will be copied
    
    id_new_name : String (optional)
    -------------------------------
    - Name of a new column to append to the output dataset that will be 
      populated with an incrementing output row id value, starting with 
      id value 1. 
    - The output rows will be ordered in the same order as the received 
      input rows.
    
    dict_col_name: dict
    -------------------
    -- dictionary of old_name:new_name for output column
    -- user may specify one or more source column names to rename on output. 
    -- Eg, rename where some spreadhsheets use name "end" that SQL Server 
       rejects.
    
    od_column_type: dict
    --------------------
    -- dictionary of output column name keys, and value is a spec (eg, integer, 
       nvarchar(128), etc) to use (by a dbms style that supports sql table 
       creation: eg, pyodbc, maybe others) to create that column in the 
       output dataset.
    -- If parameter dict_col_name is used, the 'new name' should be a key here, 
       rather than the old name.
    -- This dictionary may be missing or list only a subset of the columns 
       to be output, because if an output column is not found here, a default 
       column type will be given.
    
    default_column_spec: string  (optional)
    ---------------------------------------
    -- the default column spec to use in a table-like dataset, if the 
       going default is not desired. 
    -- Normal efault is 'nvarchar(7999)' because it uses no more room 
       than nvarchar(x) for x < 8000 for the same data value, per some SQL 
       Server 2012 docs. UPDATE: it is now 3999 because SQL EXPRESS max is 
       3999 and we have no columns bigger than that now, so just go with it.
    
    """
    iam = inspect.stack()[0][3]

    if (id_new_name is not None and dict_col_name is not None):
        raise ValueError(
          "Cannot use both id_new_name and dict_col_name in one call.")
    if dsr is None or dsw is None:
        raise ValueError("Both dsr and dsw keyword params must be set")
    if verbosity:
        print "data(): dsr=%s, dsw=%s" % (repr(dsr), repr(dsw))
    reader = dsr.DictReader()
   
    # If dsr has odFirst set od_column_type to that from dsr, if it has that
    if hasattr(reader, 'od_column_type') and reader.od_column_type is not None:
        output_od_column_type = reader.od_column_type
        if verbosity:
            print ("data(): reader has od_column_type")
        # Augment the dict with info from any argument od_column_type
        if od_column_type is not None:
            if verbosity:
                print ("data(): But using param od_column_type")

            #Future: augment output_od_column_type here with 
            # info in parameter od_column_type.
            output_od_column_type = od_column_type
            pass
    else:
        output_od_column_type = od_column_type
        
    # convey the input column types, possibly revised, to output.
    if verbosity:
        print ("data(): using od_column_type=%s" % repr(output_od_column_type ))
    dsw.od_column_type = output_od_column_type
    if rows_chunk is None:
        rows_chunk = 500
        
    out_column_names = []
    if id_new_name is not None:
        out_column_names.append(id_new_name)
    # Use column_names from the reader    
    out_column_names.extend(reader.fieldnames)
    if verbosity:
        print ("data(): out_column_names='%s'" 
           % repr(out_column_names))
    if dict_col_name is not None:
        # rename given output column names
        for idx, cname in enumerate(out_column_names):
            if dict_col_name.get(cname) is not None:
                # cname is a replacement column name to use.
                out_column_names[idx] = dict_col_name[cname]
               
    writer = dsw.DictWriter(column_names=out_column_names
      ,default_column_spec=default_column_spec
      ,od_column_type=output_od_column_type, verbosity=verbosity)
    
    writer.writeheader()
    #reader = dsr.DictReader()
    
    # Select and run a data read-write loop.
    # This code has multiple output loops to save a bit of time by 
    # not having the outer conditional checks within a single read-write loop
    # that may encounter hundreds of thousands, if not millions, of rows.
   
    if dict_col_name is not None:
        # Do column name replacement on output.
        ispow = isinstance(writer, PyodbcWriter)
        if ispow:
            # PyodbcWriter supports faster writing by a simple list of values.
            # Read-write loop. 
            for idx, row in enumerate(reader, start=1):
                #if (idx < 10):
                #   print ("data1: writerow idx=%d, row_columns='%s'" 
                #   % (idx, repr(row.itervalues())))
                if verbosity and (idx % rows_chunk == 0):
                    print "l1:Outputted row %d" % idx
                
                out_column_values=[]
                out_column_values.extend([ row[x] for x in reader.fieldnames])
                writer.writerow(row_columns=out_column_values)
        else:
            # This writer is not the pyodbc writer
            # Read-write loop.
            for idx, row in enumerate(reader, start=1):
                # Writer expects a dict to output so we must populate it.
                if verbosity and idx % rows_chunk == 0:
                    print "l2:Outputted row %d" % idx
                
                orow = OrderedDict()
                orow = zip(
                  out_column_names, [row[x] for x in reader.fieldnames])
                if verbosity:
                    print ("data7: writerow idx=%d, row='%s'" % (idx, repr(orow)))
                writer.writerow(orow)
            
    elif id_new_name is not None:
        # No column renames, but generate an id.
        # Read-write loop: also add a simple id column to output
        for idx, row in enumerate(reader, start=1):
            if verbosity and idx % rows_chunk == 0:
                print "l3:Outputted row %d" % idx
            orow = row.copy()
            orow[id_new_name] = repr(idx)
            #print ("data3: writerow idx=%d, row='%s'" % (idx, repr(orow)))
            writer.writerow(orow)
    elif unsafecopy:
        writer.copyreader_unsafe(reader) 
    else:
        for row in reader:
            #print ("data2: writerow idx=%d, row='%s'" % (idx, repr(row)))
            writer.writerow(row)
    
    # close the underlying files
    if dsw.csvfile is not None:
        dsw.csvfile.close()
    return out_column_names

    # end def data() ------------------------------------------

import struct

class FixedReader(object):
    def __init__(self, name=None, ds_layout=None, lspec=None):
        if lspec is None:
            lspec = 0
        if ds_layout is None or name is None:
            raise ValueError("Both name and ds_layout must be specified")
        drlo = ds_layout.DictReader()
        required_fieldnames=['length','name']
        for field in required_fieldnames:
            if field not in drlo.fieldnames:
                raise ValueError(
                  "Missing required fieldname='%s' in ds_layout='%s'"
                  % (field, repr(ds_layout)))
        self.format_string = ""
        self.fieldnames=[]
        self.odict = OrderedDict()
        self.lines_read = 0
        self.nfields = 0;
        self.line_length = 0
        self.name=name
        for row in drlo:
            name = row['name']
            self.fieldnames.append(name)
            self.odict[name] = ""
            length=int(row['length'])
            self.nfields += 1
            self.line_length += length
            self.format_string += ("%ds" % (length))
        #Create a struct-based line parser
        self.parse = struct.Struct(self.format_string).unpack_from
        # open the input file
        self.file = open(self.name,mode='rb')
        
    def __getitem__(self, index):
        
        # Populate the dict with the file's next line of fixed column values
        # (stripped) and return the dict.
        line = self.file.readline()
        if line is None or len(line) == 0:
            raise IndexError
        self.lines_read +=1
        # Got a line, so parse out the fields
        fields = self.parse(line)
        
        # Populate dict of column_name:values from row.
        # All data values are normalized to stripped string.
        for key,val in zip(self.odict.iterkeys(), fields):
            #print ("Got row: key='%s', val='%s'" % (key,val.strip()))
            self.odict[key] = val.strip()
        return self.odict  
    
    def __repr__(self):
        return ("%s: name=%s\n\tds_layout=%s." 
          % (self.__class__.__name__, 
             repr(self.query),
             repr(self.ds_layout), 
            ))
        
# end ====================== class FixedReader() =================
        
class FixedReader2(object):
    def __init__(self, filename, lengths, fieldnames, lspec=0):
        self.format_string = ""
        self.fieldnames=fieldnames
        self.odict = OrderedDict()
        for name in fieldnames:
            self.odict[name] = ""
        self.lines_read = 0
        for length in lengths:
            self.format_string += ("%ds" % (length))
        #Create a struct-based line parser
        self.parse = struct.Struct(self.format_string).unpack_from
        # open the input file
        self.file = open(filename,mode='rb')
    def __getitem__(self, index):
        
        # Populate the dict with the file's next line of fixed column values
        # (stripped) and return the dict.
        line = self.file.readline()
        if line is None or len(line) == 0:
            raise IndexError
        self.lines_read +=1
        # Got a line, so parse out the fields
        fields = self.parse(line)
        
        # Populate dict of column_name:values from row.
        # All data values are normalized to stripped string.
      
        for key,val in zip(self.odict.iterkeys(), fields):
            #print ("Got row: key='%s', val='%s'" % (key,val.strip()))
            self.odict[key] = val.strip()
        return self.odict  
    
    def __repr__(self):
        return ("%s: filename=%s\n\tds_layout=%s." 
          % (self.__class__.__name__, 
             repr(self.query),
             repr(self.ds_layout), 
            ))
# end ================ class FixedReader2() ===============================


class FastTableStreamWriter(object):
    
    def __init__(self,
      conn=None, cursor=None, table_name=None, columns=None,
      replace=None,
      nvarchar_fixed_size=128):
        """
NOTE::: not ready for primetime... Code is just pushed here so it does not get
lost in case an effort to pick up this work direction is undertaken.

Create a table with AIR Utilities for FastTable processing and return a writer with method 
writerows() to write rows to it.

Consider: rather than here, create the table in Dataset's __init__ and allow multiple 
writers to exist that can open their own cursors to race or operate in parallel to insert 
their own rows into the table. 

Along the same lines, but independent issue; consider allowing an option to 
specify appending to an extant table.
For example, copy the SAS param name "replace", and if 1 then create else assume 
table exists for appending...

Parameters:
===========
conn : pyodbc connection object

cursor: pyodbc cursor object

table_name: string 
- Name of database table to which to write

columns: list of strings
- list of column names of the table. If no column_types are
  given, the columns are all of type nvarchar().
        """
        self.conn = conn
        self.cursor = cursor
        
        if table_name is None:
            raise ValueError("table_name must be given")
        self.table_name = table_name
        
        if columns is None:
            raise ValueError("columns[] must be given")
        self.odict = OrderedDict()
        # set up initial official column order in odict
        for column in columns:
            self.odict[column] = ""
        self.columns = columns
        self.fieldnames = columns
        replace = False if replace is None else replace
        self.nvarchar_fixed_size = (
          128 if nvarchar_fixed_size is None else nvarchar_fixed_size)
        
        # create a new table with the given column names of generic string type.
        column_specs = ""
        columns_string = ""
        qmarks = ""
        delim = ""
        for cn in columns:
            cs = "%s%s nvarchar(%d)" % (delim, cn, self.nvarchar_fixed_size)
            column_specs += cs
            columns_string += delim + cn
            qmarks +=  delim + "?"
            delim = ", "
        
        sql_create = "create  table %s ( %s )" % (table_name, column_specs)
        #print ("sql_create = '%s'" % sql_create)
        
        self.cursor = self.conn.cursor()
        
        #create the table
        if replace == True:
            # try to delete the table. If cannot delete, no worries.
            try:
                sql_delete = "drop table %s" % table_name
                self.cursor.execute(sql_delete)
                self.conn.commit()
            except: 
                pass
                
        self.cursor.execute(sql_create)
        self.conn.commit()
        
        # create the insertion sql
        self.insert_sql = (
          "insert into %s(%s) values (%s)" %
          (table_name, columns_string, qmarks)
          )
        # print ("insert_sql= '%s'" % self.insert_sql)
        
    def writerow(self, row=None, row_columns=None):
        
        """ Write a row of data to a table.

        Parameters:
        ===========
        dict_row: dictionary 

           - A dictionary whose keys are column names and values are string values to write to a new row in the table.
           - Either this parameter or parameter row must be provided, but not both.
           - An error is raised if a key in this dictionary is not in self.columns. 
           - If not all columns in self.columns exist in the dictionary, 
             the result of the operation is undefined or an exception may be raised.

        row: list

            - A list of string values to write a new row to the table. The column order given in self.columns is written.
              An error is raised if too many or too few string values exist in the list.
        """
        if row_columns is not None:
            self.cursor.execute(self.insert_sql, *row_columns)
        elif row is not None:
            # Use odict to order the row column values for writing
            for idx,(key,val) in enumerate(row.iteritems()):
                self.odict[key] = val;
            row_columns = [val for key,val in self.odict.iteritems()]
            # print "row_columns='%s'" % repr(row_columns)
            self.cursor.execute(self.insert_sql, *row_columns)
        else:
            raise ValueError("Either row or dict_row must be given")
      
        return
    
    def writeheader(self,row=None):
        # simply return - the 'header' columns are already incorporated in
        # the schema of this table to which to write rows.
        return
    
# end ===============   class FastTableStreamWriter =====================

if  __name__ == "__main__" and 1 == 2:
    # local temporary testing area...
    #
    
    print "Testing fixed reads..."
    ddir='C:/users/temp_rphillips/'
    ds_layout = Dataset(name=ddir+'layout.csv', open_mode='rb')
    ds_fixed = Dataset(dbms='fixed',ds_layout=ds_layout, open_mode='rb',
        name=ddir+'fixed.txt')  
    dr = ds_fixed.DictReader()
    for row in dr:
        print "row = %s" % repr(row)
        
    #print "Testing dict_col_name substitutions"
    import os
    #from airassessmentreporting.datacheck.dataset import *
    server='DC1PHILLIPSR\SQLEXPRESS'
    db='testdb'
    home = os.path.expanduser("~")+ os.sep
    tddir = home+"testdata/intake_local/"
    fnr_intake_layout = tddir + 'OGT_SP12_Op_DataLayout_IntakeLayout_local.xls'
    dsr_intake_layout = Dataset(open_mode='rb',dbms='excel_srcn',
      workbook_file=fnr_intake_layout)
    dsw_intake_layout = Dataset(open_mode='wb',dbms='pyodbc',
      server=server,db=db,table="rvp_intake_recoding_layout",replace=True)
    data(dsr=dsr_intake_layout,dsw=dsw_intake_layout,
         dict_col_name={'end':'endpos', "min":'vmin', 'max':'vmax'})
    
