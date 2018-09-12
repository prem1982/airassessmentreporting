import os.path
import re
import xlwt
import xlrd
import string
from decimal import Decimal
import pyodbc
from string import Template
import codecs
from airassessmentreporting.airutility import *

###############################################################
# Changes to be made:
#      Add more informats from SAS to dictionary inside
#        read_old_layoutfile
#      
###############################################################

__all__ = [ 'Layout', 'create_sample_layout', 'read_old_layoutfile', 'read_new_layoutfile',
            'check_value_against_layout', 'Intake' ]

def create_us_datestring(inString):
    "This function takes a string of the form mmddyyyy and turns it into mm/dd/yyyy"
    inString = str(inString)
    retString = inString[:2] + '/' + inString[2:4] + '/' + inString[4:]   
    return retString

def return_same_string(inString):
    "This function just returns the same string it is passed"
    return str(inString)

#this Layout class holds a row from the layout excel file. A dictionary of them,
# with the key being the varname, will hold all the lines of the file
class Layout:
    "This class represents a single line of a layout file for the intake function"
    varname = ''
    vartype = ''
    length = 0
    start = 0
    end = 0
    informat = ''
    regex = ''
    minval = 0
    maxval = 0
    keep_in_error_ds = 0
    checkmin = False
    checkmax = False
    convert_informat = return_same_string
    def __init__(self):
        self.varname = ''
        self.vartype = ''
        self.length = 0
        self.start = 0
        self.end = 0
        self.informat = ''
        self.regex = ''
        self.minval = 0
        self.maxval = 0
        self.keep_in_error_ds = 0
        self.checkmin = False
        self.checkmax = False
        self.convert_informat = return_same_string
    
#this function creates a layout skeleton with the correct column names
def create_sample_layout(Sheetname,isFixedWidth, Layoutfile,CompMode):
    "Create a sample layout file (Must be .xls, not .xlsx)"
    oldHeaders = ['varname','type','length','informat','perl_expression','min','max']
    fixedWidthHeaders = ['start','end']
    newHeaders = ['varname','type','length','informat','regex','min','max']
    font0 = xlwt.Font()
    font0.name = 'Times New Roman'
    font0.colour_index = 2
    font0.bold = True
    style0 = xlwt.XFStyle()
    style0.font = font0
    wb = xlwt.Workbook()
    ws = wb.add_sheet(Sheetname)
    if CompMode:
        if isFixedWidth:
            oldHeaders += fixedWidthHeaders
        for header in oldHeaders:
            ws.write(0,oldHeaders.index(header),header,style0)
    else:
        if isFixedWidth:
            newHeaders += fixedWidthHeaders
        for header in newHeaders:
            ws.write(0,newHeaders.index(header),header,style0)        
    wb.save(Layoutfile)

#this function reads a layout excel file for SAS and returns a list of layout objects
# This list will contain the entire layout excel file.
def read_old_layoutfile(LayoutFileName,Sheet,isFixedWidth):
    """ This function reads in the old SAS layout file and returns a list of Layout objects. 
        A Layout object is a single row in the layout file.It error checks the layout as it 
        iterates.
    """
    symbols=['!','%','^','&','*','(',')','-','+','=','`','~',
             '[',']','{','}','\\','|',';',':','"','\'','<',',','>','.','/',
             '?']
    invalidfirstchars=symbols + ['$'] + ['1','2','3','4','5','6','7','8','9','0']
    returnlayoutlist = []
    #informat dictionary of SAS informats. The key is the informat, value[0] is the function pointer, 
    #    value[1] is the informat code, value[2] is the vartype
    informatDict = {"mmddyy8.":[create_us_datestring,101,'Datetime']}
    #setup mapping for each headers column number, if -1 then it doesn't appear 
    #NOTE: This is no longer used. But it does show the possible columns.
    colmapping = {"length":-1,"varname":-1,"type":-1,"perl_expression":-1,
                  "min":-1,"max":-1,"informat":-1,"start":-1,"end":-1,
                  "keep_in_error_ds":-1}
    reader = SafeExcelReader(None,filename=LayoutFileName,sheet_name=Sheet)
    #go through rows of sheet starting at 0
    rownum = 0
    for row in reader.getRows():
        new_layout = Layout()
        #change keys in dictionary to lowercase
        #cols = {name.lower():row[name] for name in row}
        cols = {}
        for elem in row.keys():
            cols[elem] = row[elem]
            if row[elem] is None:
                cols[elem] = ""
        #only adding those lines that contain a variable name
        lenMissing = False
        if "varname" in cols:
            if "length" in cols:
                leng = cols["length"]
                if leng == "":
                    new_layout.length = 8
                    lenMissing = True
                else: new_layout.length = int(leng)
            else:
                raise Exception("ERROR: Variable length does not exist in the layout file.")
            new_layout.varname = cols["varname"]
            if len(new_layout.varname) == 0:
                raise Exception("ERROR: Invalid variable name. No varname specified at row " + str(rownum))
            #check varname against invalid characters for sql column names
            if len(new_layout.varname) > 128 or (new_layout.varname[0] in invalidfirstchars) or any(sym in new_layout.varname for sym in symbols):
                raise Exception("ERROR: Invalid variable name: " + new_layout.varname + " you cannot include symbols and the name must be < 128 characters long.")
            if "type" in cols:
                new_layout.vartype = cols["type"]
                if not new_layout.vartype in ['N','C']:
                    raise Exception("ERROR: N and C are the only valid values for type. type of "+ new_layout.varname +" must be either N or C.")
                if new_layout.vartype == 'N' and new_layout.length != 8:
                    raise Exception("ERROR: Numeric variables should have a length of 8 or missing value. The variable "+ new_layout.varname +" does not have a length of 8 or missing")
                if new_layout.vartype == 'C' and lenMissing:
                    raise Exception("ERROR: Character variables should have a length assigned. " + new_layout.varname + " does not have a length assigned.")
            else:
                raise Exception("ERROR: Variable type does not exist in the layout file.")
            if "perl_expression" in cols:
                #strip the leading and trailing / from the perl regex
                new_layout.regex = cols["perl_expression"].strip("/")
            if "keep_in_error_ds" in cols:
                errorCode = str(cols["keep_in_error_ds"])
                if not errorCode in ['1','0','']:
                    raise Exception("ERROR: 1,0 and Blank are the only valid values for Keep_In_Error_Ds. Keep_In_Error_Ds of " + new_layout.varname + " must be 1,0 or Blank.")
                if errorCode == '':
                    errorCode = '0'
                new_layout.keep_in_error_ds = int(errorCode)                    
            if "min" in cols:    
                minvar= str(cols["min"])
                if new_layout.vartype == 'N' and any(char in minvar for char in string.letters):
                    raise Exception("ERROR: min can have only numeric values")
                if minvar == '' or minvar == '.':
                    new_layout.checkmin = False
                else:
                    new_layout.checkmin = True
                    new_layout.minval = int(minvar)
            if "max" in cols:
                maxvar = str(cols["max"])
                if new_layout.vartype == 'N' and any(char in maxvar for char in string.letters):
                    raise Exception("ERROR: max can have only numeric values")
                if maxvar == '' or maxvar == '.':
                    new_layout.checkmax = False
                else:
                    new_layout.checkmax = True
                    new_layout.maxval = int(maxvar)
            if "informat" in cols:
                infmt = str(cols["informat"]).strip()
                fctnptr = return_same_string
                if infmt != '':
                    if infmt in informatDict:
                        fctnptr = informatDict[infmt][0]
                        new_layout.vartype = informatDict[infmt][2]
                        infmt = informatDict[infmt][1] # this must be after previous line because we're setting the value of infmt
                    else: raise Exception("ERROR: Informat not recognized - May need to add a case for informat: " + infmt)
                new_layout.informat = infmt
                new_layout.convert_informat = fctnptr
            if isFixedWidth:
                if "start" in cols:
                    new_layout.start = int(cols["start"])
                else:
                    raise Exception("ERROR: Variable start does not exist in the layout file.")
                if "end" in cols:
                    new_layout.end = int(cols["end"])
                else:
                    raise Exception("ERROR: Variable end does not exist in the layout file.")
                varlength = new_layout.end - new_layout.start+1
                if new_layout.vartype == 'C' and (not varlength > 0):
                    raise Exception("ERROR: Character variable "+ new_layout.varname +" has not been assigned enough length to accomodate the input. length: " + str(varlength))
            returnlayoutlist.append(new_layout)
        else:
            raise Exception("ERROR: Variable varname does not exist in the layout file.")
        rownum += 1
    return returnlayoutlist

#this function reads a layout excel file for SQL and list of Layout objects
#     This dictionary will contain the entire layout excel file
def read_new_layoutfile(LayoutFileName,Sheet,isFixedWidth):
    """ This function reads in a layout file in the new form for SQL and returns a list of 
        Layout objects. A Layout object is a single row in the layout file. It error checks 
        the layout as it iterates.
        
        The new layout has column name `regex` instead of `perl_reg_expression` and
        assumes it is a regex expression provided and not a perl regular expression
        (it doesn't have to remove the leading and trailing slashes).
    """
    symbols=['!','%','^','&','*','(',')','-','+','=','`','~',
             '[',']','{','}','\\','|',';',':','"','\'','<',',','>','.','/',
             '?']
    numericvartypes = ['INT','BIGINT','FLOAT','N']
    charvartypes = ['DATETIME','NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR','C']
    vartypes = numericvartypes + charvartypes
    invalidfirstchars=symbols + ['$'] + ['1','2','3','4','5','6','7','8','9','0']
    returnlayoutlist = []
    #setup mapping for each headers column number, if -1 then it doesn't appear in the layout
    colmapping = {"length":-1,"varname":-1,"type":-1,"regex":-1,
                  "min":-1,"max":-1,"informat":-1,"start":-1,"end":-1,
                  "keep_in_error_ds":-1}
    reader = SafeExcelReader(None,filename=LayoutFileName,sheet_name=Sheet)
    #go through rows of sheet starting at 0
    rownum = 0
    for row in reader.getRows():
        new_layout = Layout()
        #change keys in dictionary to lowercase
        #cols = {name.lower():row[name] for name in row} 
        cols = {}
        for elem in row.keys():
            cols[elem] = row[elem]
            if row[elem] is None:
                cols[elem] = ""   
        #only adding those lines that contain a variable name
        lenMissing = False
        if "varname" in cols:
            if "length" in cols:
                leng = cols["length"]
                if leng == "":
                    new_layout.length = 8
                    lenMissing = True
                else: new_layout.length = int(leng)
            else:
                raise Exception("ERROR: Variable length does not exist in the layout file.")
            new_layout.varname = cols["varname"]
            if len(new_layout.varname) == 0:
                raise Exception("ERROR: Invalid variable name. No varname specified at row " + str(rownum))
            #check varname against invalid characters for sql column names
            if len(new_layout.varname) > 128 or (new_layout.varname[0] in invalidfirstchars) or any(sym in new_layout.varname for sym in symbols):
                raise Exception("ERROR: Invalid variable name: " + new_layout.varname + " you cannot include symbols and the name must be < 128 characters long.")
            if "type" in cols:
                new_layout.vartype = cols["type"]
                if not new_layout.vartype.upper() in vartypes:
                    raise Exception("ERROR: int,bigint,datetime,nvarchar,varchar,char,nchar and float are the only valid values for type. type of "+ new_layout.varname +" must be fixed.")
                if new_layout.vartype.upper() in numericvartypes and new_layout.length != 8:
                    raise Exception("ERROR: Numeric variables should have a length of 8 or missing value. The variable "+ new_layout.varname +" does not have a length of 8 or missing")
                if new_layout.vartype.upper() in charvartypes and lenMissing:
                    raise Exception("ERROR: Character variables should have a length assigned. " + new_layout.varname + " does not have a length assigned.")
                if new_layout.vartype.upper() == 'DATETIME' and len(new_layout.informat) == 0:
                    raise Exception("ERROR: Datetime variables must have an informat specified. " + new_layout.varname + " does not have an informat specified.")
            else:
                raise Exception("ERROR: Variable type does not exist in the layout file.")
            if "regex" in cols:
                #strip the leading and trailing / in case its still a perl regex
                new_layout.regex = cols["regex"].strip("/")
            if "keep_in_error_ds" in cols:
                errorCode = str(cols["keep_in_error_ds"])
                if not errorCode in ['1','0','']:
                    raise Exception("ERROR: 1,0 and Blank are the only valid values for Keep_In_Error_Ds. Keep_In_Error_Ds of " + new_layout.varname + " must be 1,0 or Blank.")
                if errorCode == '':
                    errorCode = '0'
                new_layout.keep_in_error_ds = int(errorCode)                    
            if "min" in cols:    
                minvar= str(cols["min"])
                if new_layout.vartype in numericvartypes and any(char in minvar for char in string.letters):
                    raise Exception("ERROR: min can have only numeric values")
                if minvar == '' or minvar == '.':
                    new_layout.checkmin = False
                else:
                    new_layout.checkmin = True
                    new_layout.minval = int(minvar)
            if "max" in cols:
                maxvar = str(cols["max"])
                if new_layout.vartype in numericvartypes and any(char in maxvar for char in string.letters):
                    raise Exception("ERROR: max can have only numeric values")
                if maxvar == '' or maxvar == '.':
                    new_layout.checkmax = False
                else:
                    new_layout.checkmax = True
                    new_layout.maxval = int(maxvar)
            if "informat" in cols:
                new_layout.informat = cols["informat"]
            if isFixedWidth:
                if "start" in cols:
                    new_layout.start = int(cols["start"])
                else:
                    raise Exception("ERROR: Variable start does not exist in the layout file.")
                if "end" in cols:
                    new_layout.end = int(cols["end"])
                else:
                    raise Exception("ERROR: Variable end does not exist in the layout file.")
                varlength = new_layout.end - new_layout.start+1
                if new_layout.vartype in charvartypes and (not varlength > 0):
                    raise Exception("ERROR: Character variable "+ new_layout.varname +" has not been assigned enough length to accomodate the input. length: " + str(varlength))
                new_layout.convert_informat = return_same_string
            returnlayoutlist.append(new_layout)
        else:
            raise Exception("ERROR: Variable varname does not exist in the layout file.")
        rownum += 1
    return returnlayoutlist

def check_value_against_layout(elem,lineNum,layout,db_context,Error_Ds):
    """ This function checks the element against the layout values for the regular expression, 
        min, and max values. If it fails it adds it to the error SQL table.    
        
        Parameters
        -------------------
        elem : ?
            This is the current value we are checking against the layout. It could be a string, it could
            be a number. 
        
        lineNum : int
            This is the line number in the data file. This is used for error reporting purposes.
        
        layout : Layout object
            This class is defined in the top of this script. This is the layout that contains the values
            and specifications for error checking the values in the data file.
        
        db_context : DBContext object
            This is the DBContext object which we will run queries on
        
        Error_Ds : String
            This is the name of the error table to insert errors into.
    """
    insert_to_errords = "INSERT INTO " + Error_Ds + " VALUES ("
    #check values against layout spec
    if layout.regex != "":
        ptrn = re.compile(layout.regex)
        if not ptrn.match(elem): #didn't match regular expression so add to error table
            query = insert_to_errords + str(lineNum)+",'"+layout.varname+"','"+str(elem)+"','Regular Expression')"
            db_context.executeNoResults(query)        
    if layout.checkmin: # if supposed to check the minimum value
        if Decimal(elem) < layout.minval: # if failed min value test add to error table
            query = insert_to_errords + str(lineNum)+",'"+layout.varname+"','"+str(elem)+"','Min value check')"
            db_context.executeNoResults(query)
    if layout.checkmax: #if supposed to check maximum value
        if Decimal(elem) > layout.maxval: # if failed max value test add to error table
            query = insert_to_errords + str(lineNum)+",'"+layout.varname+"','"+str(elem)+"','Max value check')"
            db_context.executeNoResults(query)


#Definition of the Intake function
def Intake(Mode='Run',     # Can have values of run or create. Run is the default option. Create will create a skeleton layout for the user to fill
           Type_of_Datafile='FixedWidth', # Type of values accepted are fixedwidth, delimited, or excel
           Delimiter=",",  # "," will be the default delimiter.delimiter used in the data. wrap the delimiter with double quotes
           Layoutfile='',  # layoutfile location. The layout should be a .xls or .xlsx file (must be .xls when mode = 'Create')
           Sheetname='',   # The sheetname on the layoutfile
           Infile='',      # Location of the input data
           Infile_Excel_Sheetname='', # Sheetname for input excel file. Only used when reading an excel file
           Outdata='',     # Name of the output table
           db_context=None, # this is the DBContext in which the script will run
           Error_Ds='',    # Name of error table to hold errors with data (layout errors throw exceptions)
           firstobs=2,     # Used for delimited files. Set to 1 if there are no headers
           Getnames='Yes', # Only for Excel or delimited data files, Yes if data has column names, No if it does not
           Overwrite='No', # Yes to overwrite out table and error table without question, false to raise error if they exist
           CompatibilityMode='Yes', # Yes if it is the old SAS style layout, No if it is the new SQL layout
           encoding='ascii'# Encoding used for data file. Default is ASCII, but can specify 'utf-8' etc.
           ):
    """ This function takes a data file, checks it against a layout file, then uploads it into a SQL table.
    
        Extended summary
        -------------------------
        This function does the calculations specified in the means and percentages layout file.
        
        Parameters
        --------------     
        Mode : String ('Run' or 'Create')
            This must be either 'Run' or 'Create'. Run is the default option. Create will create a 
            skeleton layout for the user to fill in. Run will actually do the import and error check.
             
        Type_of_Datafile : String ('Fixedwidth', 'Delimited', or 'Excel')
            This specifies the format of the data file. Accepted values are are 'fixedwidth', 'delimited', 
            or 'excel'.
            
        Delimiter : String
            If `Type_of_Datafile`='delimited' then this must be specified. This is used in a regular expression
            inside of square brackets [], so be sure it doesn't clash with any regex special characters
            when used in them.
        
        Layoutfile : String
            Layout filename and location. It should be a .xls or .xlsx file (must be .xls when Mode='Create').
            If `Mode`='Create' this is the name of the layout skeleton file to create.
            
        Sheetname : String
            This should be the name of the sheet to get data from inside the layout file specified.
            
        Infile : String
            This should be the name and location of the input data file.
            
        Infile_Excel_Sheetname : String
            This should be the name of the sheet that holds the data to get from the `Infile`. This is only
            used when `Type_of_Datafile`='Excel'.
            
        Outdata : String
            This is what the output SQL table will be named.
            
        db_context : DBContext object
            This is the DBContext within which all processing will be done. This specifies the DB connection.
            
        Error_Ds : String
            This is what the SQL table will be named that holds errors that occur during the run.
            
        firstobs : int
            Used only when `Type_of_Datafile`='Delimited'. This is the line number of the first line of data 
            (note: it is 1 based, not zero based). Default value = 2 (it assumes there are headers on the first 
            line). Set to 1 if there are no headers.
            
        Getnames : String ('Yes' or 'No')
            Only for Excel or delimited data files, 'Yes' if data has column names, 'No' if it does not.
            Default value is 'Yes'.
            
        Overwrite : String ('Yes' or 'No')
            This should be either 'Yes' to overwrite the output table, or 'No' to throw an error if the table already
            exists. Default value is 'No'.
            
        CompatibilityMode : String ('Yes' or 'No')
            This should be 'Yes' or 'No'. 'Yes' if it is the old SAS style layout, 'No' if it is the new SQL layout.
            
        encoding : String ('ascii','utf-8', etc.)
            This specifies the encoding of the data file. The default is 'ascii'.

        Returns
        ----------
        Nothing.     
        
        Notes
        --------
        Many of the parameters are 'Yes' or 'No' parameters. These should be updated to be True/False (boolean).
        If you specify `Mode`='Create' then only the `Layoutfile` and `CompatibilityMode` need to also be specified.
    """
    yes_list = ['YES','Y']
    numericvartypes = ['INT','BIGINT','FLOAT', 'N']
    charvartypes = ['DATETIME','NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'C'] # character variable types
    vartypes = numericvartypes + charvartypes    
    #error checking
    if not Mode.upper() in ['RUN','CREATE']:
        raise Exception("The valid parameters for Mode are CREATE and RUN.")
    if Layoutfile == "":
        raise Exception("ERROR: The Layoutfile parameter cannot be empty.")
    if not '.xls' in Layoutfile:
        raise Exception("ERROR: The layoutfile should be an excel file with an extention of .xls or .xlsx.")
    #os.path.isfile may be dangerous? need to lock file first?..
    if Mode.upper() == "CREATE":
        if os.path.isfile(Layoutfile):
            raise Exception("ERROR: Layout file already exists. Rename or give a different filename for the skeleton layout.")
        #create sample layout file in excel format
        #need xlwt to do this
        compmode = True
        if CompatibilityMode.upper() in yes_list:
            compmode = True
        else: compmode = False
        if Type_of_Datafile.upper() == "FIXEDWIDTH":
            create_sample_layout(Sheetname,True,Layoutfile,compmode)
        else: create_sample_layout(Sheetname,False,Layoutfile,compmode)
    elif Mode.upper() == "RUN":
        #error checking
        if not Type_of_Datafile.upper() in ['FIXEDWIDTH','DELIMITED','EXCEL']:
            raise Exception("ERROR: The valid parameters for  Type_of_Datafile are FIXEDWIDTH, DELIMITED and EXCEL.")
        if Sheetname == "":
            raise Exception("ERROR: The Sheetname parameter cannot be empty.")
        if Type_of_Datafile.upper() =="DELIMITED" and Delimiter == "":
            raise Exception("ERROR: For a delimited file the delimiter can not be blank.")
        if Infile == "":
            raise Exception("ERROR: The Infile parameter cannot be empty.")
        if Outdata == "":
            raise Exception("ERROR: The Outdata parameter cannot be empty.")
        if Error_Ds == "":
            raise Exception("ERROR: The Error_Ds parameter cannot be empty.")
        if db_context is None:
            raise Exception("ERROR: The db_context parameter cannot be None.")
        run_context = db_context.runContext
        #Possibly bad, again using os.path.isfile()
        if not os.path.isfile(Infile):
            raise Exception("ERROR: Input data file",Infile,"does not exist.")
        #Possibly bad, again using os.path.isfile()
        if not os.path.isfile(Layoutfile):
            raise Exception("ERROR: Layout file",Layoutfile,"does not exist.")
        if Type_of_Datafile.upper() == "EXCEL":
            if not ".xls" in Infile:
                raise Exception("ERROR: The Input file should be an excel file with an extention of .xls or .xlsx. when Type_of_Datafile=EXCEL")
            if Infile_Excel_Sheetname == "":
                raise Exception("ERROR: The Infile_Excel_Sheetname parameter cannot be empty when the Type_of_Datafile is EXCEL.")
        layoutlist = []
        try:            
            if Type_of_Datafile.upper() == "FIXEDWIDTH":
                if CompatibilityMode.upper() in yes_list:
                    layoutlist = read_old_layoutfile(LayoutFileName=Layoutfile,Sheet=Sheetname,isFixedWidth=True)
                else: layoutlist = read_new_layoutfile(LayoutFileName=Layoutfile,Sheet=Sheetname,isFixedWidth=True)
            else: 
                if CompatibilityMode.upper() in yes_list:
                    layoutlist = read_old_layoutfile(LayoutFileName=Layoutfile,Sheet=Sheetname,isFixedWidth=False)
                else: layoutlist = read_new_layoutfile(LayoutFileName=Layoutfile,Sheet=Sheetname,isFixedWidth=False)                
        except IOError as ioer:
            raise Exception("Problem opening Layout file: " + ioer.strerror)
#        except Exception as excpt:
#            raise Exception(excpt.args[0] + """
#            ERROR: For a FIXEDWIDTH file "varname type length start end" are required variables in the layout;
#            ERROR: For a DELIMITED and EXCEL file "varname type length " are required variables in the layout;
#            ERROR: Keep_In_Error_Ds regex, informat, min and max are optional variables for both FIXEDWIDTH DELIMITED and EXCEL file;""")
        #Now that we've processed the layout without errors we connect to SQL and create the tables     
        varslist = ''
        #build var list for create table query
        for layout in layoutlist:
            varslist += layout.varname
            if layout.vartype.upper() == 'C':
                layout.vartype = 'NVARCHAR'
            elif layout.vartype.upper() == 'N':
                layout.vartype = 'FLOAT'
            varslist += " " + layout.vartype
            #if character do nvarchar for only its length, otherwise make it a double
            if layout.vartype.upper() in charvartypes and layout.vartype.upper() != "DATETIME":
                varslist += "(" + str(int(layout.length)) + "),"
            else:
                varslist += ","
        #get rid of trailing comma and add ending parenthesis
        varslist = varslist[:-1]        
        create_table_query = Template("""
        CREATE TABLE $Outdata 
        (
        $varslist
        )
        """).substitute(locals())
        
        #now execute the command
        try:
            if Overwrite.upper() in yes_list: #if set not to overwrite it will throw an exception if the table already exists
                drop_table_if_exists(Outdata,db_context)
            db_context.executeNoResults(create_table_query)     
        except pyodbc.ProgrammingError as prgErr:
            raise Exception(prgErr) #("Data Table already exists named: " + Outdata)
        #build error table query
        create_error_table_query = """
        CREATE TABLE %s 
        (
           LineNumber Integer,
           VarName nvarchar(512),
           Value nvarchar(512),
           PointOfFailure nvarchar(512)
        )
        """ % Error_Ds
        #now execute the command
        try:
            if Overwrite.upper() in yes_list: #if set not to overwrite it will throw an exception if the table already exists
                drop_table_if_exists(Error_Ds,db_context)
            db_context.executeNoResults(create_error_table_query)
        except pyodbc.ProgrammingError as prgErr:
            raise Exception("Errors table already exists named: " + Error_Ds)
        insert_to_errords = "INSERT INTO " + Error_Ds + " VALUES ("
        
        #now read the file and insert each line into our output table
        if Type_of_Datafile.upper() == 'DELIMITED':
            #f = open(Infile,mode='r')
            f = codecs.open(Infile,'r',encoding)
            lineNum = 1
            #initialize colmapping default values (current order for layoutlist is what we go by)
            colmapping = []
            for i in range(len(layoutlist)):
                colmapping.append(i)                      
            #go through line by line
            for line in f:
                cols = []
                newline = line.strip('\r\n')
                cols = [x[0] + x[1] for x in re.findall('[' + Delimiter + '](?: *"(.*?)" *|(.*?))(?=[' + Delimiter + '])',Delimiter + newline + Delimiter)]                
                #Removing BOM from unicode files - this is useless and not part of the content. 
                # It should not be there so we ignore it
                BOM = u'\ufeff'
                if encoding.upper() == 'UTF-8' and BOM in cols[0]:
                    cols[0] = cols[0].replace(BOM,'')
                #get rid of leading and trailing \'s, and protect against 
                # SQL Injection by replacing all ' with ''
                for i in xrange(len(cols)):
                    cols[i] = cols[i].strip().strip("'\"").strip().replace("'","''")
                if lineNum < firstobs and firstobs == 2 and Getnames.upper() in yes_list:
                    #if file contains column names we setup column mapping, if not we assume they are in correct order
                    for elem in cols:
                        found = False
                        for layout in layoutlist:
                            if layout.varname.upper() == str(elem).upper():
                                colmapping[cols.index(elem)] = layoutlist.index(layout)
                                found = True
                                break
                        #if a variable in the col names is not found in layout add error to table
                        if not found:
                            db_context.executeNoResults(insert_to_errords + str(lineNum)+",'"+str(elem)+"','"+str(elem)+"','Variable Not Found In Layout')")                
                    lineNum += 1
                    continue
                #if some columns were not found then raise an exception
                if len(set(colmapping)) < len(layoutlist) or len(cols) < len(layoutlist):
                    for i in xrange(len(layoutlist)):
                        if not i in colmapping:
                            raise Exception("ERROR: Not all variables in layout found in data file. Variable \"" + str(layoutlist[i].varname) + "\" not found in data file")
                    raise Exception("ERROR: Not all variables in layout found in data file.")
                lineNum += 1                
                query = "INSERT INTO " + Outdata + " VALUES ("                
                #iterate through the values received and inserting them into the insert query
                for elem in cols:
                    idx = cols.index(elem)
                    if idx > len(colmapping)-1: # double check to make sure no errors if more columns in data file than variables in layout
                        db_context.executeNoResults(insert_to_errords + str(lineNum)+",'"+str(elem)+"','"+str(elem)+"','More columns than specified in layout')")
                        continue               
                    item = layoutlist[colmapping[idx]]
                    if elem == "" or elem == ".": #an empty string represents a missing value, so set to SQL NULL
                        val = 'NULL' #set value to SQL NULL for missing value
                    else: val = elem
                    #add value to query
                    if item.vartype.upper() in charvartypes and val != 'NULL': #must add quotes if a character variable
                        if len(val) > item.length:
                            val = val[:item.length]      
                        if item.vartype.upper() == 'DATETIME': #cast to datetime if column is datetime column
                            val = item.convert_informat(val)                           
                            query += "convert(datetime,'" + str(val) + "'," + str(item.informat) + "),"
                        else: query += "\'" + val + "\',"
                    else: query += val + ","
                    #check values against layout spec
                    if val != "NULL":
                        check_value_against_layout(elem,lineNum,item,db_context,Error_Ds)
                #adding null for values of variables not mentioned in data
                for i in range(len(layoutlist) - len(cols)):
                    if i == len(layoutlist) - len(cols):
                        break
                    query += "NULL,"
                query = query[:-1] + ")"
                try:
                    db_context.executeNoResults(query)
                except pyodbc.Error as er:
                    #query failed so add to error table. Varname = query run, and the message is the error from SQL
                    db_context.executeNoResults(insert_to_errords + str(lineNum)+",'"+query.replace("'","''")+"',NULL,'Error inserting data: " + er.args[0].replace("'","''") + "')")
        elif Type_of_Datafile.upper() == 'FIXEDWIDTH':
            #This kind of file can throw errors and break easily if the user is not careful. If there
            #are less variables in the data file than specified in the layout it will break with no checks
            f = open(Infile,mode='r')
            lineNum = 1
            #go through line by line
            for line in f:
                if lineNum < firstobs:
                    lineNum += 1
                    continue
                lineNum += 1      
                query = "INSERT INTO " + Outdata + " VALUES ("
                #assumes layout lists variables in order. Should start at 0 and go to end of line.
                for layout in layoutlist: #going through variables in layout and adding them to query in order from layout
                    if layout.start < 0 or layout.end > len(line): #error if start < 0 or end is beyond the width of the file
                        errorquery = insert_to_errords + str(lineNum)+",'"+layout.varname+"',NULL,'Start < 0 or end > line width')"
                        db_context.executeNoResults(errorquery)
                    else:               
                        elem = str(line[layout.start-1:layout.end]).strip("'\"").strip().replace("'","''")
                        if elem == "" or elem == '.': #if missing value set to SQL NULL
                            val = 'NULL'
                        else: val = elem
                        #add value to query
                        if layout.vartype.upper() in charvartypes and val != 'NULL': #must add quotes if a character variable
                            if len(val) > layout.length:
                                val = val[:layout.length]                            
                            if layout.vartype.upper() == 'DATETIME': #cast to datetime if column is datetime column
                                val = layout.convert_informat(val)                                 
                                query += "convert(datetime,'" + str(val) + "'," + str(layout.informat) + "),"
                            else: query += "\'" + val + "\',"
                        else: query += val + ","
                        #check values against layout spec
                        if val != "NULL":
                            check_value_against_layout(val,lineNum,layout,db_context,Error_Ds)
                query = query[:-1] + ")"               
                try:
                    db_context.executeNoResults(query)
                except pyodbc.Error as er:
                    #query failed so add to error table. Varname = query run, and the message is the error from SQL
                    db_context.executeNoResults(insert_to_errords + str(lineNum)+",'"+query.replace("'","''")+"',NULL,'Error inserting data: " + er.args[0].replace("'","''") + "')")
        elif Type_of_Datafile.upper() == 'EXCEL':
            wb = xlrd.open_workbook(Infile)
            sh = wb.sheet_by_name(Infile_Excel_Sheetname)
            #initialize colmapping default values (current order for layoutlist is what we go by)
            colmapping = []
            for i in xrange(len(layoutlist)):
                colmapping.append(i)
            #go through rows of sheet starting at 0
            for rownum in xrange(sh.nrows):
                cols = sh.row_values(rownum)
                for i in range(len(cols)):
                    cols[i] = str(cols[i]).strip("'\"").strip().replace("'","''")
                if rownum == 0:
                    #if excel file contains column names we setup column mapping, if not we assume they are in correct order
                    if Getnames.upper() in yes_list:
                        for elem in cols:
                            found = False
                            for layout in layoutlist:
                                if layout.varname.upper() == str(elem).upper():
                                    colmapping[cols.index(elem)] = layoutlist.index(layout)
                                    found = True
                                    break
                            #if a variable in the col names is not found in layout add error to table
                            if not found:
                                db_context.executeNoResults(insert_to_errords + str(lineNum)+",'"+str(elem)+"','"+str(elem)+"','Variable Not Found In Layout')")
                        continue
                if len(set(colmapping)) < len(layoutlist) or len(cols) < len(layoutlist):
                    for i in xrange(len(layoutlist)):
                        if not i in colmapping:
                            raise Exception("ERROR: Not all variables in layout found in data file. Variable \"" + str(layoutlist[i].varname) + "\" not found in data file")                    
                    raise Exception("ERROR: Not all variables in layout found in data file.")               
                #begin loop for getting data
                query = "INSERT INTO " + Outdata + " VALUES ("                
                #iterate through the values received and inserting them into the insert query
                for elem in cols:
                    idx = cols.index(elem)
                    if idx > len(colmapping)-1: # double check to make sure no errors if more columns in data file than variables in layout
                        db_context.executeNoResults(insert_to_errords + str(lineNum)+",'"+str(elem)+"','"+str(elem)+"','More columns than specified in layout')")
                        continue
                    item = layoutlist[colmapping[idx]]
                    if str(elem).strip() == "" or str(elem).strip() == ".": #an empty string or period represents a missing value, so set to SQL NULL
                        val = 'NULL' #set value to SQL NULL for missing value?
                    else: val = str(elem).strip()
                    #add value to query
                    if item.vartype.upper() in charvartypes and val != 'NULL': #must add quotes if a character variable
                        if len(val) > item.length:
                            val = val[:item.length]                  
                        if item.vartype.upper() == 'DATETIME': #cast to datetime if column is datetime column
                            val = item.convert_informat(val)                            
                            query += "convert(datetime,'" + str(val) + "'," + str(item.informat) + "),"
                        else: query += "\'" + val + "\',"
                    else: query += val + ","            
                    #check values against layout spec
                    if val != "NULL":
                        check_value_against_layout(val,rownum,item,db_context,Error_Ds)
                #adding null for values of variables not mentioned in data
                for i in range(len(layoutlist) - len(cols)):
                    if i == len(layoutlist) - len(cols):
                        break
                    query += "NULL,"                
                query = query[:-1] + ")"
                try:
                    db_context.executeNoResults(query)
                except pyodbc.Error as er:
                    #query failed so add to error table. Varname = query run, and the message is the error from SQL
                    db_context.executeNoResults(insert_to_errords + str(lineNum)+",'"+query.replace("'","''")+"',NULL,'Error inserting data: " + er.args[0].replace("'","''") + "')")

#if __name__ == '__main__':
    #Intake(Mode='CREATE',Layoutfile=r'C:\Layout.xls',Sheetname='Sheet1') #can only write .xls files, not .xlsx
    #Intake(Mode='RUN',Type_of_Datafile='DELIMITED',Delimiter=',',Layoutfile=r'H:\share\CSSC Folder\Zach\ForPython\intakeLayoutSimple.xlsx',Sheetname='Sheet1',Infile=r'H:\share\CSSC Folder\Zach\ForPython\intakeDataSimple.txt',Outdata='outtab',Error_Ds='errortab',firstobs=1,Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RUN',Type_of_Datafile='DELIMITED',Delimiter=',',Layoutfile=r'C:\Users\ZSchroeder\Desktop\intakeLayoutSimple.xlsx',Sheetname='Sheet1',Infile=r'C:\Users\ZSchroeder\Desktop\simpleData.txt',Outdata='outtab',Error_Ds='errortab',firstobs=1,Overwrite='Yes',encoding='utf-8', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RUN',Type_of_Datafile='FIXEDWIDTH',Layoutfile=r'C:\Users\ZSchroeder\Desktop\intakeLayoutSimple.xlsx',Sheetname='Sheet1',Infile=r'C:\Users\ZSchroeder\Desktop\intakeDataSimple.txt',Outdata='outtab',Error_Ds='errortab',firstobs=1,Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RUN',Type_of_Datafile='EXCEl',Layoutfile=r'C:\Users\ZSchroeder\Desktop\intakeLayoutSimple.xlsx',Sheetname='Sheet1',Infile=r'C:\Users\ZSchroeder\Desktop\intakeDataSimpleExcel.xlsx',Infile_Excel_Sheetname='Sheet1',Outdata='outtab',Error_Ds='errortab',firstobs=1,Getnames='No',Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RUN',Type_of_Datafile='DELIMiteD',Delimiter="|",Layoutfile=r'C:\TLES Value-Added Data Layout Klamath.xlsx',Sheetname='TeacherStudentCourseLinkage',Infile=r'C:\TLES_TEACHER_STU_CRS_LINKS_1011.txt',Outdata='linkageOut',Error_Ds='linkageError',Getnames='Yes',Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RUN',Type_of_Datafile='EXcEl',Layoutfile=r'C:\relevantcourse_layout.xlsx',Sheetname='Sheet1',Infile=r'C:\TLES_RELEVANT_CRS_LIST.xlsx',Infile_Excel_Sheetname='Sheet1',Outdata='relevCourseOut',Error_Ds='relevCourseError',Getnames='Yes',Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RUN',Type_of_Datafile='EXcEl',Layoutfile=r'G:\SAS\relevantcourse_layout.xlsx',Sheetname='Sheet1',Infile=r'G:\SAS\TLES_RELEVANT_CRS_LIST.xlsx',Infile_Excel_Sheetname='Sheet1',Outdata='relevCourseOut',Error_Ds='relevCourseError',Getnames='Yes',Overwrite='Yes', Run_Context=RunContext('unittest'))
    #Intake(Mode='RUN',Type_of_Datafile='EXcEl',Layoutfile=r'C:\relevantcourse_layout_New.xlsx',Sheetname='Sheet1',Infile=r'C:\TLES_RELEVANT_CRS_LIST.xlsx',Infile_Excel_Sheetname='Sheet1',Outdata='outtab',Error_Ds='errortab',Getnames='Yes',Overwrite='Yes',CompatibilityMode='no', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RUN',Type_of_Datafile='delimited',Delimiter=",",Layoutfile=r'C:\relevantcourse_layout.xlsx',Sheetname='Sheet1',Infile=r'C:\TLES_RELEVANT_CRS_LIST.csv',Outdata='outtab',Error_Ds='errortab',Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RuN',Type_of_Datafile='deLimITED',Delimiter="|",Layoutfile=r'C:\TLES Value-Added Data Layout Klamath.xlsx',Sheetname='TeacherStudentCourseLinkage',Infile=r'C:\TLES_TEACHER_STU_CRS_LINKS_0809.txt',Outdata='outtab',Error_Ds='errortab',firstobs=2,Getnames='Yes',Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='RuN',Type_of_Datafile='fixedWIDth',Layoutfile=r'C:\AssessmentIntake_SSW_Staar.xlsx',Sheetname='Assessment',Infile=r'C:\HarlingenCISDStateDataFile.csv',Outdata='outtab',Error_Ds='errortab',firstobs=1,Getnames='No',Overwrite='Yes', Run_Context=RunContext('newtmpdb'))
    #Intake(Mode='Create',Type_of_Datafile='fixedWidTh',Layoutfile=r'C:\createdLayout.xls',Sheetname='Sheet1',CompatibilityMode='Yes')
    
    #ReadLayoutfile(r'C:\relevantcourse_layout.xlsx','Sheet1',False)
