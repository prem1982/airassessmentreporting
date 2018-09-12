'''
Created on Feb 7, 2013

@author: zschroeder
'''

import pyodbc
from string import Template
#from datetime import datetime
import rpy2.robjects as robjects
from airassessmentreporting.airutility import *
R = robjects.r
#for optimization
to_string = str
to_float = float
#########################################################
# Changes to be made:
##    -Add more functions for the different types that
##        can be specified.
#########################################################

__all__ = [ 'Stats', 'PercentSpec', 'Spec', 'isnotmissing', 'isnotmissing_num',
            'create_percentspec_from_cols', 'read_percentspec', 'read_percentspec_fromdb',
            'create_spec_from_cols', 'read_meansspec', 'read_meansspec_fromdb', 'Means' ]

class Stats:
    "This class holds the stats for each byvar value"
    count = 0
    sum = 0
    sum_squared = 0
    numerator_count = 0
    denominator_count = 0
    def __init__(self):
        self.count = 0
        self.sum = 0
        self.sum_squared = 0
        self.numerator_count = 0
        self.denominator_count = 0
    
def calc_n(spec, *args):
    return "[" + spec.varname + "_count]"
def calc_mean(spec, *args):
    return Template("""
    CASE [${var}_count] 
        WHEN 0 THEN NULL
        ELSE ROUND(CAST([${var}_sum] as FLOAT)/CAST([${var}_count] as FLOAT),$rdval) END
    """).substitute(var = spec.varname,rdval = int(to_float(spec.roundvalue)))
def calc_percent(spec, *args):
    return Template("""
    CASE [${var}_count_denom] 
        WHEN 0 THEN NULL
        ELSE ROUND(CAST([${var}_count] as FLOAT)/CAST([${var}_count_denom] as FLOAT) * 100,$rdval) END
    """).substitute(var = spec.varname,rdval = int(to_float(spec.roundvalue)))
def calc_clm(spec, *args):
    pass
def calc_xrange(spec, *args):
    pass
def calc_css(spec, *args):
    pass
def calc_skew(spec, input_ds,inputds_col_info,tablelist,db_context):
    where = ''
    #setup where clause for query based on spec
    if len(spec.wherevarlist) > 0:
        where = " WHERE "
        cnt = 0
        for var in spec.wherevarlist:
            if 'CHAR' in inputds_col_info[var].basic_type: #double check it is lower, and no brackets around var
                where += "[" + var + "] = '" + to_string(spec.wherevaluelist[cnt]) + "' AND "
            else:
                where += "[" + var + '] = ' + to_string(spec.wherevaluelist[cnt]) + " AND "
            cnt += 1
        where = where[:-5] # remove trailing and
    drop_table_if_exists("tmp_"+spec.varname+"_skew",db_context)
    R(Template("""
    library(e1071)
    data <- sqlQuery(conn,"Select $var as ${outvar}_skew,$lvlvar from $input_ds $where")
    aggdata <- aggregate(data,by=list(data[["$lvlvar"]]),FUN=skewness,na.rm=TRUE)
    sqlSave(conn,aggdata,tablename="tmp_${outvar}_skew")
    """).substitute(var=spec.inputvar,input_ds=input_ds,where=where,lvlvar=spec.levelvar,outvar=spec.varname))
    tablelist.append(["[tmp_"+spec.varname+"_skew]",spec])
    return "[" + spec.varname + "_skew]"
def calc_cv(spec, *args):
    pass
def calc_stddev(spec, *args): 
    #should it be sample or population stddev?
    return Template("""
    CASE [${var}_count] 
    WHEN 0 THEN NULL
    WHEN 1 THEN NULL
    ELSE ROUND(SQRT(ABS(CAST([${var}_sumsq] as float) - (SQUARE([${var}_sum])/[${var}_count]))/([${var}_count]-1)),$rdval) END
    """).substitute(var = spec.varname,rdval = int(to_float(spec.roundvalue)))
def calc_kurtosis(spec, input_ds,inputds_col_info,tablelist,db_context):
    where = ''
    #setup where clause for query based on spec
    if len(spec.wherevarlist) > 0:
        where = " WHERE "
        cnt = 0
        for var in spec.wherevarlist:
            if 'CHAR' in inputds_col_info[var].basic_type: #double check it is lower, and no brackets around var
                where += "[" + var + "] = '" + to_string(spec.wherevaluelist[cnt]) + "' AND "
            else:
                where += "[" + var + '] = ' + to_string(spec.wherevaluelist[cnt]) + " AND "
            cnt += 1
        where = where[:-5] # remove trailing and
    drop_table_if_exists("tmp_"+spec.varname+"_kurt",db_context)
    R(Template("""
    library(e1071)
    data <- sqlQuery(conn,"Select $var as ${outvar}_kurt,$lvlvar from $input_ds $where")
    aggdata <- aggregate(data,by=list(data[["$lvlvar"]]),FUN=kurtosis,na.rm=TRUE)
    sqlSave(conn,aggdata,tablename="tmp_${outvar}_kurt")
    """).substitute(var=spec.inputvar,input_ds=input_ds,where=where,lvlvar=spec.levelvar,outvar=spec.varname))
    tablelist.append(["[tmp_"+spec.varname+"_kurt]",spec])
    return "[" + spec.varname + "_kurt]"
def calc_stderr(spec, input_ds,inputds_col_info,tablelist,db_context):
    where = ''
    #setup where clause for query based on spec
    if len(spec.wherevarlist) > 0:
        where = " WHERE "
        cnt = 0
        for var in spec.wherevarlist:
            if 'CHAR' in inputds_col_info[var].basic_type: #double check it is lower, and no brackets around var
                where += "[" + var + "] = '" + to_string(spec.wherevaluelist[cnt]) + "' AND "
            else:
                where += "[" + var + '] = ' + to_string(spec.wherevaluelist[cnt]) + " AND "
            cnt += 1
        where = where[:-5] # remove trailing and
    drop_table_if_exists("tmp_"+spec.varname+"_stderr",db_context)
    R(Template("""
    data <- sqlQuery(conn,"Select $var as ${outvar}_stderr,$lvlvar from $input_ds $where")
    myStdErrFunc <- function(x){
        x <- na.omit(x)
        stdError <- sd(x)/sqrt(length(x))
        return(stdError)
    }
    aggdata <- aggregate(data,by=list(data[["$lvlvar"]]),FUN=myStdErrFunc)
    sqlSave(conn,aggdata,tablename="tmp_${outvar}_stderr")
    """).substitute(var=spec.inputvar,input_ds=input_ds,where=where,lvlvar=spec.levelvar,outvar=spec.varname))
    tablelist.append(["[tmp_"+spec.varname+"_stderr]",spec])
    return "[" + spec.varname + "_stderr]"
def calc_lclm(spec, *args):
    pass
def calc_sum(spec, *args):
    return "[" + spec.varname + "_sum]"
def calc_max(spec, *args):
    return "[" + spec.varname + "_max]"
def calc_sumwgt(spec, *args):
    pass
def calc_uclm(spec, *args):
    pass
def calc_min(spec, *args):
    return "[" + spec.varname + "_min]"
def calc_uss(spec, *args):
    pass
def calc_var(spec, *args):
    return Template("""
    CASE [${var}_count] 
    WHEN 0 THEN NULL
    WHEN 1 THEN NULL
    ELSE ROUND((CAST([${var}_sumsq] as float) - (SQUARE([${var}_sum])/[${var}_count]))/([${var}_count]-1),$rdval) END
    """).substitute(var = spec.varname, rdval=int(to_float(spec.roundvalue)))
def calc_nmiss(spec, *args):
    return "[" + spec.varname + "_nmiss]"
def calc_median(spec, input_ds,inputds_col_info,tablelist,db_context):
    where = ''
    #setup where clause for query based on spec
    if len(spec.wherevarlist) > 0:
        where = " WHERE "
        cnt = 0
        for var in spec.wherevarlist:
            if 'CHAR' in inputds_col_info[var].basic_type: #double check it is lower, and no brackets around var
                where += "[" + var + "] = '" + to_string(spec.wherevaluelist[cnt]) + "' AND "
            else:
                where += "[" + var + '] = ' + to_string(spec.wherevaluelist[cnt]) + " AND "
            cnt += 1
        where = where[:-5] # remove trailing and
    #add column to tmp table that holds values
#    cursor.execute(Template("""
#    ALTER TABLE [tmp_${lvlvar}]
#    ADD [${outvar}_median] float
#    """).substitute(lvlvar=spec.levelvar,outvar=spec.varname))
#    cursor.commit()
    #now do calculations in R
#this gives error with no explanation....
#    R(Template("""
#    data <- sqlQuery(conn,"Select Grade_preid as dprxmed_median,DCRXID from Studentg3_n where uprxlep=1 and inclusionflagr=1")
#    aggdata <- aggregate(data,by=list(data[["DCRXID"]]),FUN=median,na.rm=TRUE)
#    sqlUpdate(conn,aggdata[c("dprxmed_median","DCRXID")],tablename="tmp_dcrxid",index="DCRXID")
#    """).substitute(var=spec.inputvar,input_ds=input_ds,where=where,lvlvar=spec.levelvar))
    #begin new implementation where it creates its own table
    drop_table_if_exists("tmp_"+spec.varname+"_median", db_context)
    R(Template("""
    data <- sqlQuery(conn,"Select $var as ${outvar}_median,$lvlvar from $input_ds $where")
    aggdata <- aggregate(data,by=list(data[["$lvlvar"]]),FUN=median,na.rm=TRUE)
    sqlSave(conn,aggdata,tablename="tmp_${outvar}_median")
    """).substitute(var=spec.inputvar,input_ds=input_ds,where=where,lvlvar=spec.levelvar,outvar=spec.varname))
    tablelist.append(["[tmp_"+spec.varname+"_median]",spec])
    return "[" + spec.varname + "_median]"
def calc_q3(spec, *args):
    pass
def calc_p1(spec, *args):
    pass
def calc_p90(spec, *args):
    pass
def calc_p5(spec, *args):
    pass
def calc_p95(spec, *args):
    pass
def calc_p10(spec, *args):
    pass
def calc_p99(spec, *args):
    pass
def calc_q1(spec, input_ds,inputds_col_info,tablelist,db_context):
    where = ''
    #setup where clause for query based on spec
    if len(spec.wherevarlist) > 0:
        where = " WHERE "
        cnt = 0
        for var in spec.wherevarlist:
            if 'CHAR' in inputds_col_info[var].basic_type: #double check it is lower, and no brackets around var
                where += "[" + var + "] = '" + to_string(spec.wherevaluelist[cnt]) + "' AND "
            else:
                where += "[" + var + '] = ' + to_string(spec.wherevaluelist[cnt]) + " AND "
            cnt += 1
        where = where[:-5] # remove trailing and
    drop_table_if_exists("tmp_"+spec.varname+"_q1", db_context)
    R(Template("""
    data <- sqlQuery(conn,"Select $var as ${outvar}_q1,$lvlvar from $input_ds $where")
    aggdata <- aggregate(data,by=list(data[["$lvlvar"]]),FUN=quantile,probs=0.25,na.rm=TRUE)
    sqlSave(conn,aggdata,tablename="tmp_${outvar}_q1")
    """).substitute(var=spec.inputvar,input_ds=input_ds,where=where,lvlvar=spec.levelvar,outvar=spec.varname))
    tablelist.append(["[tmp_"+spec.varname+"_q1]",spec])
    return "[" + spec.varname + "_q1]"
def calc_qxrange(spec, *args):
    pass
def calc_probt(spec, *args):
    pass
def calc_t(spec, *args):
    pass
    
class PercentSpec:
    "This class represents a row in the percent specification"
    varname=''
    levelvar=''
    numeratorvar=''
    numeratorvalue=''
    denominatorvar=''
    denominatorvalue=''
    rdvalue=0.0
    roundvalue = 0
    def __init__(self):
        self.varname=''
        self.levelvar=''
        self.numeratorvar=''
        self.numeratorvalue=''
        self.denominatorvar=''
        self.denominatorvalue=''
        self.rdvalue=0.0
        self.roundvalue = 0
        
class Spec:
    "This class represents a row in the specification (for use with data)"
    varname = ''
    levelvar = ''
    inputvar = ''
    wherevar = ''
    type = '' 
    rdvalue = 0.0
    roundvalue = 0
    wherevalue = ''
    typefctn = calc_n
    is_percent = False
    wherevarlist = []
    wherevaluelist = []
    numerator_var_list = []
    denominator_var_list = []
    numerator_val_list = []
    denominator_val_list = []
    def __init__(self):
        self.varname = ''
        self.levelvar = ''
        self.inputvar = ''
        self.wherevar = ''
        self.type = '' 
        self.rdvalue = 0.0
        self.roundvalue = 0
        self.wherevalue = ''
        self.typefctn = calc_n
        self.is_percent = False
        self.wherevarlist = []
        self.wherevaluelist = []
        self.numerator_var_list = []
        self.denominator_var_list = []
        self.numerator_val_list = []
        self.denominator_val_list = []
    def is_null(self):
        return (self.varname == '' and self.levelvar == '' and self.inputvar == '' and self.wherevar == '' 
                and self.type == '' and self.rdvalue == 0 and self.roundvalue == 0 and self.wherevalue == '')

def isnotmissing(strng):
    "this checks if a value is not missing (empty string or NULL)"
    if strng is None:
        return False
    newstr = to_string(strng).strip()
    return newstr != ''# and newstr != '.' and newstr != 'NONE' and newstr != 'NULL'

def isnotmissing_num(val):
    "This function checks if a numeric value is not missing"
    if val is None:
        return False
    if isinstance(val, str):
        newstr = val.strip()
        return newstr != '' and newstr != '.'
    return True

def create_percentspec_from_cols(cols,rownum):
    """"This function creates a Spec object from the dictionary of {columnname:value} input. The rownumber is for debugging/error throwing purposes." 
    
        Notes
        --------
            This function will denote these as percent Specs. To do this it sets type='Percent',
            and populates the (numerator/denominator)_(val/var)_lists variables.
    """
    symbols=['!','%','^','&','*','(',')','-','+','=','`','~',
             '[',']','{','}','\\','|',';',':','"','\'','<',',','>','.','/',
             '?']
    invalidfirstchars=symbols + ['$'] + ['1','2','3','4','5','6','7','8','9','0']
    newspec = PercentSpec()
    #first set values
    newspec.varname = cols["variable_name"].strip()
    newspec.numeratorvar = cols["numeratorvariable"].strip().upper()
    newspec.numeratorvalue = to_string(cols["numeratorvalue"]).strip().upper()
    newspec.levelvar = cols["levelvar"].strip().upper()
    newspec.rdvalue = cols["rdvalue"]
    newspec.denominatorvar = cols["denominatorvariable"].strip().upper()
    newspec.denominatorvalue = to_string(cols["denominatorvalue"]).strip().upper()
    #then error check
    if isinstance(newspec.numeratorvalue, (float,int)): # since it is stored as a string this is useless
        raise Exception("Error: The numerator value should be a character variable")
    if isinstance(newspec.denominatorvalue, (float,int)):# since it is stored as a string this is useless
        raise Exception("Error: The denominator value should be a character variable")
    if len(newspec.varname) > 128 or (newspec.varname[0] in invalidfirstchars) or any(sym in newspec.varname for sym in symbols):
            raise Exception("Error: Invalid SQL variable name in Percents file: " + newspec.varname + " you cannot include symbols and the name must be < 128 characters long.")
    if not isnotmissing_num(newspec.rdvalue):
        newspec.roundvalue = 12
    else: 
        try:
            if newspec.rdvalue < 0:
                raise Exception("Error: rdvalue must be missing or a numeric value and greater than or equal to zero.")
            else: newspec.roundvalue = newspec.rdvalue
        except:
            raise Exception("Error: rdvalue must be missing or a numeric value and greater than or equal to zero.")
    #this does the Percent_Modified datastep in the SAS macro
    #this converts the percent spec to the same type as the means spec
    ##The above step of reading each line into a PercentSpec and then converting
    ## here is not necessary (it could be converted right when reading) but this
    ## seems more readable and easier to understand
    retspec = Spec()
    retspec.inputvar = ''
    retspec.levelvar = newspec.levelvar
    retspec.varname = newspec.varname
    retspec.rdvalue = newspec.rdvalue
    retspec.roundvalue = newspec.roundvalue
    retspec.type = 'Percent'
    retspec.typefctn = calc_percent
    retspec.is_percent = True
    retspec.numerator_var_list = map(lambda x:x.strip().upper(),newspec.numeratorvar.split('*'))
    retspec.numerator_val_list = map(lambda x:x.strip().upper(),newspec.numeratorvalue.split())
    retspec.denominator_var_list = map(lambda x:x.strip().upper(),newspec.denominatorvar.split('*'))
    retspec.denominator_val_list = map(lambda x:x.strip().upper(),newspec.denominatorvalue.split())
    while '' in retspec.numerator_var_list:
        del retspec.numerator_var_list[retspec.numerator_var_list.index('')]
    while '' in retspec.numerator_val_list:
        del retspec.numerator_val_list[retspec.numerator_val_list.index('')]
    while '' in retspec.denominator_var_list:
        del retspec.denominator_var_list[retspec.denominator_var_list.index('')]
    while '' in retspec.denominator_val_list:
        del retspec.denominator_val_list[retspec.denominator_val_list.index('')]  
    #if number of wherevars does not equal the number of wherevalues throw an exception
    if len(retspec.numerator_var_list) != len(retspec.numerator_val_list):
        raise Exception("Error: line: " + to_string((rownum+1)) + " - The Number of NumeratorVars is not equal to the number of Numeratorvalues")
    if len(retspec.denominator_var_list) != len(retspec.denominator_val_list):
        raise Exception("Error: line: " + to_string((rownum+1)) + " - The Number of DenominatorVars is not equal to the number of Denominatorvalues")
    return retspec

def read_percentspec(reader):
    """This function reads the percent spec from specified excel file and sheet, and returns a dictionary of Specs.
    
        Parameters
        -------------------------
        reader : SafeExcelReader object
            This will be used to read the excel percents file. We will call getRows() on it.
            
        Returns
        ------------
            This function returns a dictionary of {spec.varname : spec} objects. The spec object is 
            an instance of the Spec class defined above. 
            
        Notes
        ---------
            This function calls create_percentspec_from_cols(), which does all the work of putting the data into a Spec object.
    """
    colmapping = {"variable_name":-1,"levelvar":-1,"numeratorvariable":-1,"numeratorvalue":-1,
                  "denominatorvariable":-1,"rdvalue":-1,"denominatorvalue":-1}
    new_specdict = {}
    #go through rows of sheet starting at 0
    rownum = 0
    for row in reader.getRows():
        #check headers only on first row
        if rownum == 0:
            #now set the column number in mapping based on layout column names
            i = 0
            for item in row.keys():
                if item in colmapping:
                    colmapping[item] = i
                i += 1
            #now check if any required columns are missing, and raise an exception if any are
            if -1 in colmapping.values():
                raise Exception("ERROR: The percent_file is missing a required column. The required columns are:"
                                " Variable_Name LevelVar NumeratorVariable NumeratorValue DenominatorVariable "
                                "DenominatorValue rdvalue")
        spec_to_add = create_percentspec_from_cols(row, rownum)
        if spec_to_add.is_null():
            raise Exception("Error: Problem with percents file - row " + str(rownum+1) + ". Sorry for lack of diagnostics.")
        #finally add these 2 Spec's to the return dictionary
        if spec_to_add.varname in new_specdict:
            raise Exception("Error: varname \"" + spec_to_add.varname + "\" duplicate in Percent spec")
        new_specdict.update({spec_to_add.varname : spec_to_add})
        rownum += 1
    #after going through whole file return dictionary of Specs acquired
    return new_specdict
   
def read_percentspec_fromdb(db_context,tablename):
    """This function reads the percent spec from specified DB and table, and returns a dictionary of Specs.
    
        Parameters
        -------------------------
        db_context : DBContext object
            This is the connection to the database we will use for queries.
            
        tablename : String
            This should be the name of the table to get information from in the database db_context connects to.
            
        Returns
        ------------
            This function returns a dictionary of {spec.varname : spec} objects. The spec object is 
            an instance of the Spec class defined above. 
            
        Notes
        ---------
            This function calls create_percentspec_from_cols(), which does all the work of putting the data into a Spec object.
    """
    colmapping = {"variable_name":-1,"levelvar":-1,"numeratorvariable":-1,"numeratorvalue":-1,
                  "denominatorvariable":-1,"rdvalue":-1,"denominatorvalue":-1}
    new_specdict = {}
    tablespec = db_context.getTableSpec(tablename)
    if not table_exists(tablename,db_context):
        raise Exception("Table \"" + tablename + "\" does not exist")
    for column_spec in tablespec:
        colname = db_identifier_unquote(column_spec.field_name)
        if colname in colmapping:
            colmapping[colname] = column_spec.ordinal_position - 1
    if -1 in colmapping.values():
        raise Exception("ERROR: The percent_file is missing a required column. The required columns are:"
                        " Variable_Name LevelVar NumeratorVariable NumeratorValue DenominatorVariable "
                        "DenominatorValue rdvalue")
    
    select_query = 'SELECT ['
    #add all columns we need
    select_query += "],[".join(colmapping.keys())
    select_query += r"] from [" + tablename + "]"
    #reset column mapping because we only selected the columns we need in the order we wanted
    cnt = 0
    for key in colmapping:
        colmapping[key] = cnt
        cnt+=1
    rownum = 0
    for row in db_context.executeBuffered(select_query):
        #create dictionary of {colname : value} to pass
        rowdict = {name:row[colmapping[name]] for name in colmapping}
        spec_to_add = create_percentspec_from_cols(rowdict, rownum)
        if spec_to_add.is_null():
            raise Exception("Error: Problem with percents file - row " + str(rownum+1) + ". Sorry for lack of diagnostics.")
        #finally add these 2 Spec's to the return dictionary
        if spec_to_add.varname in new_specdict:
            raise Exception("Error: varname \"" + spec_to_add.varname + "\" duplicate in Percent spec")
        new_specdict.update({spec_to_add.varname : spec_to_add})
        rownum += 1
    return new_specdict
    
def create_spec_from_cols(cols,rownum):
    "This function creates a spec object from the dictionary of {columnname:value} input. The rownumber is for debugging/error throwing purposes."
    validtypes = {"N":calc_n, "CLM":calc_clm, "RANGE":calc_xrange, "CSS":calc_css, "SKEWNESS":calc_skew, 
                  "SKEW":calc_skew, "CV":calc_cv, "STDDEV":calc_stddev, "STD":calc_stddev, "KURTOSIS":calc_kurtosis, 
                  "KURT":calc_kurtosis, "STDERR":calc_stderr,"LCLM":calc_lclm, "SUM":calc_sum, "MAX":calc_max, 
                  "SUMWGT":calc_sumwgt, "MEAN":calc_mean, "UCLM":calc_uclm, "MIN":calc_min, "USS":calc_uss, 
                  "VAR":calc_var, "NMISS":calc_nmiss, "MEDIAN":calc_median, "P50":calc_median, "Q3":calc_q3, 
                  "P75":calc_q3, "P1":calc_p1, "P90":calc_p90, "P5":calc_p5, "P95":calc_p95, "P10":calc_p10, 
                  "P99":calc_p99, "Q1":calc_q1, "P25":calc_q1, "QRANGE":calc_qxrange, "PROBT":calc_probt, 
                  "T":calc_t}
    symbols=['!','%','^','&','*','(',')','-','+','=','`','~','[',']','{',
             '}','\\','|',';',':','"','\'','<',',','>','.','/','?']
    invalidfirstchars=symbols + ['$'] + ['1','2','3','4','5','6','7','8','9','0']
    newspec = Spec()
    #first get the values
    newspec.varname = cols["variable_name"].strip()
    if cols["inputvar"] is None:
        newspec.inputvar = ''
    else: newspec.inputvar = cols["inputvar"].strip()
    newspec.levelvar = cols["levelvar"].strip().upper()
    newspec.wherevar = cols["wherevar"].strip().upper()
    newspec.type = cols["type"].strip().upper()
    newspec.rdvalue = cols["rdvalue"]
    newspec.wherevalue = to_string(cols["wherevalue"]).strip().upper()
    newspec.wherevarlist = map(lambda x:x.strip().upper(),newspec.wherevar.split('*'))
    newspec.wherevaluelist = map(lambda x:x.strip().upper(),newspec.wherevalue.split())
    while '' in newspec.wherevarlist:
        del newspec.wherevarlist[newspec.wherevarlist.index('')]
    while '' in newspec.wherevaluelist:
        del newspec.wherevaluelist[newspec.wherevaluelist.index('')]  
    #Then error check
        #checking if varname is a valid SQL column name
    if len(newspec.varname) > 128 or (newspec.varname[0] in invalidfirstchars) or any(sym in newspec.varname for sym in symbols):
            raise Exception("Error: Invalid SQL variable name in Percents file: " + newspec.varname + " you cannot include symbols and the name must be < 128 characters long.")
    if not isnotmissing_num(newspec.rdvalue):
        newspec.roundvalue = 12
    else: 
        try:
            if newspec.rdvalue < 0:
                raise Exception("Error: rdvalue must be missing or a numeric value and greater than or equal to zero.")
            else: newspec.roundvalue = newspec.rdvalue
        except:
            raise Exception("Error: line: " + to_string((rownum+1)) + " - rdvalue must be a missing or numeric value and greater than or equal to zero.")
        #if wherevalue is numeric then throw an error
    if isinstance(newspec.wherevalue,(float,int)):
        raise Exception("Error: line: "+ to_string((rownum+1)) +" - The variable WhereValue should be a Character variable")
        #skip row if no type specified
    if newspec.type == '':
        return Spec()
        #throw error if not an allowed type
    if not newspec.type.upper() in validtypes:
        raise Exception("Error: line: " + to_string((rownum+1)) +" - " + to_string(newspec.type) + " is not a correct type")
    newspec.typefctn = validtypes[newspec.type.upper()]
        #if number of wherevars does not equal the number of wherevalues throw an exception
    if len(newspec.wherevarlist) != len(newspec.wherevaluelist):
        raise Exception("Error: line: " + to_string((rownum+1)) + " - The Number of WhereVars is not equal to the number of Wherevalues")
    #passed validation so return the new spec
    return newspec         

def read_meansspec(reader):
    """This function reads the means spec from specified excel file and sheet, and returns a dictionary of Specs.
    
        Parameters
        -------------------------
        reader : SafeExcelReader object
            This will be used to read the excel aggregate file. We will call getRows() on it.
            
        Returns
        ------------
            This function returns a dictionary of {spec.varname : spec} objects. The spec object is 
            an instance of the Spec class defined above. 
            
        Notes
        ---------
            This function calls create_spec_from_cols(), which does all the work of putting the data into a Spec object.
    """
    new_specdict = {}
    colmapping = {"variable_name":-1,"levelvar":-1,"inputvar":-1,"wherevar":-1,
                      "type":-1,"rdvalue":-1,"wherevalue":-1}
    rownum = 0
    for row in reader.getRows():
        #check the headers only on the first row
        if rownum == 0:
            #get column names as lowercase list
            columns = row.keys()
            #now set the column number in mapping based on layout column names
            for item in columns:
                if item in colmapping.keys():
                    colmapping[item] = columns.index(item)
            #now check if any required columns are missing, and raise an exception if any are
            if -1 in colmapping.values():
                raise Exception("ERROR: The agg_file is missing a required column. The required columns are:"
                                    " Variable_Name LevelVar InputVar WhereVar Type Rdvalue and WhereValue")
        newspec = create_spec_from_cols(row, rownum)
        if newspec.is_null():
            raise Exception("Error: Problem with aggregate file - row " + str(rownum+1) + ". Sorry for lack of diagnostics.")
        #if its already in dictionary there is a problem
        if newspec.varname in new_specdict:
            raise Exception("Error: varname \"" + newspec.varname + "\" duplicate in Means spec")
        new_specdict.update({newspec.varname : newspec})
        rownum += 1
    return new_specdict
    
def read_meansspec_fromdb(db_context,tablename):
    """This function reads the means spec from specified DB and table, and returns a dictionary of Specs.
    
        Parameters
        -------------------------
        db_context : DBContext object
            This is the connection to the database we will use for queries.
            
        tablename : String
            This should be the name of the table to get information from in the database db_context connects to.
            
        Returns
        ------------
            This function returns a dictionary of {spec.varname : spec} objects. The spec object is 
            an instance of the Spec class defined above. 
            
        Notes
        ---------
            This function calls create_spec_from_cols(), which does all the work of putting the data into a Spec object.
    """
    new_specdict = {}
    colmapping = {"variable_name":-1,"levelvar":-1,"inputvar":-1,"wherevar":-1,
                      "type":-1,"rdvalue":-1,"wherevalue":-1}
    tablespec = db_context.getTableSpec(tablename)
    if len(tablespec) == 0:
        raise Exception("Table \"" + tablename + "\" does not exist")
    for column_spec in tablespec:
        colname = db_identifier_unquote(column_spec.field_name)
        if colname in colmapping:
            colmapping[colname] = column_spec.ordinal_position - 1
    if -1 in colmapping.values():
        raise Exception("ERROR: The agg_file is missing a required column. The required columns are:"
                        " Variable_Name LevelVar InputVar WhereVar Type Rdvalue and WhereValue")
    rownum = 0
    select_query = 'SELECT ['
    #add all columns we need
    select_query += "],[".join(colmapping.keys())
    select_query += r"] from [" + tablename + "]"
    #reset column mapping because we only selected the columns we need in the order we wanted
    cnt = 0
    for key in colmapping:
        colmapping[key] = cnt
        cnt+=1
    for row in db_context.executeBuffered(select_query):
        #create dictionary of {colname : value} to pass
        rowdict = {name:row[colmapping[name]] for name in colmapping}
        newspec = create_spec_from_cols(rowdict, rownum)
        if newspec.is_null():
            rownum += 1
            continue
        #if its already in dictionary there is a problem
        if newspec.varname in new_specdict:
            raise Exception("Error: varname \"" + newspec.varname + "\" duplicate in Means spec")
        new_specdict.update({newspec.varname : newspec})
        rownum += 1
    return new_specdict

def get_unique_var_mapping(masterspecdict):
    """ returns a list of unique vars to be selected from input ds (checks inputvars, levelvars, and wherevars)
        
        Notes
        ---------
        This function is not used.
    """
    retlist = set()
    for spec in masterspecdict.values():
        if isnotmissing(spec.levelvar):
            retlist.add(spec.levelvar)
        if isnotmissing(spec.inputvar):
            retlist.add(spec.inputvar)
        for var in spec.wherevarlist:
            if isnotmissing(var):
                retlist.add(var)
        for var in spec.numerator_var_list:
            if isnotmissing(var):
                retlist.add(var)
        for var in spec.denominator_var_list:
            if isnotmissing(var):
                retlist.add(var)
    return retlist

class Means(object):
    def __init__(self,
                 excel='Y',       # Y if the aggregation data is given as excel input or N if it is a SQL table.
                 agg_ds='',       # Name of the Aggregation SQL datatable (Given only when excel parameter has a value of "N")
                 agg_file='',     # Aggregation excel workbook
                 agg_sheet='',    # Sheet containing the means input and output variables
                 db_context=None,# A DBContext within which all processing will be done.
                 inputds='',      # name of the input SQL datatable
                 percent='N',     # N if there is no percentage variable to be calculated
                 percent_file='', # Location of the percentage variables specification sheet
                 percent_sheet='',# SheetName of the Percentage variables specification
                 percent_ds='',   # Name of the Percent SQL datatable (Given only when excel parameter has a value of "N")
                 overwrite='Y',   # must be 'Y' or 'N' and specifies if the output datatables should be overwritten or not
                 odbcconn=''      #if using statistics that require R, this is the ODBC connection you must setup to your database for R
                 ):
        self.excel = excel
        self.agg_ds = agg_ds
        self.agg_file = agg_file
        self.agg_sheet = agg_sheet
        self.db_context = db_context
        self.inputds = inputds
        self.percent = percent
        self.percent_file = percent_file
        self.percent_sheet = percent_sheet
        self.percent_ds = percent_ds
        self.overwrite = overwrite
        self.odbcconn = odbcconn
        self.created_tables_list = [] # this will hold the names of the tables the execute method creates
        self.levelvardict = {}
        
    def execute(self):
        """ Efficient computation of means, standard deviations, mins, count, maxes and percentages for score 
            reporting data sets.
        
            Extended summary
            -------------------------
            This function does the calculations specified in the means and percentages layout file.
            
            Attributes
            ---------------
            created_tables_list : list of Strings
                This list will hold the names of the tables that are created when execute() is run.
            
            Parameters
            --------------     
            excel : String ('Y' or 'N')
                This must be either 'Y' or 'N' and indicates whether the bookmaps are in an excel file or a
                SQL table. If excel='N' we assume it is a SQL table.
                 
            agg_ds : String
                Name of the Aggregation SQL datatable (Given only when excel parameter has a value of 'N').
                
            agg_file : String
                Name of the Aggregation excel file (and path) (Given only when excel parameter has a value of 'Y').
            
            agg_sheet : String
                Sheet name inside of Aggregation excel file (Given only when excel parameter has a value of 'Y').
                
            db_context : DBContext object
                This is the DBContext within which all processing will be done. This specifies the DB connection.
                
            inputds : String
                This should be the name of the SQL table that contains the Aggregation data. This is only used when excel='N'.
                
            percent : String
                This should be 'Y' or 'N'. 'Y' says there is a percents file, and 'N' says there is not.
                
            percent_file : String
                Name of the Percent excel file (and path) (Given only when percent parameter has a value of 'Y').
                
            percent_sheet : String
                Sheet name inside of Percent excel file (Given only when percent parameter has a value of 'Y').
                
            percent_ds : String
                This should be the name of the SQL table that contains the Percent data. This is only used when excel='N'.
                
            overwrite : String ('Y' or 'N')
                This should be either 'Y' to overwrite the output table, or 'N' to throw an error if the table already
                exists.
                
            odbcconn : String
                If using statistics that require R, this is the ODBC connection you must setup to your 
                database for R. This can be done in Control Panel -> Administrative Tools -> Data Sources
    
            Returns
            ----------
            Nothing.     
            
            Notes
            --------
            If you are using statistics that require R you must first setup an ODBC connection for your database you specify
            in the parameters. This is required in order for R to work correctly. 
        """
    #    starttime = datetime.now()
        self.created_tables_list = []
        YorN = ['Y','N']
        r_types = ["CLM", "RANGE", "CSS", "SKEWNESS", "SKEW", "CV", "KURTOSIS","KURT", "STDERR","LCLM", 
                      "SUMWGT", "UCLM", "USS","MEDIAN", "P50", "Q3","P75", "P1", "P90", "P5", "P95", "P10", 
                      "P99", "Q1", "P25", "QRANGE", "PROBT","T"]
        if self.excel.upper() not in YorN:
            raise Exception("Error: excel parameter must be 'Y' or 'N'")
        if self.percent.upper() not in YorN:
            raise Exception("Error: percent parameter must be 'Y' or 'N'")
        if self.overwrite.upper() not in YorN:
            raise Exception("Error: overwrite parameter must be 'Y' or 'N'")
        if self.inputds.strip() == '':
            raise Exception("Error: You must specify the inputds parameter")
        if self.db_context is None:
            raise Exception("Error: You must specify a DBContext")
        meansspec_dict = {}
        percentspec_dict = {}
        if self.excel.upper() == 'Y':
            if not os.path.exists(self.agg_file):
                raise Exception("Error: Excel file " + self.agg_file + " not found.")
            if self.percent.upper() == 'Y':
                if not os.path.exists(self.percent_file):
                    raise Exception("Error: Excel file " + self.percent_file + " not found.")
                #open percents file here too - it is also excel
                reader = SafeExcelReader(None,filename=self.percent_file, sheet_name=self.percent_sheet)
                percentspec_dict = read_percentspec(reader)
            reader = SafeExcelReader(None,filename=self.agg_file, sheet_name=self.agg_sheet)
            meansspec_dict = read_meansspec(reader)
        else: #excel is 'N' - checked above. This means both are SQL Tables
            if self.percent.upper() == 'Y':
                percentspec_dict = read_percentspec_fromdb(self.db_context,self.percent_ds)
            meansspec_dict = read_meansspec_fromdb(self.db_context,self.agg_ds)
        #check for duplicate variable names
        for key in percentspec_dict.keys():
            if key in meansspec_dict.keys():
                raise Exception("Error: varname \"" + key + "\" duplicate")
        for key in meansspec_dict.keys():
            if key in percentspec_dict.keys():
                raise Exception("Error: varname \"" + key + "\" duplicate")
        masterspecdict = dict(meansspec_dict.items() + percentspec_dict.items())
        #free up some memory -- double check this doesnt delete from masterspecdict
        del meansspec_dict
        del percentspec_dict
        #check if we will need a connection to R, and if we do make sure odbcconn parameter is not empty
        r_types_list = list([x for x in masterspecdict.values() if x.type in r_types])
        if len(r_types_list) > 0 and self.odbcconn.strip() == '':
            raise Exception("Error: You must specify an ODBC Connection if you want to use any of types: " + ",".join(r_types))
        self.levelvardict = {}
        #1-liner to setup dictionary of {levelvar:{outputvar:spec}} in compiled C (fast)
        self.levelvardict.update({spec.levelvar:{spec2.varname:spec2 for spec2 in masterspecdict.values() if spec2.levelvar == spec.levelvar} for spec in masterspecdict.values()})
        #getting info from table, i.e. type and length of columns
        input_info_dict = self.db_context.getTableSpec(self.inputds)
        #setup and run each select query getting the count, and if necessary the
        #    sum, min, max, and number of missing values all according to the 
        #    spec sheet conditions provided
        for lvlvar in self.levelvardict:
            select_query = "SELECT " + lvlvar #this is the main query all the other queries get added to. This one is run.
            tier1_dict = self.levelvardict[lvlvar]
            for outputvar in tier1_dict:
                cur_spec = tier1_dict[outputvar]
                select_query += ",SUM("
                denom_query = '' # will only not be '' when it is a spec percent
                #if it is a percent spec do numerator and denominator counts
                if cur_spec.is_percent:
                    #if there are where conditions for the numerator then add them to cases
                    if len(cur_spec.numerator_var_list) > 0:
                        select_query += " CASE WHEN " #numerator count gets stored in _count
                        denom_query += ",SUM( CASE WHEN " #denominator counts have _count_denom at the end of the varname
                        cnt = 0
                        for wherevar in cur_spec.numerator_var_list:
                            if 'CHAR' in input_info_dict[wherevar].basic_type:
                                select_query += wherevar + " = '" + to_string(cur_spec.numerator_val_list[cnt]) + "' AND "
                            else:
                                select_query += wherevar + " = " + to_string(cur_spec.numerator_val_list[cnt]) + " AND "
                            cnt += 1
                        select_query = select_query[:-5] + " THEN 1 ELSE 0 END" 
                    else:
                        #otherwise just add 1 for each occurance
                        select_query += "1"
                    #if there are where conditions for the denominator then add them to cases
                    if len(cur_spec.denominator_var_list) > 0:
                        cnt = 0
                        for wherevar in cur_spec.denominator_var_list:
                            if 'CHAR' in input_info_dict[wherevar].basic_type: #double check it is lower, and no brackets around wherevar
                                denom_query += wherevar + " = '" + to_string(cur_spec.denominator_val_list[cnt]) + "' AND "
                            else:
                                denom_query += wherevar + " = " + to_string(cur_spec.denominator_val_list[cnt])+ " AND "
                            cnt += 1
                        denom_query = denom_query[:-5] + " THEN 1 ELSE 0 END" 
                    else:
                        #otherwise just sum up 1 for each occurance of the variable
                        denom_query += "1"
                    denom_query += ") AS [" + outputvar + "_count_denom]" # hardcoded varname 
                else:
                #otherwise it's not a percent spec so use wherevars
                ##first build the count query since it is always calculated
                    #if there are wherevars add case, otherwise just add up 1
                    if len(cur_spec.wherevarlist) > 0:
                        select_query += " CASE WHEN "
                        cnt = 0
                        for wherevar in cur_spec.wherevarlist:
                            if 'CHAR' in input_info_dict[wherevar].basic_type:
                                select_query += wherevar + " = '" + to_string(cur_spec.wherevaluelist[cnt]) + "' AND "
                            else:
                                select_query += wherevar + " = " + to_string(cur_spec.wherevaluelist[cnt]) + " AND "
                            cnt += 1
                        select_query = select_query[:-5] + " THEN 1 ELSE 0 END" 
                    else:
                        select_query += "1"
                select_query += ") AS [" + outputvar + "_count]"
                #if it was a percent spec add the count for denominator too
                if denom_query != '' and cur_spec.is_percent:
                    select_query += denom_query
                ##now go through and add the other select statements for sum, sum of squares,min, max, and nmissing if needed.
                if cur_spec.type != "N" and cur_spec.type != "Percent":
                    sum_query = ",SUM("
                    #also making query for sum_squared at same time and will add it after complete
                    sumsq_query = ",SUM("
                    min_query = ",MIN("
                    max_query = ",MAX("
                    nmissing_query = ",SUM("
                    #if there are wherevars add case, otherwise just add up sum
                    if len(cur_spec.wherevarlist) > 0:
                        sum_query += " CASE WHEN "
                        sumsq_query += " CASE WHEN "
                        min_query += " CASE WHEN "
                        max_query += " CASE WHEN "
                        nmissing_query += " CASE WHEN "
                        cnt = 0
                        for wherevar in cur_spec.wherevarlist:
                            #need to add single quotes around character values for SQL
                            if 'CHAR' in input_info_dict[wherevar].basic_type: #double check it is lower, and no brackets around wherevar
                                sum_query += wherevar + " = '" + to_string(cur_spec.wherevaluelist[cnt]) + "' AND "
                                sumsq_query += wherevar + " = '" + to_string(cur_spec.wherevaluelist[cnt]) + "' AND "
                                min_query += wherevar + " = '" + to_string(cur_spec.wherevaluelist[cnt]) + "' AND "
                                max_query += wherevar + " = '" + to_string(cur_spec.wherevaluelist[cnt]) + "' AND "
                                nmissing_query += wherevar + " = '" + to_string(cur_spec.wherevaluelist[cnt]) + "' AND "
                            else:
                                sum_query += wherevar + " = " + to_string(cur_spec.wherevaluelist[cnt]) + " AND "
                                sumsq_query +=  wherevar + " = " + to_string(cur_spec.wherevaluelist[cnt]) + " AND "
                                min_query +=  wherevar + " = " + to_string(cur_spec.wherevaluelist[cnt]) + " AND "
                                max_query +=  wherevar + " = " + to_string(cur_spec.wherevaluelist[cnt]) + " AND "
                                nmissing_query +=  wherevar + " = " + to_string(cur_spec.wherevaluelist[cnt]) + " AND "
                            cnt += 1
                        sum_query = sum_query[:-5] # removing trailing " AND "
                        sumsq_query = sumsq_query[:-5] # removing trailing " AND "
                        min_query = min_query[:-5] # removing trailing " AND "
                        max_query = max_query[:-5] # removing trailing " AND "
                        nmissing_query +=  "[" + cur_spec.inputvar + "] IS NULL " #adding the condition the value is null
                        sum_query += " THEN ["+cur_spec.inputvar+"] ELSE 0 END" 
                        sumsq_query += " THEN CAST(["+cur_spec.inputvar+"] AS FLOAT)*CAST([" + cur_spec.inputvar+"] AS FLOAT) ELSE 0 END" 
                        min_query += " THEN [" + cur_spec.inputvar + "] ELSE NULL END"
                        max_query += " THEN [" + cur_spec.inputvar + "] ELSE NULL END"
                        nmissing_query += " THEN 1 ELSE 0 END"
                    else:
                        #no wherevars specified so just add the values
                        sum_query += "[" + cur_spec.inputvar + "]"
                        sumsq_query += "CAST(["+cur_spec.inputvar+"] AS FLOAT)*CAST([" + cur_spec.inputvar+"] AS FLOAT)"
                        min_query += "[" + cur_spec.inputvar + "]"
                        max_query += "[" + cur_spec.inputvar + "]" 
                        nmissing_query += " CASE WHEN [" + cur_spec.inputvar + "] IS NULL THEN 1 ELSE 0 END"
                    sum_query += ") AS [" + outputvar + "_sum]"
                    sumsq_query += ") AS [" + outputvar + "_sumsq]"
                    min_query += ") AS [" + outputvar + "_min]"
                    max_query += ") AS [" + outputvar + "_max]"
                    nmissing_query += ") AS [" + outputvar + "_nmiss]"
                    if cur_spec.type == "MIN": # only need min if asked for it
                        select_query += min_query
                    elif cur_spec.type == "MAX": # only need max if asked for it
                        select_query += max_query
                    elif cur_spec.type == "NMISS": # only need nmissing if asked for it
                        select_query += nmissing_query
                    else: 
                        #if not min, max, or nmiss then we need the sum
                        select_query += sum_query
                        if cur_spec.type != 'MEAN':
                            select_query += sumsq_query # only need the sum of squares if it's not calculating mean, min, nmiss or max
            #setting tmp table to select into and specifying not to get null values, and the group by
            select_query += " INTO [tmp_" + lvlvar + "] FROM [" + self.inputds + "] WHERE [" + lvlvar + "] IS NOT NULL GROUP BY [" + lvlvar + "]"
            try:
                if self.overwrite.upper() == 'Y':
                    #if supposed to overwrite we drop the table before triyng to create a new one
                    drop_table_if_exists("tmp_{0}".format(lvlvar), self.db_context)
                self.db_context.executeNoResults(select_query)
            except pyodbc.Error as er:
                print er
                print er.message
        #now get R connected to the sql database    
        if self.odbcconn != '':    
            R(Template("""
                library(RODBC)
                conn <- odbcConnect("$odbcconn")
            """).substitute(odbcconn=self.odbcconn))
        list_of_tables_to_drop = []
                
        #now we go through and do the calculations for each variable since we have the count, sum, and sum of squares
        for lvlvar in self.levelvardict:
    #        #make first column primary key in temporary table
    #            #first delete any null values
    #        cursor.execute("DELETE FROM [tmp_" + lvlvar + "] WHERE [" + lvlvar + "] IS NULL")
    #        cursor.commit()
    #        type_query = ' float'
    #        if 'char' in input_info_dict[lvlvar]["variabletype"]:
    #            type_query = " " + input_info_dict[lvlvar]["variabletype"] + "(" + to_string(input_info_dict[lvlvar]["length"]) + ")"
    #            #now set column to not null
    #        cursor.execute("ALTER TABLE [tmp_" + lvlvar + "] ALTER COLUMN " + lvlvar + type_query + " NOT NULL")
    #        cursor.commit()
    #            #and finally make it a PK
    #        cursor.execute("ALTER TABLE [tmp_" + lvlvar + "] ADD PRIMARY KEY (" + lvlvar + ")")
    #        cursor.commit()
            tier1_dict = self.levelvardict[lvlvar]
            tmp_tab_drop_list = [] # for inside loop so we know what to add for joins
            #add the name of the temporary table created in the above data collection loop to the master list of tables to drop
            list_of_tables_to_drop.append(["tmp_" + lvlvar])
            if self.overwrite.upper() == 'Y':
                drop_table_if_exists("Mean_" + lvlvar, self.db_context)
            select_query = " SELECT orig." + lvlvar 
            for outputvar in tier1_dict:
                cur_spec = tier1_dict[outputvar]
                select_query += "," + cur_spec.typefctn(cur_spec,self.inputds,input_info_dict,tmp_tab_drop_list,self.db_context) + " as [" + outputvar + "]"
            select_query += " INTO Mean_" + lvlvar + " FROM [tmp_" + lvlvar + "] orig"
            self.created_tables_list.append("Mean_" + lvlvar)
            #now setup the joins to the tables created in R (if any)
            tabcnt = 0
            for tab_list in tmp_tab_drop_list:
                spec = tab_list[1]
                #In this query we must use Group1 to compare against the level variable because in some calculations
                #    in R it does them on the levelvariable and changes the values. But it stores the original values 
                #    of that level variable inside the variable Group1. So we match that against the current table.
                #In addition we use left outer join because not all values are used (ignoring null) and if some values
                #    are null in the return table from R then they won't be matched. It is guaranteed the left table
                #    has all the values for the level variable.
                select_query += Template(""" 
                LEFT OUTER JOIN $table J$cnt ON J${cnt}.Group1 = orig.$levelvar
                """).substitute(table=tab_list[0],cnt=to_string(tabcnt),levelvar=spec.levelvar)
                tabcnt += 1
            #add the list of R tables from this iteration to the master list of tables to drop
            list_of_tables_to_drop += tmp_tab_drop_list
            try:
                self.db_context.executeNoResults(select_query)
            except pyodbc.Error as er:
                print er
                print er.message
        #cleanup. Drop tables that need it.
        for table in list_of_tables_to_drop:
            drop_table_if_exists(db_identifier_unquote(table[0]), self.db_context)
    #    print "Total time taken: " + to_string(datetime.now() - starttime)
    def get_spec(self,varname):
        """ This function will find the spec for the variable you enter. If the variable
            is not found then it will return None.
            
            Extended Summary
            ---------------------
            This will search the levelvardict attribute's values (a list of dictionaries of {varname : Spec})
            for the varname key. If it is found it will return the Spec associated with it, if it is not
            then it will return None.
            
            Parameters
            --------------
            varname : String
                This is the name of the variable to find the spec for.
        """
        #create a dictionary of { varname : {varname : spec} } for easy and fast searching
        values_dict_list = {dict_.key:dict_ for dict_ in self.levelvardict.values()}
        if varname in values_dict_list:
            return values_dict_list[varname][varname]
        return None
            
    

#if __name__ == '__main__':
#    #sample call 
#    #cProfile.run(r"Means(input_dbname='mynewdb',inputds='studentg3_n',agg_file=r'G:\SAS\SpecSheet.xls',agg_sheet='Means',percent='Y',percent_file=r'G:\SAS\SpecSheet.xls',percent_sheet='Percent',overwrite='Y')");       
#    meansclass = Means(run_context=RunContext('mynewdb'),inputds='studentg3_n',agg_file=r'G:\SAS\SpecSheet.xls',agg_sheet='Means',percent='Y',percent_file=r'G:\SAS\SpecSheet.xls',percent_sheet='Percent',overwrite='Y')
#    #meansclass = Means(run_context=RunContext('mynewdb'),inputds='studentg3_n',odbcconn='Scratch',agg_file=r'G:\SAS\SpecSheet_small.xls',agg_sheet='Means',percent='n',overwrite='Y')
#    #Means(run_context=RunContext('mynewdb'),excel='N',inputds='studentg3_n',agg_ds=r'Means',percent='Y',percent_ds=r'Percent',overwrite='Y')
#    #Means(run_context=RunContext('mynewdb'),inputds='studentg3_n',agg_file=r'G:\SAS\SpecSheet.xls',agg_sheet='Means',percent='N',overwrite='Y')
#    #Means(run_context=RunContext('mynewdb'),inputds='SmallerDS',agg_file=r'C:\SpecSheet.xls',agg_sheet='Means',percent='Y',percent_file=r'C:\SpecSheet.xls',percent_sheet='Percent',overwrite='Y')
#    meansclass.execute()