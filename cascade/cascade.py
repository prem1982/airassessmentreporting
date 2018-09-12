'''
Created on May 13, 2013

@author: zschroeder
'''

from string import Template
import rpy2.robjects as robjects
from airassessmentreporting.airutility import *
from airassessmentreporting.means import *
R = robjects.r

__all__ = [ 'cascade' ]

_ADD_COLUMN_QUERY = """
    ALTER TABLE [{0}] 
    ADD {1} {2}
"""
_UPDATE_TYPE_AND_RDVALUE_QUERY = """
    UPDATE [{0}]
    SET type='MEAN',rdvalue=''
"""
_UPDATE_COLUMN_VALUES_QUERY = """
    UPDATE [{0}]
    SET {1} = {2}
"""
_COPY_TABLE_QUERY = """
    select * into [{0}] from [{1}]
"""
_CREATE_TABLE_QUERY = """
    SELECT {0}
    INTO {1}
    FROM {2} 
"""
_GET_VARNAME_QUERY = """
    select variable_name from [{0}]
"""
_R_QUERY = """
     dat <- sqlQuery(conn,"Select {levelvar},{column},{selectlist} from {table} WHERE {where}")
     fm <- lm({column} ~ {indepvars}, data=dat,na.action=na.exclude)
     dat$fit <- fitted(fm)
     X <- model.matrix(fm)
     XtXInv <- solve(crossprod(X))
     dat$se <- sqrt(diag((tcrossprod(X, XtXInv) %*% t(X) + 1) * summary(fm)$sigma^2))
     dat$t <- (dat${column}-dat$fit)/dat$se   
     newFrame <- data.frame({levelvar}=dat${levelvar},{column}=dat${column},T=dat$t,row.names='{levelvar}')
     sqlSave(conn,newFrame,tablename="{outtable}")
"""
_SELECT_LAYOUT_QUERY = """
    SELECT outputvar,subject,level,levelvar,inputvar,criterion,
           grade,wherevar,wherevalue,import_order,type,rdvalue,variable_name
    FROM [{0}]
"""
_FINAL_T_CHECK_QUERY = """
    SELECT [{0}],{1}.T,
        CASE WHEN {1}.T < (-1)*({1}.{2}) THEN 1
             WHEN {1}.T > {1}.{2} THEN 3
             ELSE 2 END AS [{3}]
    INTO [{3}]
    FROM [{4}]
"""
_T_CASE_STATEMENT = """
    CASE WHEN {0}.T < (-1)*({1}) THEN 1
             WHEN {0}.T > {1} THEN 3
             ELSE 2 END AS [{2}]
"""
class CascadeLayout:
    "This class holds the layout information for the cascade function"
    def __init__(self):
        self.outputvar = ''
        self.level = ''
        self.levelvar = ''
        self.subject = ''
        self.inputvar = ''
        self.criterion = 1
        self.grade = ''
        self.wherevar = ''
        self.wherevalue = ''
        self.import_order = 1
        self.type = ''
        self.rdvalue = ''
        self.variable_name = ''
    
def get_cascade_layouts(tablename,db_context):
    """ This function will go to the database and grab the cascade layout to store in memory.
    
    Parameters
    ---------------
    tablename : String
        This is the table name that holds the layout information.
    
    db_context : DBContext object
        This is the DBContext that will be used to query the database.
        
    Returns
    -------------
    This returns a dictionary of { variable_name : CascadeLayout}.
    
    Notes
    -----------
    This function assumes the existence of the CascadeLayout object
    
    """
    layout_dict = {}
    for row in db_context.executeBuffered(_SELECT_LAYOUT_QUERY.format(tablename)):
        new_layout = CascadeLayout()
        # the order of the columns is hardcoded, so we can hardcode what value goes where
        new_layout.outputvar = row[0]
        new_layout.subject = row[1]
        new_layout.level = row[2]
        new_layout.levelvar = row[3]
        new_layout.inputvar = row[4]
        new_layout.criterion = row[5]
        new_layout.grade = row[6]
        new_layout.wherevar = row[7]
        new_layout.wherevalue = row[8]
        new_layout.import_order = row[9]
        new_layout.type = row[10]
        new_layout.rdvalue = row[11]
        new_layout.variable_name = row[12].lower()
        layout_dict.update({new_layout.variable_name.strip():new_layout})
    return layout_dict

def cascade(excel='Y',          # Can be 'Y' or 'N'. 'Y' if the aggregate file is an excel file
            agg_table='',       # If excel='N' then this should be the name of the table that holds the aggregate information
            agg_file='',        # if excel='Y' this should be the name and path to the aggregate file 
            agg_sheet='',       # if excel='Y' this should be the name of the sheet holding the information in the aggregate file
            inputds='',         # this should be the name of the table that holds the input data
            db_context=None,   # this is the DBContext in which the script will be run.
            odbcconn=''         # this is the ODBC connection you must setup to your database for R
            ):
    """ The Cascade macro is used in identifying the weakest content strand for each entity in Ohio project.
    
        Parameters
        ----------------
        excel : string ('Y' or 'N')
            This indicated whether the aggregate file is an excel file or a SQL table. If excel='Y' then the
            aggregate file is an excel file, if excel='N' then it is a SQL table.
        
        agg_table : string
            If excel='N' then this should be the name of the table that holds the aggregate information. If
            excel='Y' then this is not used.
        
        agg_file : string
            If excel='Y' this should be the name and path to the aggregate file. If excel='N' then this is
            not used.
        
        inputds : string
            This should be the name of the table that holds the input data.
        
        db_context : DBContext object
            This is the DBContext within which all processing will be done. This specifies the DB connection.
            
        odbcconn : String
            This is the name of the ODBC connection you must setup to your database for R. This can be done in 
            Control Panel -> Administrative Tools -> Data Sources.
        
        Returns
        ----------
        Nothing. Creates tables in database with names "cascade_{level}_{subject}"
        
        Notes
        ----------
        This script uses the Means script. It uses the means function, as well as the read_meansspec functions.           
    """
    yes_list = ['Y','YES']
    no_list = ['N','NO']
    tmp_aggtable_name = 'cascade_tmp_agg'
    tmp_tables_list = []
    if db_context is None:
        raise Exception("ERROR: You must pass a DBContext")
    if odbcconn.strip() == '':
        raise Exception("ERROR: You must pass an odbc connection")
    run_context = db_context.runContext
    if excel.upper() in yes_list:
        drop_table_if_exists(db_context=db_context, table=tmp_aggtable_name)
        
        # create a DB table with the information from the aggregate file,
        # then we will add columns "type" and "rdvalue" to it so it is a valid
        # aggregate file for the means script.
        reader = SafeExcelReader(run_context,db_context=db_context,filename=agg_file,
                                 sheet_name=agg_sheet,output_table=tmp_aggtable_name)
        reader.createTable()
    elif excel.upper() in no_list:
        if agg_table.strip() == '':
            raise ValueError( "ERROR: You must specify an aggregate table name when excel='N'" )
        drop_table_if_exists(db_context=db_context, table=tmp_aggtable_name)
        
        # first we copy the aggregate information to a temp table
        db_context.executeNoResults(_COPY_TABLE_QUERY.format(tmp_aggtable_name,agg_table))
    else:
        raise Exception("ERROR: parameter 'excel' must have a value of either 'Y' or 'N'")
    tmptab_tablespec = db_context.getTableSpec(tmp_aggtable_name)
    tmp_tables_list.append(tmp_aggtable_name)
    # now we add the 'type' and 'rdvalue' columns to the table we created. These columns
    # are needed for creating a meansspec from the table. 
    # adding type column
    db_context.executeNoResults(_ADD_COLUMN_QUERY.format(tmp_aggtable_name,'type','varchar(10)'))
    # adding rdvalue column
    db_context.executeNoResults(_ADD_COLUMN_QUERY.format(tmp_aggtable_name,'rdvalue','varchar(10)'))
    # now set the values for the columns we just added (type="MEANS",rdvalue="")
    db_context.executeNoResults(_UPDATE_TYPE_AND_RDVALUE_QUERY.format(tmp_aggtable_name))
    # add the column variable_name and copy the value from outputvar into it. This column
    # is needed for creating a meansspec from the table.
    length = tmptab_tablespec[db_identifier_quote('levelvar')].data_length + \
             tmptab_tablespec[db_identifier_quote('subject')].data_length + \
             tmptab_tablespec[db_identifier_quote('inputvar')].data_length
    db_context.executeNoResults(_ADD_COLUMN_QUERY.format(tmp_aggtable_name,'variable_name','VARCHAR(' + str(length + 8) + ")"))
    db_context.executeNoResults(_UPDATE_COLUMN_VALUES_QUERY.format(tmp_aggtable_name,'variable_name',
                                                                   "'mean_' + levelvar + '_' + subject + '_' + inputvar"))
    
    #store the layouts from the tmp table for easy access
    layout_dict = get_cascade_layouts(tmp_aggtable_name,db_context)
    
    # defining the means class and calling execute() to run the means script
    means_class = Means(excel='N',agg_ds=tmp_aggtable_name,db_context=db_context,inputds=inputds,odbcconn=odbcconn,overwrite='Y')
    means_class.execute()
    
    #setup connection between R and SQL
    R(Template("""
                library(RODBC)
                conn <- odbcConnect("$odbcconn")
               """).substitute(odbcconn=odbcconn))
    joins_dict = {}
    
    # go through each table means outputs, and for each column in each table we perform regression where all other columns
    # in that table are the independent variables, and the current column is the dependent variable. 
    for tablename in means_class.created_tables_list:
        tmp_tables_list.append(tablename)
        current_tablespec = db_context.getTableSpec(tablename)
        levelvar = tablename[5:]
        #dont want the column that is the values for the by variable. Also want colnames without square brackets
        #NOTE: tablename[5:] because the table name starts with "Mean_"
        current_col_list = [db_identifier_unquote(colname.field_name) for colname in current_tablespec if not colname.field_name == db_identifier_quote(levelvar)]
        for column in current_col_list:
            
            col_table_name = 'tmp_cascade_' + column # the name we will use for the temp table to hold the results from regression
            layout = layout_dict[column]
            level = layout.level
            subject = layout.subject
            cascade_table_name = 'cascade_'+ level + '_' + subject # the name of the final cascade output table
            
            #drop these tables if they arleady exist
            drop_table_if_exists(db_context=db_context, table=col_table_name)

            #list of the independent vars for regression
            indep_vars = [varname for varname in current_col_list if not varname == column]
            
            #only want to do calculations where the value is not null
            where_clause = column + " IS NOT NULL AND " + " IS NOT NULL AND ".join(indep_vars) + " IS NOT NULL "
            #do regression in R here
            r_query = _R_QUERY.format(levelvar=levelvar, column=column, selectlist=",".join(indep_vars), where=where_clause,
                                      table=tablename, indepvars="+".join(indep_vars), outtable=col_table_name)
            R(r_query)
            
            #adding regression table to list of temp tables to drop
            tmp_tables_list.append(col_table_name)
            if not cascade_table_name in joins_dict:
                joins_dict[cascade_table_name] = [col_table_name]
            else: 
                joins_dict[cascade_table_name].append(col_table_name)
    
    join_query = '''JOIN {0} {1} ON {1}.{2} = A1.{2}
    ''' # adding newline so it is easier to read
    
    for finaltable in joins_dict:
        cnt = 1
        join_list = [] # running list of joins for this table
        select_list = [] # running list of queries to piece together for the select statement for this table
        for innertable in joins_dict[finaltable]:
            # table spec for current sub table
            tablespec = db_context.getTableSpec(innertable)
            # key column
            key_column = tablespec[0].field_name
            # kind of hacky. setting the key columns name based on the levelvar of the second column
            # in the temp table R created. Doing this assumes that all variables in the table have
            # the same levelvar, which seems to be what SAS did too.
            key_alias = layout_dict[db_identifier_unquote(tablespec[1].field_name)].levelvar
            select_query = ''
            # only want to add the key column once
            if cnt == 1:
                select_query = "A1." + key_column + " as " + key_alias + ","
            # getting columns in table to add to select query
            cols = [db_identifier_unquote(x.field_name) for x in tablespec if x.field_name != key_column and x.field_name != '[t]']
            # create case statements for the select query
            for col in cols:
                layout = layout_dict[col]
                alias = "A" + str(cnt)
                case_query = _T_CASE_STATEMENT.format(alias,layout.criterion,layout.outputvar)
                if len(select_query) > 0 and select_query[-1] != ',':
                    select_query += ","
                select_query += case_query
                select_list.append(select_query)
            if cnt == 1:
                #if it's the first table then it will be in the from clause
                join_list.append(innertable + """ A1 
                """) # adding a space for nicer formatting
            else:
                join_list.append(join_query.format(innertable,alias,key_column))
            cnt += 1
        # will wipe out the table if it already exists
        drop_table_if_exists(db_context=db_context, table=finaltable)
        final_query = _CREATE_TABLE_QUERY.format(",".join(select_list),finaltable," ".join(join_list))
        db_context.executeNoResults(final_query)
            
    #clean up our temp tables
    for table in tmp_tables_list:
        drop_table_if_exists(db_context=db_context, table=table)
            
#if __name__ == "__main__":
#    cascade(excel='Y', agg_file=r'C:\Project\ScoreReportMacros\UnitTested\Cascade\test\AggSheet.xls', 
#            agg_sheet='cascade_test', inputds='studentg3_n', run_context=RunContext('mynewdb'),odbcconn='Scratch')
    