######################################################################
# 
# (c) Copyright American Institutes for Research, unpublished work created 2013 
#  All use, disclosure, and/or reproduction of this material is 
#  prohibited unless authorized in writing. All rights reserved. 
# 
#  Rights in this program belong to: 
#   American Institutes for Research. 
# 
######################################################################

import csv
from datetime import datetime
from airassessmentreporting.airutility import FormatOut

__all__ = ['export']

def export (
  context=None, table_name=None, out_file=None, dbms=None, 
  delimiter=None, where=None, columns=None, orderby=None, 
  lspec=None, lskip=None, layout=None, replace=0, notes=1):
    #
    """
    export a table's rows as formatted output to a flat file.
    
    Extended Summary: 
    -----------------
    Read a Microsoft SQL Server or SQL Express table and write its values to an output file where various parameters specify: 
    
      - Whether to include a special header row as an output file's first line with the delimited column names.
      - The output format for a file line, each of which contain one row (observation) of data values
      - Use of delimiter characters or a fixed width layout for output values. 
    
    Parameters:
    -----------
    NOTE: Some parameter setting imply others, and some cannot be used together. The parameter names and interactions are modeled after SAS handling in an attempt to parlay the knowledge of former SAS programmers.
    
    context: RunContext of SuiteContext or duck type with/without 'conn' member
    
    table_name : string 
      A table_name. This specifies the input SQL express table name with respect to the DB connection string in the run_context
        
    out_file : string 
      The complete path name of the file to which to write the exported data.
       
      - If the filename ends with ".csv" it implies other settings for parameters are: dbms='csv' and delimiter=','. 
      - If the filename ends with ".txt" it implies that parameters dbms='txt' and delimiter='\t' (tab). 
      
    dbms : string 
      data-format-identifier: The string is one of the following values:
      
      - "csv": this value specifies that the output file will be written with a header line of table column names, separated by a comma, followed by a line of output for each input row of data values.
      - "txt": this means the same as a csv file, except the output delimiter is a tab character rather than a comma
        
      - "dlm" : Output file will be character-delimited. 
      
        - dlm setting is only allowed where outfile has no .csv nor .txt extension.
        - Default delimiter is blank/space character. 
        - This setting allows optional argument setting such as delimiter='|' to specify a non-blank delimiter character
          
      - "fixed":  Output will be in fixed format, per parameter lspec setting.
      - None: Output will be produced without a starting row of column names.
      
    lspec : string 
        Use only when dbms="fixed". This provides the specification name. Default value of "spec1" enables setting additional parameters layout and lskip.
                
    layout : string 
       It is the name of an Excel workbook filename where Sheet1 has an output specification or layout info per dataset table column. More details are in method _read_layout(). Use only when parameter lspec="spec1".
       
    lskip : int 
      Indicates to ignore this quantity of beginning sheet rows of the layout workbook file. Use only when parameter lspec is "spec1". Default is 0.
                
    columns : string
      The comma-separated string of column names to select from the input data table. 
      
      - Cannot be used with the dbms="fixed" option because the fixed format may define the output columns via a layout file.
      - Example: columns="name, phone, zipcode" 
      - Default is all columns in the order defined in the data table itself.
      
    where : string
      An sql 'where clause' with conditions to select (or in SAS terms to 'subset') rows from the dataset.
       
      - Example: where = "zip_code = '22015' and number_podengos = '3' "
                
    orderby : string 
      A string of comma-separated data column names used to order the rows of output.
      
      - Example: orderby="zip_code, number_podengos"
      
    replace : int
      Value 1 specifies to replace existing output filename. 
      Value 0 means to exit with error if outfile exists. Default is 0.
      
    Returns:
    --------
    return value: None
      However, of course, the formatted output file is created
      
    Raises:
    -------
    ValueError is raised in cases where the input parameters are invalid.

    Note:
    -----
    Unit tests for export() are coded in tests/export_tests.py
    """
    # Verify the arguments/options data
    if (context is None):
        raise ValueError("context argument was not given.")

    # If context has member "conn" reuse the TEST db connection
    if (hasattr(context, "conn")):
        cnxn = context.conn
        context.info("export(): DB connection server='%s' db='%s'" 
          % (context.server, context.db))
    else:
        # basic RunContext() here. Must connect to DB.
        now = str(datetime.now())
        context.info("export(): DB Connecting at time '%s':" % now)
        cnxn = context.getDBContext()._getDefaultConn()
    
    starttime = str(datetime.now())
    context.info("export(): Start main code at time '%s':" % starttime)
    
    if (table_name is None or table_name == ""):
        raise ValueError("table_name is missing.")
            
    context.info("export(): table='%s'"  % (table_name))    
    # out_file and dbms interaction
    if (out_file is None):
        raise ValueError("out_file argument is not given.")
    context.info("export(): out_file='%s'" % (out_file))
    if (replace is None):
        replace = 0
    if (replace == 0):
        with open(out_file, 'r'): 
            raise ValueError(
              "out_file='%s' exists and replace is not allowed."
              % out_file)
    
    dotvals = out_file.split('.')
    fileext = (dotvals[len(dotvals) -1]).lower()
    if fileext == 'csv':
        if dbms is not None and dbms != 'csv':
            raise ValueError("When output extension "
            "is csv then dbms must be unset or comma, and "
            "dbms='%s' is an error." % (dbms)) 
        dbms = 'csv'
    elif fileext == 'txt' :
        if dbms is not None and dbms != 'tab':
            raise ValueError("When output extension "
            " is txt then dbms must be unset or tab, and "
            "dbms='%s' is an error." % (dbms))
        dbms = 'tab'
    # dbms
    if dbms == 'csv':
        if delimiter is not None and delimiter != ',':
            raise ValueError("When dbms is 'csv' "
            "then delimiter must be unset or ',' , and "
            "delimiter='%s' is an error." % (delimiter))
        delimiter = ','
    elif dbms == 'tab':
        if delimiter is not None and delimiter != '\t':
            raise ValueError("When dbms is 'tab' "
            "then delimiter must be unset or '\t' , and "
            "delimiter='%s' is an error." % (delimiter))
        delimiter = '\t'
    elif dbms == 'dlm' or dbms is None:
        if delimiter is None:
            # No specification of dbms means 'dlm'
            delimiter = ' '
    elif dbms == 'fixed':
        if layout is None:
            raise ValueError("When dbms is 'fixed' "
            " then layout must be specified.")
        if lspec is None:
            lspec = "spec1"
        if lskip is None:
            lskip = 0
        elif lskip < 0:
            raise ValueError(
              "When lskip is not None it must be 0 or greater.")
          
        if columns is not None:
            raise ValueError("When dbms is fixed "
            " then COLUMNS setting is not allowed")
    else:
        raise ValueError("dbms='%s' is not implemented." % dbms)
    
    context.info("export(): dbms='%s'" % (dbms))
        
    if delimiter is not None:
        context.info("export(): delimiter='%s'" % (delimiter))
        if len(delimiter) != 1:
            raise ValueError("delimiter '%s' is not implemented." % dbms)
    if layout is not None:
        context.info("export(): layout='%s'" % (layout))
    if lspec is not None:
        context.info("export(): lspec='%s'" % (lspec))
    if lskip is not None:
        context.info("export(): lskip='%s'" % (lskip))

    if columns is not None:
        context.info("export(): columns='%s'" % (columns))
        select_columns = columns
    else:
        select_columns = None
              
    # Perform the output. 
    with open(out_file, 'wb') as fout:
        # Collect table column names in tcnames.
        cursor = cnxn.cursor()
        tcnames=[]
        for c in cursor.columns(table=table_name):
            tcnames.append(c.column_name)
        tcname_str = ",".join(tcnames)
        context.info("export(): table '%s' ALL columns='%s'" 
            % (table_name,tcname_str))
        
        # Create an output writer based on input params.
        if (dbms == 'fixed'):
            writer = FormatOut(fout, spec=lspec, lskip=lskip, 
              layout=layout)
            # glean table writer's columns as SELECT_COLUMNS
            select_columns = writer.columns
        else:
            writer = csv.writer(fout, delimiter=delimiter, dialect='excel',
              quoting=csv.QUOTE_MINIMAL)
             
        # Prepare sql select statement
        context.info("export(): select_columns='%s'" % (select_columns))
        sql = "select "
        if (select_columns is not None):
            sql += select_columns
        else:
            sql += r' * '
            
        sql += " from %s " % table_name
        context.info("export(): where='%s'" % (where))
        if (where is not None):
            sql += "where %s " % where
        context.info("export(): orderby='%s'" % (orderby))
        if (orderby is not None):
            sql += "order by %s " % orderby
        context.info("export(): sql='%s'" % (sql))
        
        if ( dbms != 'fixed' and dbms is not None):
            # Prepare outcols to write first output row of column names
            outcols = []        
            if (select_columns is not None):
                select_columns = select_columns.replace(" ","")
                scols = select_columns.split(',')
                for c in scols:
                    if (c in tcnames):
                        #Do not bother to report bad column name here.
                        #The sql execution will catch it.
                        outcols.append(c)
            else:
                # outcols are all table column names
                outcols = tcnames
            if (len(outcols) > 0):
                writer.writerow(outcols)
        # Query for table rows
        result = cursor.execute(sql)
        # Cycle through input table rows to write each one.
        nrows = 0
        for row in result.fetchall():
            writer.writerow(row)
            nrows += 1
        context.info(
          "export(): Wrote %d rows from table='%s' to out_file='%s'"
          % (nrows, table_name, out_file))
        # flushstart = str(datetime.now())
        fout.close()
        context.info("export(): %s was start time. " % starttime)
        # context.info("export(): %s was start of output flush. " % flushstart)
        context.info("export(): %s was end time." % str(datetime.now())) 
        context.info("==============================================\n") 
             
    return nrows

# Unit tests for export() exist separately under tests/export_tests.py

