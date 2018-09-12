'''
Created on May 21, 2013

@author: zschroeder
'''

import rpy2.robjects as robjects
from airassessmentreporting.airutility.dbutilities import get_table_spec
R = robjects.r

from airassessmentreporting.airutility import (db_identifier_quote,db_identifier_unquote,SafeExcelReader,
                                                drop_table_if_exists,RunContext)
from airassessmentreporting.means import *

__all__ = [ 'peer_compare' ]

_ADD_COLUMN_QUERY = """
    ALTER TABLE [{0}] 
    ADD {1} {2}
"""
_UPDATE_MEAN_INPUT_QUERY = """
    UPDATE [{0}]
    SET type='{1}',rdvalue='',levelvar='dcrxid',variable_name={2}
    WHERE type = '{3}'
"""
_COPY_TABLE_QUERY = """
    select * into [{0}] from [{1}]
"""
_SELECT_SUBJECTS_WHEREVAR_QUERY = """
    select distinct subject,wherevar,wherevalue from [{0}]
"""
_INSERT_INTO_TABLE_QUERY = """
    INSERT INTO [{0}] ({1})
    VALUES ( ?,?,?,?,?,?,? )    
"""
_SELECT_PEER_TTEST_QUERY = """
    Select A.outputvar as predvar, B.outputvar, A.inputvar, A.subject, A.wherevar, A.wherevalue
    INTO [{table}]
    FROM [{aggtable}] A
    join [{aggtable}] B 
        ON A.inputvar = B.inputvar AND A.subject = B.subject AND A.wherevar = B.wherevar
    WHERE A.type = 'Mean' AND B.type = 'STD'
"""
_SELECT_PEER_MEAN_QUERY = """
    Select outputvar,inputvar,subject
    INTO [{table}]
    FROM [{aggtable}] A
    WHERE A.type = 'Mean'
"""
_SELECT_SIMILAR_DISTRICTS_QUERY = """
    select *
    into [{tmptab}]
    from [{disttable}]
    where CAST(F1 as FLOAT) in ( 
                                SELECT CAST( LTRIM(RTRIM(dcrxid)) as FLOAT ) as dcrxid 
                                from [{table}] 
                               ) 
"""
_SELECT_PEERMEAN_QUERY = """
    select distinct LTRIM(RTRIM(outputvar)),LTRIM(RTRIM(inputvar))
    from [{table}]
    {where}
"""
_SELECT_PEERTTEST_QUERY = """
    select distinct LTRIM(RTRIM(predvar)),LTRIM(RTRIM(outputvar)),LTRIM(RTRIM(inputvar))
    from [{table}]
   {where}
"""
_SELECT_PEERTTEST_QUERY2 = """
    select distinct LTRIM(RTRIM(predvar)),LTRIM(RTRIM(outputvar)),LTRIM(RTRIM(inputvar)),subject
    from [{table}]
   {where}
"""
_SELECT_F1_QUERY = """
    SELECT Distinct CAST(F1 as varchar) as F1
    FROM [{table}]
"""
_MERGE_SIMILARDISTRICTS = """
    Select B.F1,A.*
    from [{table}] A
    JOIN [{disttable}] B
        ON CAST(A.dcrxid as FLOAT) IN (B.F2,B.F3,B.F4,B.F5,B.F6,B.F7,B.F8,B.F9,B.F10,B.F11,B.F12,B.F13,B.F14,B.F15,B.F16,B.F17,B.F18,B.F19,B.F20,B.F21)
    where B.F1 = '{f1}'
""" 
_SUM_QUERY = "SUM( ISNULL([pweight_{subject}],0) )"
_SE_QUERY = "SUM( (ISNULL([pweight_{subj}],0) - 1)*ISNULL([{inputvar}_seh],0)*ISNULL([{inputvar}_seh],0) + ISNULL([pweight_{subj}],0)*ISNULL([{inputvar}],0)*ISNULL([{inputvar}],0) )"
_COUNT_CASE_QUERY = """
    CASE WHEN {count} <= 1 THEN NULL 
         ELSE SQRT( CASE WHEN ( {se} - {count} * ISNULL({pred},0) * ISNULL({pred},0) ) / ISNULL(NULLIF({count},0),1) / (ISNULL(NULLIF({count}-1,0),1)) < 0 THEN 0 ELSE ({se} - {count} * ISNULL({pred},0) * ISNULL({pred},0) ) / ISNULL(NULLIF({count},0),1) / (ISNULL(NULLIF({count}-1,0),1)) END )
    END AS {alias}
"""
_MEAN_CASE_QUERY = """
    CASE WHEN {count} = 0 THEN NULL 
         ELSE {total} / {count}
    END AS {alias}
"""
_INSERT_TTEST_CALCS_QUERY = """ 
INSERT INTO {table} ( 
                    {cols} 
                    ) 
SELECT {selects} 
FROM ( {froms} 
     ) A
JOIN {meanstab} B
    ON A.F1 = B.F1
Group By {groupby}
"""
_INSERT_MEANS_CALCS_QUERY = """ 
INSERT INTO {table} ( 
                    {cols} 
                    ) 
SELECT {selects} 
FROM ( {froms} ) A
GROUP BY A.F1
"""
_TTEST_CALCS_QUERY = """ 
SELECT {sels}
INTO [{table}]
FROM [{meanstab}] A
JOIN [{ttesttab}] B
    ON A.F1 = B.F1
JOIN [{dcrxidtable}] C
    ON CAST(C.dcrxid as FLOAT) = A.F1
    """
_TTEST_CASE_QUERY = """
    CASE WHEN ROUND({invar},0) = ROUND({pred},0) THEN 2
         WHEN [{invar}] IS NULL THEN 2
         WHEN [{invar}_se] IS NOT NULL AND [{invar}_se] != 0 THEN
                CASE WHEN (([{invar}] - [{pred}]) / [{invar}_se]) < (-1)*{critval} THEN 1
                     WHEN (([{invar}] - [{pred}]) / [{invar}_se]) > {critval} THEN 3
                     ELSE 2
                END
         ELSE 
             CASE WHEN pcount_{subj} = 1 or pcount_{subj} = 0 or pcount_{subj} is null THEN 2
                  ELSE 
                      CASE WHEN [{invar}] > [{pred}] THEN 3
                           WHEN [{invar}] < [{pred}] THEN 1
                           ELSE 2
                      END
             END
     END as [{alias}]
"""

def peer_compare(excel='Y',
                 agg_table='',
                 agg_file='',
                 agg_sheet='',
                 indata='',
                 outdata='',
                 pc_data='',
                 crit_val=1.96,
                 db_context=None,
                 odbcconn=''
                 ):
    """ Compares Peer group Scores for OAT with a TTEST and computes the percentage 
        of people in each level for the peer group.
        
        Parameters
        ----------------
        excel : String ('Y' or 'N')
            Required. This should be 'Y' if your aggregate information is an excel file, or 'N' if it is a SQL table.
        
        agg_table : String
            If excel='N' this should be the table name that holds the aggregate information. If excel='Y'
            this is not used.
            
        agg_file : String
            If excel='Y' this should be the path to the aggregate file. If excel='N' this is not used.
            
        agg_sheet : String
            If excel='Y' this should be the sheet name in the aggregate file. If excel='N' this is not used.
            
        indata : String
            Required. This should be the SQL table name that holds the input data.
            
        outdata : String
            Required. This will be the name of the SQL table name to hold the output data.
            
        pc_data : String
            Required. This should be the name of the SQL table that holds the peer districts information.
            
        crit_val : float
            Required. This will be the the cutoff point for calculations. It must be between 1.28 and 3.89.
            
        db_context : DBContext object
            This is the DBContext within which all processing will be done. This specifies the DB connection.
            
        odbcconn : String
            This is the name of the ODBC connection you must setup to your database for R. This can be done in 
            Control Panel -> Administrative Tools -> Data Sources.
        
        Notes
        ---------------
        I had to add ISNULL(NULLIF()) and case conditions because the query is too complex for SQL Server. It was not subsetting
        the data until after doing the select, which would get incorrect values that broke the query planner. Changing all values to 
        safe values handles this, and those values that we changed are then thrown out once it subsets to the data we want to use.
        Also note for the final query we add .0000000001 to the number before rounding. This is an attempt to compensate for the
        "smart" rounding in SAS which allows for a small margin of difference (i.e. 4.4999999999999 rounding to 5).
    """
    #defining variables we will need throughout run
    yes_list = ['Y','YES']
    no_list = ['N','NO']
    tmp_aggtable_name = 'peercompare_tmp_agg'
    tmp_peertable_t_name = 'peercompare_tmp_t'
    tmp_peertable_mean_name = 'peercompare_tmp_mean'
    tmp_similar_districts_table = 'peercompare_tmp_dist'
    tmp_tables_list = []
    #error checking on params
    if db_context is None:
        raise ValueError( "ERROR: You must pass a DBContext" )
    if odbcconn.strip() == '':
        raise ValueError( "ERROR: You must pass an odbc connection" )
    if pc_data.strip() == '':
        raise ValueError( "ERROR: You must pass a table name for pc_data" )
    run_context = db_context.runContext
    if excel.upper() in yes_list:
        drop_table_if_exists(db_context=db_context, table=tmp_aggtable_name)
        
        # create a DB table with the information from the aggregate file,
        # then we will add columns "type","rdvalue",and "variable_name" to it so 
        # it is a valid aggregate file for the means script.
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
    # now we add the 'levelvar' and 'rdvalue' columns to the table we created. These columns
    # are needed for creating a meansspec from the table. 
    # adding levelvar column
    db_context.executeNoResults(_ADD_COLUMN_QUERY.format(tmp_aggtable_name,'levelvar','varchar(10)'))
    # adding rdvalue column
    db_context.executeNoResults(_ADD_COLUMN_QUERY.format(tmp_aggtable_name,'rdvalue','varchar(10)'))
    
    # add the column variable_name and copy the value from outputvar into it. This column
    # is needed for creating a meansspec from the table.
    length = tmptab_tablespec[db_identifier_quote('inputvar')].data_length + 4
    db_context.executeNoResults(_ADD_COLUMN_QUERY.format(tmp_aggtable_name,'variable_name','VARCHAR(' + str(length) + ")"))
    
    #now update the values for the columns we just added
    
    # first we change the values for those we are getting Means for
    db_context.executeNoResults(_UPDATE_MEAN_INPUT_QUERY.format(tmp_aggtable_name,'MEAN',"inputvar",'Mean'))
    # next we update those we are getting T-Tests for
    db_context.executeNoResults(_UPDATE_MEAN_INPUT_QUERY.format(tmp_aggtable_name,'STD',"RTRIM(LTRIM(inputvar)) + '_seh'",'TTEST'))
    
    # get a list of 3-tuples of (subject,wherevar,wherevalue)
    tuples_list = [ (x,y,z) for x,y,z in db_context.executeBuffered( _SELECT_SUBJECTS_WHEREVAR_QUERY.format( tmp_aggtable_name ) ) ]
    
    cols = [ '[variable_name]','[levelvar]','[wherevar]','[wherevalue]','[rdvalue]','[type]','[subject]' ]
    _insert_query = _INSERT_INTO_TABLE_QUERY.format( tmp_aggtable_name, ",".join(cols) )
    
    for row in tuples_list:
        subject = row[0]
        wherevar = row[1]
        wherevalue = row[2]
        # harcoding variable_name to pweight_{subject}
        values = [ 'pweight_' + subject, 'dcrxid',wherevar,wherevalue,"",'N',subject ]
        db_context.executeNoResults( _insert_query, values )
    
    # now the aggregate table is ready for means, so run means on it
    means_class = Means( excel='N',agg_ds=tmp_aggtable_name,db_context=db_context,inputds=indata,
                         odbcconn=odbcconn,overwrite='Y' )
    means_class.execute()
    
    if len(means_class.created_tables_list) != 1:
        raise ValueError( "Error: Means call returned more than one table. It should only return 'Mean_DCRXID' but it returned " + ",".join(means_class.created_tables_list) )
    
    # setup the Peerttest dataset in SAS - This holds the variables we will loop through
    drop_table_if_exists( tmp_peertable_t_name, db_context )
    db_context.executeNoResults( _SELECT_PEER_TTEST_QUERY.format( table=tmp_peertable_t_name, aggtable=tmp_aggtable_name ) )
    tmp_tables_list.append( tmp_peertable_t_name )
    
    # setup the Peermean dataset in SAS - This holds the variables we will loop through
    drop_table_if_exists( tmp_peertable_mean_name, db_context )
    db_context.executeNoResults( _SELECT_PEER_MEAN_QUERY.format( table=tmp_peertable_mean_name, aggtable=tmp_aggtable_name ) )
    tmp_tables_list.append( tmp_peertable_mean_name )
    
    # getting similar districts into table tmp_similar_districts_table
    drop_table_if_exists( tmp_similar_districts_table, db_context )
    db_context.executeNoResults( _SELECT_SIMILAR_DISTRICTS_QUERY.format( tmptab=tmp_similar_districts_table,disttable=pc_data,
                                                                         table=indata ) )
    tmp_tables_list.append( tmp_similar_districts_table )
    # list of distinct subjects that appear in tmp_aggtable_name
    subj_list = list( set( tup[0] for tup in tuples_list ) ) 
    
    # creating 2 tables, one for means calculations and one for ttest calculations
    vars_list_means = [ x for x in db_context.executeBuffered( _SELECT_PEERMEAN_QUERY.format( table=tmp_peertable_mean_name,where='' ) ) ]
    vars_list_ttest = [ x for x in db_context.executeBuffered( _SELECT_PEERTTEST_QUERY.format( table=tmp_peertable_t_name,where='' ) ) ]
    means_cols = [ "F1" ] + [ "pcount_" + x for x in subj_list ]
    ttest_cols = [ "F1" ]
    for outvar,invar in vars_list_means:
        if outvar is not None and outvar.strip() not in means_cols:
            means_cols.append( outvar.strip() )
        if invar is not None and invar.strip() + "_total" not in means_cols:
            means_cols.append( invar.strip() + "_total" )
    for predvar,outvar,invar in vars_list_ttest:
        if invar is not None and invar.strip() + "_se" not in ttest_cols:
            ttest_cols.append( invar.strip() + "_se" )
    # create temporary table to hold all results from calculations for means, and one for ttest
    tmp_mean_calculated_table = "peermean_tmp_calc_mean"
    tmp_ttest_calculated_table = "peermean_tmp_calc_ttest"
        # first means
    query = "CREATE TABLE " + tmp_mean_calculated_table + " ( [" + """] float,
                        [""".join(means_cols) + "] float )"
    drop_table_if_exists(tmp_mean_calculated_table, db_context)
    db_context.executeNoResults( query ) 
    tmp_tables_list.append( tmp_mean_calculated_table )
        # now ttest
    query = "CREATE TABLE " + tmp_ttest_calculated_table + " ( [" + """] float,
                        [""".join(ttest_cols) + "] float )"
    drop_table_if_exists(tmp_ttest_calculated_table, db_context)
    db_context.executeNoResults( query ) 
    tmp_tables_list.append( tmp_ttest_calculated_table )
    # cleanup
    del vars_list_ttest
    del query
    del means_cols
    del ttest_cols
    
    #define where clause for peermean and peerttest queries
    whereclause= "where subject = '{subj}'"
    
    # get list of distinct F1 values from out similar districts table
    f1_list = [ x[0] for x in db_context.executeBuffered( _SELECT_F1_QUERY.format( table=tmp_similar_districts_table ) ) ]
    
    # Go through the districts and insert them into the final table one at a time, inserting null for any values not calculated
    for f1 in f1_list:
        # list to hold all the select queries we create. One for the means table and one for the ttest table
        select_list_means = [ "A.F1" ]
        select_list_ttest = [ "A.F1" ]
        cols_means = [ "F1" ]
        cols_ttest = [ "F1" ]
        select_table_query = _MERGE_SIMILARDISTRICTS.format( table=means_class.created_tables_list[0],
                                                             disttable=tmp_similar_districts_table,f1=f1)
        ttest_groupby = [ "A.F1" ]
        for subject in subj_list:
            # means_output_input_list is of the form  [ (outvar,invar) ] 
            means_output_input_list = [ x for x in db_context.executeBuffered( _SELECT_PEERMEAN_QUERY.format( table=tmp_peertable_mean_name,where=whereclause.format(subj=subject) ) ) ]
            # ttest_prev_input__list is of the form [ (predvar,outputvar,inputvar) ]
            ttest_prev_input__list = [ x for x in db_context.executeBuffered( _SELECT_PEERTTEST_QUERY.format( table=tmp_peertable_t_name,where=whereclause.format(subj=subject) ) ) ]
            _count = _SUM_QUERY.format(subject=subject)
            for outvar,invar in means_output_input_list:
                total_query = "SUM( ISNULL([pweight_{subj}],0) * ISNULL([{invar}],0) )".format( subj=subject,invar=invar )
                select_list_means.append( total_query +  " as [" + invar + "_total]" )
                select_list_means.append( _MEAN_CASE_QUERY.format( count=_count, total=total_query, alias=db_identifier_quote(outvar) ) )
                cols_means.append( "[" + invar + "_total]" )
                cols_means.append( db_identifier_quote(outvar) )
                # adding count variable
                if _count + " as [pcount_{subj}]".format( subj=subject ) not in select_list_means:
                    select_list_means.append(_count + " as [pcount_{subj}]".format( subj=subject ) )
                    cols_means.append( "[pcount_{subj}]".format( subj=subject ) )
            for predvar,outvar,inputvar in ttest_prev_input__list:
                se_query = _SE_QUERY.format( subj=subject,inputvar=inputvar )
                select_list_ttest.append( _COUNT_CASE_QUERY.format( count="pcount_" + subject,se=se_query,pred=predvar,alias= "[" + inputvar + "_se]" ) )
                cols_ttest.append( "[" + inputvar + "_se]" )
                ttest_groupby.append( predvar )
            if "pcount_" + subject not in ttest_groupby:
                ttest_groupby.append( "pcount_" + subject )
        # now insert into means table
        insert_query = _INSERT_MEANS_CALCS_QUERY.format( table=tmp_mean_calculated_table,cols="""
                                                ,""".join(cols_means),selects="""
                                                ,""".join(select_list_means),froms=select_table_query )
        db_context.executeNoResults( insert_query )
        #now insert into ttest table
        insert_query = _INSERT_TTEST_CALCS_QUERY.format( table=tmp_ttest_calculated_table,cols="""
                                                ,""".join(cols_ttest),selects="""
                                                ,""".join(select_list_ttest),froms=select_table_query,
                                                meanstab=tmp_mean_calculated_table, groupby=",".join(ttest_groupby) )
        db_context.executeNoResults( insert_query )
    
    # now we go through these output tables and get the t-values
    # setup initial select query list
    select_query_list = [ "C.dcrxid" ] + list( set( [ "ROUND(" + x + " + .0000000001,0) as [" + x + "]" for x,y in vars_list_means ] ) )
    for predvar,outvar,invar,subject in [ x for x in db_context.executeBuffered( _SELECT_PEERTTEST_QUERY2.format( table=tmp_peertable_t_name,where='' ) ) ]:
        select_query_list.append( _TTEST_CASE_QUERY.format( invar=invar,pred=predvar,critval=crit_val,alias=outvar,subj=subject ) )
    final_query = _TTEST_CALCS_QUERY.format( sels=",".join(list(set(select_query_list))), table=outdata, 
                                                            meanstab=tmp_mean_calculated_table,ttesttab=tmp_ttest_calculated_table,
                                                            dcrxidtable=means_class.created_tables_list[0] )
    drop_table_if_exists(outdata, db_context)
    db_context.executeNoResults( final_query )
    
    #cleanup temporary tables
    for table in tmp_tables_list:
        drop_table_if_exists(table, db_context)
    
#if __name__ == "__main__":
#    agg = r'G:\SAS\AggregationSheet.xls'
#    runContext = RunContext('mynewdb')
#    db_context = runContext.getDBContext()
#    peer_compare( excel='Y', agg_file=agg, agg_sheet='PeerCompare', indata='stud10', outdata='peerCompare', 
#                  pc_data=r'similardist', crit_val=1.96, db_context=db_context, odbcconn='Scratch' )
    