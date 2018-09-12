'''
Created on Mar 13, 2013

@author: zschroeder
'''
from string import Template
from airassessmentreporting.airutility import *

__all__ = [ 'erasure' ]

def erasure(db_context=None,   #the DBContext this will be run in
            inputds1='',        #the table name for the first half of input data (will join with inputds2 on lithocode)
            inputds2='',        #the table name for the second half of input data (will join with inputds1 on lithocode)
            bookmaps=[],        # a list of bookmaps (from BookMapReader)
            outputds='',        #name of output table
            overwrite='Y'       #overwrite output table? Y/N
            ):
    """Analysis of erasure marks
    
        Extended summary
        -------------------------
        This function does analysis on the erasure marks of a test, and outputs the results to a table. It reports on
        wrong to right erasures, right to wrong erasures, and wrong to wrong erasures (along with the total number
        of erasures).
        
        Parameters
        --------------      
         db_context : DBContext object
            This is the DBContext within which all processing will be done. This specifies the DB connection.
            
        inputds1 : String
            This should be the name of the table that contains the first half of the data. This will be joined
            with inputds2 on lithocode.
            
        inputds2 : String
            This should be the name of the table that contains the second half of the data. This will be joined
            with inputds1 on lithocode.
            
        bookmaps : list of BookMap objects
            This should be the return value of running BookMapReader function on a bookmap locations file.
            
        outputds : String
            This specifies what we should name the output table.
            
        overwrite : String ('Y' or 'N')
            This should be either 'Y' to overwrite the output table, or 'N' to throw an error if the table already
            exists.

        Returns
        ----------
        Nothing.     
        
        Notes
        --------
        This function relies on the return value of the BookMapReader class. It takes this as an input
        parameter. Also Note I had to add a lot of ISNUMERIC() ELSE 0 checks because the query is too complex
        for SQL server. The execution plan was not doing the where clause first and it was getting some of the 
        wrong values. By doing ISNUMERIC() ELSE 0 we allow it to use those wrong values, which it will then throw
        away once it gets to the where clause.
    """
    YorN = ['Y','N']
    if db_context is None:
        raise Exception("Error: Must pass a DBContext in parameter db_context")
    if inputds1.strip() == '' or inputds2.strip() == '':
        raise Exception("Error: Must pass input table names as parameters inputds1 and inputds2")
    if outputds.strip() == '':
        raise Exception("Error: Must pass output table name as parameters outputds")
    if overwrite.upper().strip() not in YorN:
        raise Exception("Error: overwrite must be either " + " or ".join(YorN))
    run_context = db_context.runContext
    if not table_exists(inputds1,db_context):
        raise Exception("Table \"" + inputds1 + "\" does not exist")
    if not table_exists(inputds2,db_context):
        raise Exception("Table \"" + inputds2 + "\" does not exist")
    if len(bookmaps) == 0:
        raise Exception("You must provide a list of the bookmaps")
    
    #get unique list of subject values (1 letter abbreviation for subjects)
    subj_vals_list = list(set([bm.subject_values for bm in bookmaps]))
    #get unique list of subjects
    subject_list = list(set([bm.subject for bm in bookmaps]))
    #setup dictionaries of {subject:list_of_queries} for each of our different return table columns
    r_w_query_list = {subj:set() for subj in subject_list}
    w_r_query_list = {subj:set() for subj in subject_list}
    w_w_query_list = {subj:set() for subj in subject_list}
    pass_fail_query_list = {subj:set() for subj in subject_list}
    erasure_query_list = {subj:set() for subj in subject_list}
    raw_query_list = {subj:set() for subj in subject_list}
    xraw_query_list = {subj:set() for subj in subject_list}
    itemcount_query_list = {subj:[] for subj in subject_list}
    
    #plan: 
    #     create a temp table of the fields you need with a subject column also, and their respective columns with subject in
    #     name and then create query at end that queries that table and gets the columns it needs as the correct ones.
    
    for bm in bookmaps:
        #setting up pass_fail column for each subject
        pass_fail = Template(""" WHEN CAST(up${subj}xlev as nvarchar) ='1' AND CAST(form_$sbj as nvarchar) = '${formval}' then 'L'
                    WHEN CAST(up${subj}xlev as nvarchar) ='2' AND CAST(form_$sbj as nvarchar) = '${formval}'  then 'B'
                    WHEN CAST(up${subj}xlev as nvarchar) ='3' AND CAST(form_$sbj as nvarchar) = '${formval}'  then 'P'
                    WHEN CAST(up${subj}xlev as nvarchar) ='4' AND CAST(form_$sbj as nvarchar) = '${formval}'  then 'A'
                    WHEN CAST(up${subj}xlev as nvarchar) ='5' AND CAST(form_$sbj as nvarchar) = '${formval}'  then 'AD'
                    """).substitute(subj = bm.subject_values,sbj=bm.subject_values,formval=bm.form_values)
        pass_fail_query_list[bm.subject].add(pass_fail)
        for item in bm.items:
            #only can do these calculations for operational MC items
            if item.format is None or item.format.upper().strip() != 'MC' or item.role.upper().strip() != 'OPERATIONAL':
                continue
            item_pos = int(item.position)
            str_item_pos = str(item_pos)
            #set up way to count valid items
            item_count = Template(""" (CASE WHEN form_$sbj = '${formval}' THEN 1 ELSE 0 END) 
            """).substitute(sbj=bm.subject_values,formval=bm.form_values)
            itemcount_query_list[bm.subject].append(item_count)
            #define the variables we'll use
            #converting intensities from letters to numbers (i.e. A=10,B=11,etc.)
            raw_intensity = Template(""" CAST(CASE WHEN CAST([up${subject_values}x_raw_intensity_${pos}] as varchar) = 'A' THEN 10
            WHEN CAST([up${subject_values}x_raw_intensity_${pos}] as varchar)  = 'B' THEN 11
            WHEN CAST([up${subject_values}x_raw_intensity_${pos}] as varchar)  = 'C' THEN 12
            WHEN CAST([up${subject_values}x_raw_intensity_${pos}] as varchar)  = 'D' THEN 13
            WHEN CAST([up${subject_values}x_raw_intensity_${pos}] as varchar)  = 'E' THEN 14
            WHEN CAST([up${subject_values}x_raw_intensity_${pos}] as varchar)  = 'F' THEN 15
            WHEN CAST([up${subject_values}x_raw_intensity_${pos}] as varchar)  = '-' THEN 0
            WHEN ISNUMERIC([up${subject_values}x_raw_intensity_${pos}])=1 THEN CAST([up${subject_values}x_raw_intensity_${pos}] as FLOAT)
            ELSE 0 END AS float) """).substitute(subject_values=bm.subject_values,pos=str_item_pos)
            second_inten = Template(""" CAST(CASE WHEN CAST([up${subject_values}x_second_inten_${pos}] as varchar) = 'A' THEN 10
            WHEN CAST([up${subject_values}x_second_inten_${pos}] as varchar) = 'B' THEN 11
            WHEN CAST([up${subject_values}x_second_inten_${pos}] as varchar) = 'C' THEN 12
            WHEN CAST([up${subject_values}x_second_inten_${pos}] as varchar) = 'D' THEN 13
            WHEN CAST([up${subject_values}x_second_inten_${pos}] as varchar) = 'E' THEN 14
            WHEN CAST([up${subject_values}x_second_inten_${pos}] as varchar) = 'F' THEN 15
            WHEN CAST([up${subject_values}x_second_inten_${pos}] as varchar) = '-' THEN 0
            WHEN ISNUMERIC([up${subject_values}x_second_inten_${pos}])=1 THEN CAST([up${subject_values}x_second_inten_${pos}] as FLOAT)
            ELSE 0 END AS float) """).substitute(subject_values=bm.subject_values,pos=str_item_pos)
            raw_editor = " CAST(CASE WHEN ISNUMERIC([up" + bm.subject_values + "x_raw_editor_" + str_item_pos + "])=1 THEN [up" + bm.subject_values + "x_raw_editor_" + str_item_pos + "] ELSE 0 END AS float)"
            second_dark = " CAST(CASE WHEN ISNUMERIC([up" + bm.subject_values + "x_second_dark_" + str_item_pos + "])=1 THEN [up" + bm.subject_values + "x_second_dark_" + str_item_pos + "] ELSE 0 END AS float)"
            raw_item = " CAST(CASE WHEN ISNUMERIC([up" + bm.subject_values + "x_raw_item_" + str_item_pos + "])=1 THEN [up" + bm.subject_values + "x_raw_item_" + str_item_pos + "] ELSE 0 END AS float)"
            #setting up conditionals to be used on everything
            #conditional for if an item type is a valid response (VALID in sas macro)
            valid_conditional = " " + raw_intensity + " >= 5 "
            #conditionals for if an item type is multiple marks (MULTI in sas macro)
            multi_conditional_1 = " " + raw_intensity + " > 10 AND " + second_inten + " > 10 "
            multi_conditional_2 = " " + raw_intensity + " >= 5 AND (" + raw_intensity + " - " + second_inten + ") < 3 " # SAS macro said 2...
            #conditionals for determining if type is ERASE (ERASE in sas macro)
            erase_conditional_1 = valid_conditional + " AND " + second_inten + " >= 3 AND NOT(" + multi_conditional_1 + ") AND NOT(" + multi_conditional_2 + ")"
            erase_conditional_2 = multi_conditional_1 + " AND " + raw_editor + " = 1 "
            erase_conditional_3 = multi_conditional_2 + " AND " + raw_editor + " = 1 "
            #since 3 different erase conditionals we need 3 different case statements for each of these. 
            # These will be compiled to create the sums of r_w,w_w,and w_r.
                #right to wrong erasures
            r_w_query_1 = Template(""" $erase_1 AND $second_dark = $num_key AND $raw_item <> $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_1=erase_conditional_1,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values)
            r_w_query_2 = Template(""" $erase_2 AND $second_dark = $num_key AND $raw_item <> $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_2=erase_conditional_2,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values)
            r_w_query_3 = Template(""" $erase_3 AND $second_dark = $num_key AND $raw_item <> $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_3=erase_conditional_3,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values) 
                #wrong to wrong erasures
            w_w_query_1 = Template(""" $erase_1 AND $second_dark <> $num_key AND $raw_item <> $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_1=erase_conditional_1,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values) 
            w_w_query_2 = Template(""" $erase_2 AND $second_dark <> $num_key AND $raw_item <> $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_2=erase_conditional_2,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values)
            w_w_query_3 = Template(""" $erase_3 AND $second_dark <> $num_key AND $raw_item <> $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_3=erase_conditional_3,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values)
                #wrong to right erasures
            w_r_query_1 = Template(""" $erase_1 AND $second_dark <> $num_key AND $raw_item = $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_1=erase_conditional_1,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values)
            w_r_query_2 = Template(""" $erase_2 AND $second_dark <> $num_key AND $raw_item = $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_2=erase_conditional_2,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values)
            w_r_query_3 = Template(""" $erase_3 AND $second_dark <> $num_key AND $raw_item = $num_key AND form_$sbj = '${formval}' 
            """).substitute(erase_3=erase_conditional_3,second_dark=second_dark,num_key=item.numeric_key,raw_item=raw_item,sbj=bm.subject_values,formval=bm.form_values)
            #setting up raw score query of columns to be added together to create raw score
            raw_query = "cast(CASE WHEN form_" + bm.subject_values + " = '" + bm.form_values + "' AND ISNUMERIC([up" + bm.subject_values + "x_score_item_" + str_item_pos + "])=1 THEN CAST(up" + bm.subject_values + "x_score_item_" + str_item_pos + " AS FLOAT) ELSE 0 END AS float)"
            raw_query_list[bm.subject].add(raw_query)
            #setting up erasure query to tell if you need to add to erasure count
            erasure_query_list[bm.subject].add(erase_conditional_1 + " AND form_" + bm.subject_values + " = '" + bm.form_values + "'")
            erasure_query_list[bm.subject].add(erase_conditional_2 + " AND form_" + bm.subject_values + " = '" + bm.form_values + "'")
            erasure_query_list[bm.subject].add(erase_conditional_3 + " AND form_" + bm.subject_values + " = '" + bm.form_values + "'")
            #now add the r_w,w_w,w_r queries to their respective lists to compile from after the loop.
            r_w_query_list[bm.subject].add(r_w_query_1)
            r_w_query_list[bm.subject].add(r_w_query_2)
            r_w_query_list[bm.subject].add(r_w_query_3)
            w_w_query_list[bm.subject].add(w_w_query_1)
            w_w_query_list[bm.subject].add(w_w_query_2)
            w_w_query_list[bm.subject].add(w_w_query_3)
            w_r_query_list[bm.subject].add(w_r_query_1)
            w_r_query_list[bm.subject].add(w_r_query_2)
            w_r_query_list[bm.subject].add(w_r_query_3)
    #now go through and create the queries from the lists and run them to create our temp table
    #with columns for each subject and their counts
    #hardcoded to A1.Lithocode and A1.SSID because it must be in two tables and we dont want ambiguous column names
    select_query_list = ["[dcrxid_attend]","[dcrxnm_attend]","[bcrxid_attend]","[bcrxnm_attend]",
                         "Lithocode","StudentID","SSID","Grade"]
    #compile queries for each subject
    for subj in subject_list: 
        query_pass_fail = ' CASE '
        #pass_fail columns first
        for line in pass_fail_query_list[subj]:
            query_pass_fail += line
        query_pass_fail += " ELSE '' END AS " + subj + "_Pass_Fail "
        #now raw scores -- just summing all the variables in the list
        query_raw_scores = ' (' + "+".join(raw_query_list[subj]) + ") AS " + subj + "_Raw "
        #now erasure
        query_erasure = " ("
        for line in erasure_query_list[subj]:
            query_erasure += " (CASE WHEN " + line + " THEN 1 ELSE 0 END)+" 
        #remove trailing +
        query_erasure = query_erasure[:-1] + ") AS " + subj + "_Erasure "
        #now valid item counts for use with where clause in final query getting percent of w_r
        query_item_count = " (" + "+".join(itemcount_query_list[subj]) + ") as " + subj + "_itemcount "
        #now right to wrong erasure
        query_r_w = " ("
        for line in r_w_query_list[subj]:
            query_r_w += " (CASE WHEN " + line + " THEN 1 ELSE 0 END)+" 
        #remove trailing +
        query_r_w = query_r_w[:-1] + ") AS " + subj + "_r_w "
        #now wrong to wrong erasure
        query_w_w = " ("
        for line in w_w_query_list[subj]:
            query_w_w += " (CASE WHEN " + line + " THEN 1 ELSE 0 END)+" 
        #remove trailing +
        query_w_w = query_w_w[:-1] + ") AS " + subj + "_w_w "
        #now wrong to right erasure
        query_w_r = " ("
        for line in w_r_query_list[subj]:
            query_w_r += " (CASE WHEN " + line + " THEN 1 ELSE 0 END)+" 
        #remove trailing +
        query_w_r = query_w_r[:-1] + ") AS " + subj + "_w_r "
        #now add them to master select query list
        select_query_list.append(query_erasure)
        select_query_list.append(query_pass_fail)
        select_query_list.append(query_r_w)
        select_query_list.append(query_raw_scores)
        select_query_list.append(query_w_r)
        select_query_list.append(query_w_w)
        select_query_list.append(query_item_count)
        select_query_list.append(" up" + subj[:1] + "xraw " + " AS " + subj + "_xraw")# pulling xraw values for each subject -- will be checked in final where clause
    #now creating where clause
    where_clause = " WHERE schtype_attend <> 'H'"
    #now that we have the select query made we must make the final query to run to create our temp table
    # which will hold all of the raw scores and counts etc. for us to check for erasure analysis.
    #we need to join inputds1 to inputds2 on lithocode
    table1_spec = get_table_spec(inputds1, db_context)
    table1_spec.populate_from_connection()
    table1_spec.alias = "A1"
    table2_spec = get_table_spec(inputds2, db_context)
    table2_spec.populate_from_connection()
    table2_spec.alias = "A2"
    tab1cols = [ "A1." + x.field_name for x in table1_spec ]
    tab2cols = [ x.field_name for x in table2_spec if "A1." + x.field_name not in tab1cols ]
    
    select_query = """select {cols}
        INTO #erasure_tmp
        FROM (
                Select {innercols} 
                from {ds1} A1
                JOIN {ds2} A2
                    ON A1.Lithocode = A2.Lithocode
                {where}
             ) A
    """.format( cols=",".join(list(set(select_query_list))), innercols=",".join(tab1cols) + "," + ",".join(tab2cols), 
                ds1=inputds1, ds2=inputds2, where=where_clause )
    #select_query = "SELECT " + ",".join(list(set(select_query_list))) + " INTO #erasure_tmp FROM " + inputds1 + " A1 JOIN " + inputds2 + " A2 ON A1.Lithocode = A2.Lithocode " + where_clause
    #################
    #for debugging
    #################
    f = open("""C:\query.txt""","w")
    f.write(select_query)
    f.close()
    #################
    #end debug
    #################
    if overwrite.upper() == 'Y':
        #if supposed to overwrite we drop the table before triyng to create a new one
        drop_table_if_exists('#erasure_tmp',db_context)
    #cleanup
    del table1_spec
    del table2_spec
    del tab1cols
    del tab2cols
    db_context.executeNoResults(select_query)
    #free up lots of memory
    del select_query
    
    #now we need to write the final query to get the data we need from our temp table into the final output table
    final_select = """ Select Grade,
                       bcrxid_attend,
                       dcrxid_attend,
                       bcrxnm_attend,
                       dcrxnm_attend,
                       Lithocode,
                       SSID,
                       StudentID"""
    #now add final select statements
    final_subject_queries = []
    #go through each subject and create a separate select statement for each, and then union them all at the end
    #each should have their own where conditions too
    for subj in subject_list:
        #cutpoint is 9 and percent is .3 for all subjects except writing
        point = 9
        percent = 0.3
        #so if it's writing set cutpoint to 4 and percent to .5
        if 'W' in subj.upper():
            point = 4 
            percent = 0.5
        query_final = Template("""
        ,${subj}_pass_fail as pass_fail
        ,'${subj}' as subject
        ,${subj}_raw as raw
        ,${subj}_erasure as erased
        ,${subj}_w_r as w_r
        ,${subj}_w_w as w_w
        ,${subj}_r_w as r_w
        FROM #erasure_tmp
        WHERE ${subj}_erasure > $point AND ${subj}_w_r >= ${subj}_itemcount * $percent AND ${subj}_xraw NOT IN ('A','I')
        """).substitute(locals())
        final_subject_queries.append(final_select + query_final)
    #now go through the final queries and union them all and insert into the final table
    union = ''
    query_cnt = 0
    for query in final_subject_queries:
        if query_cnt == 0: #dont want an extra union in the beginning...
            union = query
        else: #union starting at second query so it is in correct format
            union += Template("""
            UNION
            $query
            """).substitute(locals())
        query_cnt += 1
    final_query = Template("""
    Select *
    into $outputds 
    from (
    $union
    ) A
    """).substitute(locals())
    #################
    #for debugging
    #################
#    f = open("""C:\queryFINAL.txt""","w")
#    f.write(final_query)
#    f.close()
    #################
    #end debug
    #################
    #add in overwrite here
    if overwrite.upper() == 'Y':
        #if supposed to overwrite we drop the table before triyng to create a new one
        drop_table_if_exists(outputds,db_context)
    db_context.executeNoResults(final_query)

#if __name__ == '__main__':
#    maps = BookMapReader(excel='Y',inputfile='''G:\SAS\Erasure\InputData\Bookmaplocations1.xls''',inputsheet='BookMap')
#    erasure(RunContext("erasuredb"), inputds1="AIR1",inputds2="AIR2", bookmaps=maps,outputds='erasure_out')
#    
    