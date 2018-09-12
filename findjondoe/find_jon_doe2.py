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
from string import Template

def big_nominal_counts(cursor, table, varname):
    """ big_nominal_counts(cursor, table, string) -> number
        Get's counts of distinct values of varname in the big data table
    """
    cursor.execute(Template("""
        select 
            count(distinct $varname) as cnt
        from 
            $table
        where 
            $varname is not null
    """).substitute(locals()))
    row = cursor.fetchone()
    return row.cnt

def nominal_counts(cursor, table, varname, weight, N):
    """ nominal_counts(cursor, table, string, number, number) -> void
        Compute frequencies for each value of varname in the big data. 
        scaled by its weighting factor and save the result 
        in temporary table for later merge
    """
    cursor.execute(Template("""
        if exists(
            select 
                * 
            from 
                sys.tables 
            where 
                name = 'tmp_$varname'
        )
        begin
            drop table tmp_$varname
        end
        """).substitute(locals()))
    cursor.commit()
    
    cursor.execute(Template("""
        select 
            $varname as val, 
            POWER(cast(count(*) as float)/$N, $weight) as freq
        into 
            tmp_$varname
        from 
            $table
        where 
            $varname is not null
        group by 
            $varname
        """).substitute(locals()))
    cursor.commit()

def name_calibration(cursor, table, firstname, lastname, sortfield, nameweight):
    """ name_calibration(cursor, table, string, string, string, number) -> number
        Compute the frequencies with which names are similar
    """
    cursor.execute(Template("""
        select
            round(20*dbo.NameSimilarity(H1.$firstname, H1.$lastname, H2.$firstname, H2.$lastname),0) as sim, 
            count(*) as cnt
        from      
            (select 
                top 20 $firstname, $lastname
            from 
                $table
            where 
                $sortfield is not null
            order by 
                $sortfield
            ) as H1,
            $table as H2
        group by
            round(20*dbo.NameSimilarity(H1.$firstname, H1.$lastname, H2.$firstname, H2.$lastname),0)
       """).substitute(locals()))

    similarityprobs = {}
    total = 0
    for row in cursor:
        similarityprobs[row.sim] = row.cnt
        total = total + row.cnt
    
    runsum = 0    
    for sim in range(20,-1,-1):
        if sim in similarityprobs:
            runsum = runsum + similarityprobs[sim]
        similarityprobs[sim] = pow(float(runsum)/float(total), nameweight) 
    return  similarityprobs           

def table_contents(cursor, table):
    """ table_contents(cursor, table) -> dictionary
        Get information for specified table
    """
    cursor.execute(Template("""
        select 
            C.name, 
            C.user_type_id, 
            Y.name as vartype, 
            C.max_length, 
            C.[precision]
        from
            sys.columns C
            join sys.tables T on C.object_id = T.object_id
            join sys.types Y on C.user_type_id = Y.user_type_id
        where
            T.name = '$table'
        """).substitute(locals()))
    temp_dict = {}
    for row in cursor:
        info = {}
        info["variablename"] = row.name
        info["variabletype"] = row.vartype
        info["length"] = row.max_length
        info["precision"] = row.precision
        temp_dict[row.name.lower()] = info
    return temp_dict

def list_to_sql_string(varlist, template, sep):
    """ list_to_sql_string(varlist, template, sep) -> string
        Turns a list of variables into a string using sep as a seperator and
        replacing each $var in the template string with a variable from the list
    """
    outstring = ''
    for var in varlist:
        if len(outstring) > 0:
            outstring += sep
        outstring += Template(template).substitute(var = var)
    return outstring

def validate_variables(allvars, table1vars, needles_table, table2vars, haystack_table):
    """ validate_variables(seq string, dict, string, dict, string) -> boolean
        Validates variables given for their type, length, precision match
        Returns true if the variables are ok.
    """
    error = False
    for var in allvars:
        tmp_error = True
        var_lower_case = var.lower()
        if not var_lower_case in table1vars:
            print var + " is not present in table " + needles_table
        elif not var_lower_case in table2vars:
            print var + " is not present in table " + haystack_table
        elif table1vars[var_lower_case]["variabletype"] != \
            table2vars[var_lower_case]["variabletype"]:
            print var + \
                " does not have the same type in " + \
          needles_table + " and " + haystack_table + " (" + \
          table1vars[var_lower_case]["variabletype"] + " vs. " + \
          table2vars[var_lower_case]["variabletype"] + ")"
        elif table1vars[var_lower_case]["length"] != \
            table2vars[var_lower_case]["length"]:
            print var + " does not have the same length in " + \
          needles_table + " and " + haystack_table + " (" + \
          table1vars[var_lower_case]["length"] + " vs. " + \
          table2vars[var_lower_case]["length"] + ")"
        elif table1vars[var_lower_case]["precision"] != \
            table2vars[var_lower_case]["precision"]:
            print var + " does not have the same precision in " \
        + needles_table + " and " + haystack_table + " (" + \
        table1vars[var_lower_case]["precision"] + " vs. " + \
        table2vars[var_lower_case]["precision"] + ")"
        else:
            tmp_error = False
        if tmp_error:
            error = True
    return error 

def find_jon_doe(db_connection, needles_table, haystack_table, \
        table_variables_dict, \
        name, name_weight,\
        out_table, outvars,\
        top20_key_variable, \
        cut,\
        count) :
    """ find_jon_doe(connection, table, table, dict,
                    seq string, number,
                    table, seq string, string, number, count) ->             
        Find Jon Doe using the given parameters
    """
    nominal_variables_weights_dict = table_variables_dict['nominal'] ;
    bignominal_variables_weights_dict = table_variables_dict['bignominal'] ;
    nominal_variables = nominal_variables_weights_dict.keys() ;
    bignominal_variables = bignominal_variables_weights_dict.keys() ;
    allvars = list(set(nominal_variables + bignominal_variables + name + outvars))
    
    cursor = db_connection.cursor()
    table1vars = table_contents(cursor, needles_table)
    table2vars = table_contents(cursor, haystack_table)
    
    ## Validate the variables for match in the tables    
    if  validate_variables(allvars, table1vars, needles_table, table2vars, haystack_table) :
        return 1;
    
    ## Setup the nominal variable frequency tables 
    for nominal_variable in nominal_variables:
        cursor.execute(Template("""
            select 
                count(*) as cnt
            from 
                $haystack_table
            where 
                $nominal_variable is not null
        """).substitute(locals()))
        row = cursor.fetchone()
        N = row.cnt
        nominal_counts(cursor, haystack_table, nominal_variable, nominal_variables_weights_dict[nominal_variable], N)

    ## Setup the big nominal variable frequencies 
    big_frequencies = {}
    for bignominal_variable in bignominal_variables:
        big_frequencies[bignominal_variable] = \
            pow(1.0/big_nominal_counts(cursor, haystack_table, bignominal_variable), \
                    bignominal_variables_weights_dict[bignominal_variable])
    
    ## Calibrate and get the similarity probabilities
    name_sims = name_calibration(cursor, haystack_table, name[0], name[1], top20_key_variable, name_weight)

    ## Create from clauses for needles and haystack tables 
    vars_from_needles_string = list_to_sql_string(list(\
       set(bignominal_variables + nominal_variables + name)), \
       'B.$var as N_$var', ', ')
    vars_from_haystack_string = list_to_sql_string(list(\
       set(bignominal_variables + nominal_variables + name + outvars)), \
       'A.$var as H_$var', ', ')
    
    ## Delete the previously created tmp needles table
    cursor.execute(Template("""
        if exists(select * from sys.tables where name = 'tmp_$needles_table')
        begin
            drop table tmp_$needles_table
        end
        """).substitute(locals()))
    cursor.commit()

    select_sql = ''
    join_sql = ''
    prob_sql = ''
    cnt = 0        
    for nominal_variable in nominal_variables:
        cnt += 1
        select_sql += ", isnull(P" + str(cnt) + ".freq,1) as freq" + str(cnt)
        if len(prob_sql) > 0:
            prob_sql += "*"
        prob_sql += Template("""
        case when
            A.$nominal_variable = B.$nominal_variable then freq$cnt
            else 1
        end
        """).substitute(locals())
        join_sql += Template("""
            left join tmp_$nominal_variable as P$cnt on B.$nominal_variable = P$cnt.val
        """).substitute(locals())
    cursor.execute(Template("""
        select 
            B.* $select_sql
        into 
            tmp_$needles_table
        from
            $needles_table as B
            $join_sql
        """).substitute(locals()))
    cursor.commit()
    
    for bignominal_variable in bignominal_variables:
        if len(prob_sql) > 0:
            prob_sql += "*"
        prob = big_frequencies[bignominal_variable]
        prob_sql += Template("""
        case
            when  A.$bignominal_variable = B.$bignominal_variable then 
                $prob
            else 1
        end
        """).substitute(locals())
    if len(prob_sql) > 0:
        prob_sql += """
        *
        """
    prob_sql += Template("case round(20*dbo.NameSimilarity(A.$firstname, A.$lastname, B.$firstname, B.$lastname),0)\r\n").substitute(firstname = name[0], lastname = name[1])
    for sim in range(0,21):
        prob_sql += "when " + str(sim) + " then " + str(name_sims[sim]) + '\r\n'
    prob_sql += 'end\r\n'

    ## Delete the previously created tmp output table
    cursor.execute(Template("""
        if exists(select * from sys.tables where name = 'tmp_$out_table')
        begin
            drop table tmp_$out_table
        end
        """).substitute(locals()))
    cursor.commit()
    
    query = Template("""
        select
            $vars_from_needles_string, $vars_from_haystack_string, $prob_sql as prob
        into 
            tmp_$out_table
        from
            $haystack_table as A,
            tmp_$needles_table as B
        where
            $prob_sql < $cut
        """).substitute(locals())
    print query
    cursor.execute(query)
    cursor.commit()

    ## Delete the previously created out table
    cursor.execute(Template("""
        if exists(select * from sys.tables where name = '$out_table')
        begin
            drop table $out_table
        end
        """).substitute(locals()))
    cursor.commit()
    
    # report
    report = list_to_sql_string(bignominal_variables + nominal_variables, "case when N_$var = H_$var then '$var matches, ' else '' end", ' + ')
    report += Template(" + N_$fnm + ' '  + N_$lnm + ' vs. ' + H_$fnm + ' ' + H_$lnm as report").substitute(fnm = name[0], lnm = name[1])
    out_string = list_to_sql_string(outvars, 'H_$var as $var', ', ')
    partition_string = list_to_sql_string(bignominal_variables + nominal_variables + name, 'N_$var', ', ')
    join_on = list_to_sql_string(outvars, 'A.H_$var = B.$var', ' and ')
    query = Template("""
        select 
            B.*
        into 
            $out_table
        from
            (select 
                $report, 
                $out_string, 
                prob, 
                rank() over (partition by $partition_string order by prob) as rank
            from 
                tmp_$out_table
            ) as B
            join tmp_$out_table as A on $join_on
        where
            rank <= $count
        """).substitute(locals())
    print query
    cursor.execute(query)
    cursor.commit()
    return 1
