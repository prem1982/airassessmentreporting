#
# TODO - coding!!
# TODO - testing
# TODO - documentation
#

import os
from airassessmentreporting.airutility import SafeExcelReader, dbutilities

# Create empty table for counts.
CREATE_COUNTS_TABLE_SQL = '''
    SELECT {field_names}
        INTO {schema}.{temp_table_name}
        FROM {schema}.{input_table_name} AS itn1
        WHERE 1=0

    ALTER TABLE {schema}.{temp_table_name}
        ADD {array_fields}
    '''

SELECT_FOR_COUNTS_SQL = '''
    SELECT dcrxid
        FROM {schema}.{input_table_name} AS itn1
        WHERE LOWER(LTRIM(RTRIM(schtype))) IN ( {schtype} )
        ORDER BY {agg_var}, dcrxid
    '''

INSERT_INTO_COUNTS_SQL = '''
    INSERT INTO {schema}.{table_name}
        ( {column_names} )
        values ( {column_values} )
    '''


def district_performance_summary_report( run_context, specfile, input_table_name, county_name='dcxx_county' ):
    if not run_context:
        raise ValueError( 'run_context is null or empty.' )
    if not specfile:
        raise ValueError( 'specfile is null or empty.' )
    if not input_table_name:
        raise ValueError( 'input_table_name is null or empty.' )
    if not county_name:
        raise ValueError( 'county_name is null or empty.' )

    if not os.path.exists( specfile ):
        msg = 'specfile[{}] not found.'.format( specfile )
        raise IOError( msg )

    initial_rows = excel_to_vars( run_context, specfile, 'initial' )
    initial_variables = { }
    for row in initial_rows:
        initial_variables[ row[ 0 ] ] = row[ 1 ]
    dataset = initial_variables[ 'dataset' ]
    agg_var = initial_variables[ 'agg_var' ]

    subject_variables = excel_to_vars( run_context, specfile, 'subjects' )
    level_variables = excel_to_vars( run_context, specfile, 'levels' )

    report_variables = excel_to_vars( run_context, specfile, 'report' )
    schtype = report_variables[ 0 ][ 1 ].split( ' ' )

    db_context = run_context.getDBContext( instance_name='default', cached=True )

    if not dbutilities.table_exists( table=dataset, db_context=db_context ):
        msg = 'dataset[{}] not found.'.format( dataset )
        raise IOError( msg )

    temp_tables = init_temp_tables( db_context )
    field_names = [ 'dcrxid', 'schtype', agg_var ]
    create_counts_table( db_context=db_context, input_table_name=input_table_name, output_table_name=temp_tables[ 'counts' ],
        field_names=field_names, county_name=county_name, number_of_subjects=len( subject_variables ),
        number_of_levels=len( level_variables ) )

    count_frequency_and_percentage( db_context=db_context, input_table_name=input_table_name,
        output_table_name=temp_tables[ 'counts' ], schtype=schtype, agg_var=agg_var,
        number_of_subjects=len( subject_variables ),
        number_of_levels=len( level_variables ) )


def count_frequency_and_percentage( db_context, input_table_name, output_table_name, schtype, agg_var, number_of_subjects,
        number_of_levels ):
    if not db_context:
        raise ValueError( 'db_context is null or empty.' )
    if not input_table_name:
        raise ValueError( 'input_table_name is null or empty.' )
    if not output_table_name:
        raise ValueError( 'output_table_name is null or empty.' )
    if not schtype:
        raise ValueError( 'schtype is null or empty.' )
    if not agg_var:
        raise ValueError( 'agg_var is null or empty.' )
    if not number_of_subjects:
        raise ValueError( 'number_of_subjects is null or 0.' )
    if not number_of_levels:
        raise ValueError( 'number_of_levels is null or 0.' )

    schtype_str = ''
    for type in schtype:
        schtype_str += "'" + type + "', "
    schtype_str = schtype_str.rstrip( ', ' )

    count = [ ]
    perc = [ ]
    for subject_num in range( number_of_subjects ):
        count[ subject_num ] = [ ]
        perc[ subject_num ] = [ ]
        for level_num in range( number_of_levels + 1 ):
            count[ subject_num ][ level_num ] = 0
        for level_num in range( number_of_levels ):
            perc[ subject_num ][ level_num ] = 0

    sql = SELECT_FOR_COUNTS_SQL.format( schema=db_context.schema, input_table_name=input_table_name, schtype=schtype_str,
        agg_var=agg_var )
    # is_first_dcrxid = True
    for row in db_context.executeBuffered( sql ):
        print( "DEBUG: row[%s]" % row)
        # if is_first_dcrxid:
        #     is_first_dcrxid = False
        # TODO - tally and insert counts here...
        for subject_num in range( number_of_subjects ):
            # if flag[subject_num] == 1:
            #     count[subject_num][1] += 1
            #     for level_num in range( number_of_levels ):
            #         # TODO - if index() > 0 then count[subject_num][level_num] += 1
            pass



def create_counts_table( db_context, input_table_name, output_table_name, field_names, county_name,
        number_of_subjects, number_of_levels ):
    if not db_context:
        raise ValueError( 'db_context is null or empty.' )
    if not input_table_name:
        raise ValueError( 'input_table_name is null or empty.' )
    if not output_table_name:
        raise ValueError( 'output_table_name is null or empty.' )
    if not field_names:
        raise ValueError( 'field_names is null or empty.' )
    if not county_name:
        raise ValueError( 'county_name is null or empty.' )
    if not number_of_subjects:
        raise ValueError( 'number_of_subjects is null or 0.' )
    if not number_of_levels:
        raise ValueError( 'number_of_levels is null or 0.' )

    field_names_str = ''
    for field in field_names:
        field_names_str += field + ', '
    field_names_str += 'dcrxnm, ' + county_name

    array_fields = ''
    for subject_num in range( number_of_subjects ):
        for level_num in range( number_of_levels + 1 ):
            array_fields += '\n' + ' ' * 12 + 'zcount_%d_%d  INTEGER  NULL, ' % (subject_num, level_num)
    for subject_num in range( number_of_subjects ):
        for level_num in range( number_of_levels ):
            array_fields += '\n' + ' ' * 12 + 'zperc_%d_%d  INTEGER  NULL, ' % (subject_num, level_num)
    for level_num in range( number_of_levels ):
        array_fields += '\n' + ' ' * 12 + 'zlevel_%d  INTEGER  NULL, ' % level_num
    for subject_num in range( number_of_subjects ):
        array_fields += '\n' + ' ' * 12 + 'zsubject_%d  INTEGER  NULL, ' % subject_num
    for subject_num in range( number_of_subjects ):
        array_fields += '\n' + ' ' * 12 + 'zflag_%d  INTEGER  NULL, ' % subject_num
    array_fields = array_fields.rstrip( ', \n' )

    sql = CREATE_COUNTS_TABLE_SQL.format( field_names=field_names_str, schema=db_context.schema,
        temp_table_name=output_table_name, input_table_name=input_table_name, array_fields=array_fields )
    # print('DEBUG - sql[%s]' % sql)
    db_context.executeNoResults( query=sql, commit=True )


def init_temp_tables( db_context ):
    temp_tables = { }
    if not db_context:
        raise ValueError( 'db_context is null or empty.' )
    temp_tables[ 'counts' ] = dbutilities.get_temp_table( db_context ).table_name
    temp_tables[ 'result' ] = dbutilities.get_temp_table( db_context ).table_name
    temp_tables[ 'resultc' ] = dbutilities.get_temp_table( db_context ).table_name
    temp_tables[ 'total' ] = dbutilities.get_temp_table( db_context ).table_name
    temp_tables[ 'totalc' ] = dbutilities.get_temp_table( db_context ).table_name
    temp_tables[ 'allfive' ] = dbutilities.get_temp_table( db_context ).table_name
    return temp_tables


def excel_to_vars( run_context, filename, sheetname ):
    rows = [ ]
    if not run_context:
        raise ValueError( 'run_context is null or empty.' )
    if not filename:
        raise ValueError( 'filename is null or empty.' )
    if not sheetname:
        raise ValueError( 'sheetname is null or empty.' )
    reader = SafeExcelReader( run_context=run_context, filename=filename, sheet_name=sheetname )
    rows = [ row for row in reader.getRows( ) ]  # copy the row values
    return rows


def sql1( ):
    query = '''
        insert into result
            (&agg_var,dcrxid, dcrxnm, &countyname,_count1,%do k=1 %to &lev_num_of.; _perc&k., %end; subject,suborder,
            gcorder)
            select &agg_var, dcrxid,dcrxnm, &countyname, _count&first_c.
                %do k=1 %to &Lev_num_of.;
                    , _perc%eval(&k.+(&i-1)*&lev_num_of.)
                %end;
                ,subject,suborder,gcorder
            from new
            where strip(schtype) ne "C";

        insert into resultc
            (&agg_var,dcrxid,dcrxnm, &countyname,_count1,%do k=1 %to &lev_num_of.; _perc&k., %end; subject,suborder,gcorder)
            select &agg_var, dcrxid,dcrxnm, &countyname, _count&first_c.
                %do k=1 %to &Lev_num_of.;
                    , _perc%eval(&k.+(&i-1)*&lev_num_of.)
                %end;
                ,subject,suborder,gcorder
            from new
            where strip(schtype) eq "C";

        insert into total
            (&agg_var.,_count1,%do k =1 %to &lev_num_of.; _perc&k.,%end; subject,suborder,gcorder,dcrxnm)
            select distinct &agg_var.,sum(_count&first_c.) as _count1
                %do k=1 %to &lev_num_of.;
                    , round(sum(_count%eval(&k.+&first_c.))*100/sum(_count&first_c.),&Rep_round_1.) as _perc&k.
                %end;
                ,subject,suborder,gcorder,"All Community Schools" as dcrxnm
                from new
                where strip(schtype) eq 'C'
                group by &agg_var.;

        insert into total
            (&agg_var.,_count1,%do k =1 %to &lev_num_of.; _perc&k.,%end; subject,suborder,gcorder,dcrxnm)
            select distinct &agg_var.,sum(_count&first_c.) as _count1
                %do k=1 %to &lev_num_of.;
                    , round(sum(_count%eval(&k.+&first_c.))*100/sum(_count&first_c.),&Rep_round_1.) as _perc&k.
                %end;
                ,subject,suborder,gcorder,"All &Rep_Label_1. Schools" as dcrxnm
                from new
                group by &agg_var.;

        insert into totalc
            (&agg_var.,_count1,%do k =1 %to &lev_num_of.; _perc&k.,%end;subject,suborder,gcorder,dcrxnm)
            select distinct &agg_var.,sum(_count&first_c.) as _count1
                %do k=1 %to &lev_num_of.;
                    , round(sum(_count%eval(&k.+&first_c.))*100/sum(_count&first_c.),&Rep_round_1.) as _perc&k.
                %end;
                ,subject,suborder,gcorder,"All Community Schools" as dcrxnm
            from new
            where strip(schtype) eq 'C'
            group by &agg_var.;
        '''
    return query


def sql2( ):
    query = '''
        insert into result
            (&agg_var,dcrxid,dcrxnm, &countyname,_count1,%do k=1 %to &lev_num_of.; _perc&k., %end; subject,suborder,gcorder)
            select &agg_var, dcrxid, dcrxnm, &countyname,_count&first_c.
                %do k=1 %to &Lev_num_of.;
                    , _perc%eval(&k.+(&i-1)*&lev_num_of.)
                %end;
                ,subject,suborder,gcorder
            from new
            where strip(schtype) ne "C";

        insert into resultc
            (&agg_var,dcrxid,dcrxnm, &countyname,_count1,%do k=1 %to &lev_num_of.; _perc&k., %end; subject,suborder,gcorder)
            select &agg_var, dcrxid,dcrxnm, &countyname, _count&first_c.
                %do k=1 %to &Lev_num_of.;
                    , _perc%eval(&k.+(&i-1)*&lev_num_of.)
                %end;
                ,subject,suborder,gcorder
            from new
            where strip(schtype) eq "C";

        insert into total
            (&agg_var.,_count1,%do k =1 %to &lev_num_of.; _perc&k.,%end;subject,suborder,gcorder,dcrxnm)
            select distinct &agg_var.,sum(_count&first_c.) as _count1
                %do k=1 %to &lev_num_of.;
                    , round(sum(_count%eval(&k.+&first_c.))*100/sum(_count&first_c.),&Rep_round_1.) as _perc&k.
                %end;
                ,subject,suborder,gcorder,"All Community Schools" as dcrxnm
                from new
                where strip(schtype) eq 'C'
                group by &agg_var.;

        insert into total
            (&agg_var.,_count1,%do k =1 %to &lev_num_of.; _perc&k.,%end;subject,suborder,gcorder,dcrxnm)
            select distinct &agg_var.,sum(_count&first_c.) as _count1
                %do k=1 %to &lev_num_of.;
                    , round(sum(_count%eval(&k.+&first_c.))*100/sum(_count&first_c.),&Rep_round_1.) as _perc&k.
                %end;
                ,subject,suborder,gcorder,"All &Rep_Label_1. Schools" as dcrxnm
                from new
                group by &agg_var.;

        insert into totalc
            (&agg_var.,_count1,%do k =1 %to &lev_num_of.; _perc&k.,%end;subject,suborder,gcorder,dcrxnm)
            select distinct &agg_var.,sum(_count&first_c.) as _count1
                %do k=1 %to &lev_num_of.;
                    , round(sum(_count%eval(&k.+&first_c.))*100/sum(_count&first_c.),&Rep_round_1.) as _perc&k.
                %end;
                ,subject,suborder,gcorder,"All Community Schools" as dcrxnm
                from new
                where strip(schtype) eq 'C'
                group by &agg_var.;
        '''
    return query


def sql3( ):
    query = '''
        create view allfive1 as
            select distinct(dcrxid),&agg_var.,dcrxnm, &countyname, count(dcrxid) as tot
            from temp
            where &agg_var. in (&sub_grades_1.)
                %do i=1 %to &sub_num_of;
                    and &&sub_flag_&i eq 1
                %end;
            group by dcrxid,&agg_var.;

        create view allfive2 as
            select distinct(dcrxid),&agg_var., count(dcrxid) as subtot
                from temp
                where &agg_var.  in (&sub_grades_1.)
                    %do i=1 %to &sub_num_of;
                        and &&sub_flag_&i eq 1
                    %end;
                    %do i=1 %to &sub_num_of;
                        and &&sub_variable_&i in (&&lev_value_&lev_num_of.)
                    %end;
            group by dcrxid, &agg_var.;
        '''
    query += '''
        create table allfive as
            select a.*,subtot
            from allfive1 as a left join allfive2 as b
            on a.dcrxid eq b.dcrxid and a.&agg_var. eq b.&agg_var. ;

        delete * from allfive where tot<1;

        update allfive set subtot=0 where subtot<0;

        insert into resultc
            (dcrxid,&agg_var.,dcrxnm, &countyname,_count1,_perc&lev_num_of.,subject,suborder,gcorder)
            select dcrxid,&agg_var.,dcrxnm, &countyname,
                tot,round(subtot*100/tot,&Rep_round_1.),
                "All Five" as subject,
                "99" as suborder,
                PUT((20+&agg_var.),Z3.)
            from allfive
            where dcrxid in (
                select distinct dcrxid from temp where strip(schtype) eq 'C');

        insert into result
            (dcrxid,&agg_var.,dcrxnm, &countyname,_count1,_perc&lev_num_of.,subject,suborder,gcorder)
            select dcrxid,&agg_var.,dcrxnm, &countyname,
                tot,round(subtot*100/tot,&Rep_round_1.),
                "All Five" as subject,
                "99" as suborder,
                PUT((20+&agg_var.),Z3.)
            from allfive
            where  dcrxid not in (
                select distinct dcrxid from temp where strip(schtype) eq 'C');

        create view sumtot as
            select distinct &agg_var.,sum(tot) as tot, sum(subtot) as subtot
            from allfive
            group by &agg_var.;

        create view sumtotc as
            select distinct &agg_var., sum(tot) as tot, sum(subtot) as subtot
            from allfive
            where dcrxid in (
                select distinct dcrxid from temp where strip(schtype) eq 'C')
            group by &agg_var.;

        insert into total
            (dcrxnm,&agg_var.,_count1,_perc&lev_num_of.,subject,suborder,gcorder)
            select "All &Rep_Label_1. Schools",&agg_var.,tot,
                    round(subtot*100/tot,&Rep_round_1.),
                    "All Five" ,
                    "99" ,
                PUT((20+&agg_var.),Z3.)
            from sumtot;

        insert into total
            (dcrxnm,grade,_count1,_perc&lev_num_of.,subject,suborder,gcorder)
            select "All Community Schools" ,&agg_var.,tot,
                    round(subtot*100/tot,&Rep_round_1.),
                    "All Five" ,
                    "99" ,
                PUT((20+&agg_var.),Z3.)
            from sumtotc;

        insert into totalc
            (dcrxnm,grade,_count1,_perc&lev_num_of.,subject,suborder,gcorder)
            select "All Community Schools" ,&agg_var.,tot,
                    round(subtot*100/tot,&Rep_round_1.),
                    "All Five" ,
                    "99" ,
                PUT((20+&agg_var.),Z3.)
            from sumtotc;
        '''
    return query

