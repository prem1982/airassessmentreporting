'''
Created on May 23, 2013

@author: temp_dmenes
'''

import unittest
import os
import re

from airassessmentreporting.airutility import ( SafeExcelReader,
        table_exists, Joiner, get_temp_table )
from airassessmentreporting.testutility import ( SuiteContext, compare_tables,
        integer_compare, mixed_compare )
from airassessmentreporting.ttestlevel import TTestLevel

_AGG_FILE="HI Spring 2008 Aggregations_Melissa.xls"
_GRADE_FILES=(
    ( 3, 'G03.xlsx', 'g03', '_G03' ),
    ( 4, 'G04.xlsx', 'g04', '_G04' ),
    ( 5, 'G05.xlsx', 'g05', '_G05' ),
    ( 6, 'G06.xlsx', 'g06', '_G06' ),
    ( 7, 'G07.xlsx', 'g07', '_G07' ),
    ( 8, 'G08.xlsx', 'g08', '_G08' ),
)

_WHERE_KILLER = re.compile( r"^\s*WHERE\s+", re.IGNORECASE )
_MISSING_KILLER_0 = re.compile( r"MISSING\s*\(([^)]*)\)\s*=\s*0\s*", re.IGNORECASE )
_MISSING_KILLER_1 = re.compile( r"MISSING\s*\(([^)]*)\)\s*=\s*1\s*", re.IGNORECASE )

class TestTTestSasCompare( unittest.TestCase ):
    def setUp(self):
        
        self.run_context = SuiteContext( "unittest" )
        self.db_context = self.run_context.getDBContext( "unittest" )
        self.static_context = self.run_context.getDBContext( "static" )

        self.answer_dir = os.path.join( self.run_context.logs_dir, 'ttest_level_tests' )
        if not os.path.exists( self.answer_dir ):
            os.makedirs( self.answer_dir )
        
        self.data_dir = os.path.join( self.run_context.tests_safe_dir,
                                      'ttest', 'input_data' )
        
        self.specimen_dir = os.path.join( self.run_context.tests_safe_dir,
                                      'ttest', 'sas_outputs' )

        # libname ttest "H:\share\CSSC Folder\Score Report Group\Test Data\lib_TTestLevel";
        # %let agg_file = &cvsroot.\ScoreReportMacros\UnitTested\lib_TTestLevel\test\HI Spring 2008 Aggregations_Melissa.xls;
        # %let sheet=ttestlevel;
        # %SafeExcelRead(filename=&agg_file., sheetname =&sheet., DS_out =aggds);
        self.run_context.debug( "Reading data for ttest_level tests" )
        agg_file = os.path.join( self.data_dir, _AGG_FILE)
        reader = SafeExcelReader( self.run_context,
                agg_file, "ttestlevel", scan_all = True )
        self.agg_ds = [ row for row in reader.getRows() ]
        
        # Import the input datasets
        reader.db_context = self.static_context
        for ( grade, filename, table_name, sheet_name ) in _GRADE_FILES:
            if not table_exists( table_name, self.static_context ):
                self.run_context.debug( "Reading data for grade {}".format( grade ) )
                reader.filename = os.path.join( self.data_dir, filename )
                reader.outputTable = table_name
                reader.sheetName = sheet_name
                reader.createTable()
    
    def test_13(self):
        # data locds;
        #     set ttest.g03;
        #     where courtesyTestedFlag=0 and TransferFlag=0 and AttemptFlagCRTmath=1;
        # run;
        with get_temp_table( self.db_context ) as loc_ds:
            g03=self.static_context.getTableSpec( 'g03' )
            query="""
                SELECT *
                INTO {loc_ds}
                FROM {table:qualified}
                WHERE courtesyTestedFlag=0 AND TransferFlag=0 AND AttemptFlagCRTmath=1;
            """.format( loc_ds=loc_ds, table=g03)
            self.db_context.executeNoResults(query)
            ttest = TTestLevel(ds_in=loc_ds,
                               ds_out="test13_testresult",
                               db_context=self.db_context,
                               input_col_name='upmxscor',
                               output_col_name='outvar',
                               target_group_cols=['schoolcode'],
                               parent_group_cols=['areacode'],
                               critical_value=1.96,
                               round_value=1 )
            ttest.execute()
        
        test_columns = (
            ( 'outvar', 'outvar', integer_compare ),
            ( 'schoolcode', 'schoolcode', mixed_compare ),
            ( 'areacode', 'areacode', mixed_compare ),
        )
        
        table_key_function = lambda row:( row.areacode, row.schoolcode )
        specimen_key_function = lambda row:( row[ 'areacode' ],
                                             row[ 'schoolcode' ] )
        
        answer_dir = os.path.join( self.answer_dir, 'test_13' )
        if not os.path.exists( answer_dir ):
            os.makedirs( answer_dir )
        answer_file = os.path.join( answer_dir, 'comparison.log' )
        result = compare_tables(
                answer_file,
                table="test13_testresult",
                specimen_name= os.path.join( self.specimen_dir, 'test_13/testresult.XLS' ),
                columns=test_columns,
                table_key_function=table_key_function,
                specimen_key_function=specimen_key_function,
                db_context=self.db_context)
        
        self.assertTrue( result, "Test 13 FAILED" )

    def test_20(self):
        input_assembly_function = lambda params, table: self.assemble_input_data_n_in_group( 1, params, table )
        self._run_tests( 20, input_assembly_function )
  
    def test_21(self):
        input_assembly_function = lambda params, table: self.assemble_input_data_n_in_group( 2, params, table )
        self._run_tests( 21, input_assembly_function )
          
    def test_22(self):
        def input_function( params, table ):
              
            # Pick all targets that have more than one person
            with get_temp_table( self.db_context ) as temp_table_1, get_temp_table( self.db_context ) as temp_table_2:
                query = """
                SELECT DISTINCT {parentgroups}, {targetgroups}
                INTO {temp_table_1:qualified}
                FROM {table:qualified}
                WHERE ({where}) AND ({wheret})
                    AND {parentgroups} IS NOT NULL
                    AND {targetgroups} IS NOT NULL
                GROUP BY {parentgroups}, {targetgroups}
                HAVING COUNT(1) > 1
                """.format( table=table, temp_table_1=temp_table_1, **params )
                self.db_context.executeNoResults(query)
                  
                # From those, pick the first target in each parent
                query= """
                SELECT {parentgroups}, {targetgroups},
                ROW_NUMBER() OVER( PARTITION BY {parentgroups} ORDER BY {targetgroups} ) AS r1
                INTO {temp_table_2:qualified}
                FROM {temp_table_1:qualified}
                """.format( temp_table_1=temp_table_1, temp_table_2=temp_table_2, **params )
                self.db_context.executeNoResults(query)
                  
                # For each selected target, pick the first two observations
                in_ds = get_temp_table( self.db_context )
                query = """
                SELECT {columns:itemfmt='C.{{}}'}
                INTO {in_ds}
                FROM (
                    SELECT {columns:itemfmt='A.{{}}'},
                    ROW_NUMBER() OVER( PARTITION BY A.{parentgroups}, A.{targetgroups} ORDER BY A.{targetgroups} ) AS r2
                    FROM (
                        SELECT {columns}
                        FROM {table:qualified}
                        WHERE ({where}) AND ({wheret})
                            AND {parentgroups} IS NOT NULL
                            AND {targetgroups} IS NOT NULL
                    ) AS A INNER JOIN (
                        SELECT {parentgroups}, {targetgroups}
                        FROM {temp_table_2:qualified}
                        WHERE r1=1
                    ) AS B
                    ON A.{parentgroups}=B.{parentgroups} AND A.{targetgroups}=B.{targetgroups}
                ) AS C
                WHERE C.r2<=2
                """.format( in_ds=in_ds, temp_table_2=temp_table_2,
                            table=table,
                            columns=Joiner( table ), **params )
                self.db_context.executeNoResults(query)
            in_ds.populate_from_connection()
            return in_ds
            
        self._run_tests( 22, input_function )
            
        
    def assemble_input_data_n_in_group( self, n_in_group, params, table ):

        in_ds = get_temp_table( self.db_context )
        query = """
        SELECT {columns:itemfmt='A.{{}}'}
        INTO {in_ds}
        FROM (
            SELECT {columns},
            DENSE_RANK() OVER( PARTITION BY {parentgroups} ORDER BY {targetgroups} ) AS r1,
            ROW_NUMBER() OVER( PARTITION BY {parentgroups}, {targetgroups} ORDER BY {targetgroups} ) AS r2
            FROM {table_name:qualified}
            WHERE ({where}) AND ({wheret})
                AND {parentgroups} IS NOT NULL
                AND {targetgroups} IS NOT NULL
        ) AS A
        WHERE A.r1<={n_in_group} AND A.r2=1
        """.format( table_name=table,
                    columns=Joiner( table ),
                    n_in_group=n_in_group,
                    in_ds=in_ds,
                    **params )
        self.db_context.executeNoResults(query)
        in_ds.populate_from_connection()
        return in_ds
    
    def _run_tests( self, test_nbr, input_data_assembly_function ):
        # %do grade=3 %to 8;
        #     data inds;
        #         set ttest.g0&grade.;
        #     run;
        # 
        #     sasfile inds load;
        succeed = True
        answer_dir = os.path.join( self.answer_dir, 'test_{}'.format( test_nbr ) )
        if not os.path.exists( answer_dir ):
            os.makedirs( answer_dir )
        answer_file = os.path.join( answer_dir, 'log_' )
        for ( grade, filename, table_name, sheet_name ) in _GRADE_FILES:
            self.run_context.info( "Testing ttest_level on grade {} data".format( grade ) )
            j = 1
            for params in self.agg_ds:

                # data inds_&j.(keep=&&invar_&j. &&parentgroups_&j. &&targetgroups_&j.);
                #     set inds;
                #     &&whereT_&j. and %substr(&&where_&j.,6);
                # run;
                # 
                # proc sort data=inds_&j.;
                #     by &&parentgroups_&j. &&targetgroups_&j.;
                # run;
                # 
                # data inds_&j. ;
                #     set inds_&j.;
                #     by  &&parentgroups_&j. &&targetgroups_&j.;
                #     if first. &&parentgroups_&j. AND missing(&&parentgroups_&j.)=0 and missing(&&targetgroups_&j.)=0 then  
                #         output;
                # run;
                
                params['where'] = clean_where( params['where'] )
                params['wheret'] = clean_where( params['wheret'] )
                table = self.static_context.getTableSpec(table_name)
                in_ds = input_data_assembly_function( params, table )
                
                # %Lib_ttestlevel(indata =inds_&j.,outdata=&&outdata_&j.,invar=&&invar_&j.,outlev=&&outlev_&j.,targetgroups=&&targetgroups_&j., 
                #       parentgroups=&&parentgroups_&j.,critval=&&critval_&j.,rdValue=&&rdvalue_&j.);
                ds_out = params['outdata'].replace( '&grade', str( grade ) )
                parentgroups = params['parentgroups']
                targetgroups = params['targetgroups']
                ttest = TTestLevel(ds_in=in_ds,
                                   ds_out="test{}_{}".format( test_nbr, ds_out ),
                                   db_context=self.db_context,
                                   input_col_name=params['invar'],
                                   output_col_name=params['outlev'],
                                   target_group_cols=[targetgroups],
                                   parent_group_cols=[parentgroups],
                                   target_where_expression=params['wheret'],
                                   parent_where_expression=params['where'],
                                   critical_value=params['critval'],
                                   round_value=params['rdvalue'])
                ttest.execute()
                
                test_columns = (
                    ( params['outlev'], params['outlev'], integer_compare ),
                    ( targetgroups, targetgroups, mixed_compare ),
                    ( parentgroups, parentgroups, mixed_compare ),
                )
                
                table_key_function = lambda row:( getattr( row, parentgroups ),
                                                  getattr( row, targetgroups ) )
                specimen_key_function = lambda row:( row[ parentgroups ],
                                                     row[ targetgroups ] )
                
                result = compare_tables(
                        answer_file + ds_out + ".log",
                        table="test{}_{}".format( test_nbr, ds_out ),
                        specimen_name= os.path.join( self.specimen_dir, 'test_{}'.format( test_nbr ), ds_out + ".XLS" ),
                        columns=test_columns,
                        table_key_function=table_key_function,
                        specimen_key_function=specimen_key_function,
                        db_context=self.db_context)
                succeed = succeed and result
                self.run_context.info( "Test_{} for ttest_level scenario {} {}".
                            format( test_nbr, ds_out, "PASSED" if result else "FAILED" ) )

                j += 1
        self.assertTrue( succeed, "Failures on ttest test {}".format( test_nbr ) )
                
def clean_where( epxression ):
    expression = _WHERE_KILLER.sub( '', epxression, 1 )
    expression = _MISSING_KILLER_0.sub( r'(\1 IS NOT NULL)', expression )
    expression = _MISSING_KILLER_1.sub( r'(\1 IS NULL)', expression )
    return expression
