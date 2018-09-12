'''
Created on May 30, 2013

@author: temp_dmenes
'''

import os
import unittest

from airassessmentreporting.ttest import TTest
from airassessmentreporting.testutility import compare_tables, integer_compare, mixed_compare
from airassessmentreporting.airutility import Joiner

_XLS_FILE="classinfo.xlsx"
_XLS_FILE_G3="G3.xlsx"
_XLS_SHEET=0
_AGG_FILE="AggSheet.xls"
_AGG_FILE_OAT="OAT_Agg_Sheet.xls"

COLUMNS=(
    ( 'tcrxid', 'tcrxid', integer_compare ),
    ( 'comp', 'comp', integer_compare ),
)

from airassessmentreporting.airutility import SafeExcelReader, dbutilities
from airassessmentreporting.testutility import SuiteContext
class TTestTest( unittest.TestCase ):
    def setUp(self):
        self.run_context = SuiteContext( 'unittest' )
        self.db_context = self.run_context.getDBContext( 'unittest' )
        self.data_dir = os.path.join( self.run_context.tests_safe_dir, 'ttest', 'input_data' )
        self.answer_dir = os.path.join( self.run_context.logs_dir, 'ttest_tests' )
        self.specimen_dir = os.path.join( self.run_context.tests_safe_dir, 'ttest', 'sas_outputs' )
        
    def test_10(self):
        data_file = os.path.join( self.data_dir, _XLS_FILE )
        reader = SafeExcelReader(self.run_context, data_file, _XLS_SHEET, 'class_info', self.db_context, scan_all = True )
        reader.createTable()
        ttester = TTest( 'class_info', self.db_context, os.path.join( self.data_dir, _AGG_FILE ), 0, True )
        ttester.readAggData()
        ttester.execute()
        
        answer_dir = os.path.join( self.answer_dir, 'test_10' )
        if not os.path.exists( answer_dir ):
            os.makedirs( answer_dir )
        answer_file = os.path.join( answer_dir, 'comparison.log' )
        specimen_dir = os.path.join( self.specimen_dir, 'ttest_test_10' )
        compare_function=lambda row: ( int( row.tcrxid ), int( row.comp ) )
        result = compare_tables(
                answer_file,
                table="ttest_class",
                specimen_name= os.path.join( specimen_dir, 'testresult.XLS' ),
                columns=COLUMNS,
                table_key_function=compare_function,
                specimen_key_function=compare_function,
                db_context=self.db_context)
        
        self.assertTrue( result, "TTest Test 10 FAILED" )
        
    def test_17(self):
        self.do_17_18("UPDATE {agg_sheet} SET [WhereT]='Rclass_missing_flag', [WhereT_value]='0'")
        
    def test_18(self):
        self.do_17_18("UPDATE {agg_sheet} SET [WhereP]='dummy_record_flag', [WhereP_value]='0'")

    def do_17_18(self, tweak_agg_sheet_query):
        with self.read_g3() as g3, \
             self.read_oat_agg_sheet() as temp_agg_sheet, \
             dbutilities.get_temp_table(self.db_context) as agg_sheet:
            
            ## Keep first row of aggregation definition
            self.db_context.executeNoResults("SELECT TOP(1) * INTO {agg_sheet} FROM {temp_table} WHERE [subject]='R' ORDER BY [import_order]"
                                             .format( agg_sheet=agg_sheet, temp_table=temp_agg_sheet ))
            
            print "Running first ttest"
            ttester1 = TTest( g3, self.db_context, agg_sheet, None, False )
            ttester1.readAggData()
            ttester1.execute()
            results1 = []
            for level in ttester1.target_levels:
                results1.append( dbutilities.dump( level.output_table, level.id ) )
        
            print "Updating data sheet"
            self.db_context.executeNoResults("DELETE FROM {g3} WHERE [inclusionflagr] IS NULL OR [inclusionflagr] != 1".format( g3 = g3 ))
            self.db_context.executeNoResults(tweak_agg_sheet_query.format( agg_sheet = agg_sheet ))
        
            print "Running second ttest"
            ttester2 = TTest( g3, self.db_context, agg_sheet, None, False )
            ttester2.readAggData()
            ttester2.execute()
            results2 = []
            for level in ttester2.target_levels:
                results2.append( dbutilities.dump( level.output_table, level.id ) )
            
            print "Comparing ttest outputs"
            assert len( results1 ) >= 1
            assert len( results1 ) == len( results2 )
            for i in range( len( results1 ) ) :
                res1 = results1[ i ]
                res2 = results2[ i ]
                keys = res1[0].keys()
                
                assert len( res1 ) == len( res2 )
                for j in range( len( res2 ) ):
                    row1 = res1[ j ]
                    row2 = res2[ j ]
                    for k in keys:
                        assert row1[ k ] == row2[ k ]


    def test_20(self):
        with self.read_g3() as g3, \
             self.read_oat_agg_sheet() as agg_sheet:
            
            self.db_context.executeNoResults("DELETE FROM {agg_sheet} WHERE [subject] != 'R'".format( agg_sheet = agg_sheet ))
            self.db_context.executeNoResults("DELETE FROM {g3} WHERE import_order != 8".format( g3=g3 ))

            ttester = TTest( g3, self.db_context, agg_sheet, None, False )
            ttester.readAggData()
            ttester.execute()
        
            answer_dir = os.path.join( self.answer_dir, 'test_20' )
            if not os.path.exists( answer_dir ):
                os.makedirs( answer_dir )
            specimen_dir = os.path.join( self.specimen_dir, 'ttest_test_20' )
            
            result = True
            for target_level in  ttester.target_levels:
                answer_file = os.path.join( answer_dir, target_level.level + '_comparison.log' )
                specimen_file = os.path.join(specimen_dir, 'test20_ttest_{0}.xls'.format(target_level.level))
                result_i = self.compare_output(specimen_file, target_level, answer_file)
                result = result and result_i
                if result_i:
                    print "PASSED ttest test_20 for " + target_level.level
                else:
                    print "FAILED ttest test_20 for " + target_level.level
            
            self.assertTrue( result, "TTest Test 20 FAILED" )
            
    def test_21(self):
        answer_dir = os.path.join( self.answer_dir, 'test_21' )
        if not os.path.exists( answer_dir ):
            os.makedirs( answer_dir )
        specimen_dir = os.path.join( self.specimen_dir, 'ttest_test_21' )
        result = True

        with self.read_g3() as g3, \
             self.read_oat_agg_sheet() as agg_sheet, \
             dbutilities.get_temp_table( self.db_context ) as tmp, \
             dbutilities.get_temp_table( self.db_context ) as tmp_agg :
            
            # As near as I can tell, the SAS test only runs for the tenth row of the agg sheet.
            # self.db_context.executeNoResults("DELETE FROM {agg_sheet} WHERE [import_order] != 10".format( agg_sheet=agg_sheet ))
            
            # We are just using this TTest instance to read the aggregation sheet. The actual ttest will use
            # another instance based on a truncated aggregation sheet.
            agg_sheet_reader = TTest( g3, self.db_context, agg_sheet, None, False )
            agg_sheet_reader.readAggData()
            
            assert dbutilities.table_exists(g3)
            
            targetParentRow = []
            for target_level in agg_sheet_reader.target_levels:
                for parent_level in target_level.contents:
                    for row in parent_level.contents:
                        targetParentRow.append( (target_level, parent_level, row ) )
            
            targetParentRow.sort( key = lambda(row): row[2].import_order )
            
            for target_level, parent_level, row in targetParentRow:
                where_t = target_level.get_where_expression()
                target_id = target_level.id
                where_p = parent_level.get_where_expression()
                parent_id = parent_level.id
                i = row.import_order
                    
                # Reduce the data to the desired sample
                dbutilities.drop_table_if_exists(tmp)
                query = """
                SELECT {vars},
                        COUNT( {input_var} ) OVER( PARTITION BY {parent_id}, {target_id} ) AS n_target,
                        0 AS n_parent
                INTO {tmp}
                FROM {g3}
                WHERE {where_t}
                """.format( parent_id=parent_id,
                            target_id=target_id,
                            input_var=row.inputvar,
                            where_t=where_t,
                            tmp=tmp,
                            g3=g3,
                            vars=Joiner(g3) )
                self.db_context.executeNoResults( query )
                query="""
                UPDATE {tmp} SET n_parent = A.B FROM (
                    SELECT n_parent, COUNT( {input_var} ) OVER( PARTITION BY {parent_id} ) AS B
                    FROM {tmp}
                    WHERE {where_p}
                ) AS A
                """.format( parent_id=parent_id,
                            input_var=row.inputvar,
                            where_p=where_p,
                            tmp=tmp )
                print query
                self.db_context.executeNoResults( query )
                query = "DELETE FROM {tmp} WHERE ( n_parent != 2 ) OR ( n_target != 1 )".format( tmp=tmp )
                self.db_context.executeNoResults( query )
                n_obs = dbutilities.n_obs(tmp)
                if n_obs > 0 :
                
                    # Reduce the aggregation sheet to the current row
                    query = "SELECT * INTO {tmp_agg} FROM {agg_sheet} WHERE [import_order]={i}".format(
                                tmp_agg=tmp_agg,
                                agg_sheet=agg_sheet,
                                i=i )
                    self.db_context.executeNoResults( query )
                    
                    # Do the ttest
                    ttester = TTest( tmp, self.db_context, tmp_agg, None, False )
                    ttester.readAggData()
                    ttester.execute()
                    
                    # Check the answer
                    answer_file = os.path.join( answer_dir, 'row_{0}_comparison.log'.format( i ) )
                    specimen_file = os.path.join(specimen_dir, 'test_21_ttest_{0}.xls'.format( i ) ) 
                    result_i = self.compare_output(specimen_file, target_level, answer_file)
                    result = result and result_i
                    print "{1} ttest test_21 for {0}".format( i, 'PASSED' if result_i else 'FAILED' )
        
                    self.assertTrue( result, "TTest Test 21 FAILED" )
                    return
                        

    def read_g3(self):
        print "Reading data"
        table = dbutilities.get_temp_table( self.db_context )
        data_file = os.path.join( self.data_dir, _XLS_FILE_G3 )
        reader = SafeExcelReader(self.run_context, data_file, _XLS_SHEET, table, self.db_context, scan_all = False )
        reader.createTable()
        
        print "Tweaking data"
        self.db_context.executeNoResults("UPDATE {g3} SET [state_inc_flag] = CASE WHEN ( [schtype] IN ('N','D','H') ) THEN 0 ELSE 1 END".format( g3=table ))
        self.db_context.executeNoResults("UPDATE {g3} SET [Rclass_missing_flag] = CASE WHEN ( [Rclass_id] IS NULL OR [Rclass_id]='' ) THEN 1 ELSE 0 END".format( g3=table ))
        table.populate_from_connection()
        return table
    
    def read_oat_agg_sheet(self):
        print "Reading aggregation sheet"
        table = dbutilities.get_temp_table( self.db_context )
        data_file=os.path.join( self.data_dir, _AGG_FILE_OAT )
        reader = SafeExcelReader(self.run_context, data_file, _XLS_SHEET, table, self.db_context, scan_all = True )
        reader.createTable()
        table.populate_from_connection()
        return table
    
    def compare_output(self, specimen_file, target_level, answer_file):
        compare_function = lambda row:row[target_level.id]
        output_vars = set([dbutilities.db_identifier_unquote(row.outputvar) for 
                parent_level in target_level.contents for 
                row in parent_level.contents])
        assert len(output_vars) >= 1
        columns = [(target_level.id, target_level.id, mixed_compare)]
        for var in output_vars:
            columns.append((var, var, integer_compare))
        
        print target_level.level, output_vars
        result_i = compare_tables(
            log_name=answer_file, 
            table=target_level.output_table,
            specimen_name=specimen_file,
            columns=columns,
            table_key_function=compare_function,
            specimen_key_function=compare_function,
            db_context=self.db_context)
        return result_i

