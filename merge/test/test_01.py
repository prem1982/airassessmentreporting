import unittest
import os.path

from airassessmentreporting.merge import *
from airassessmentreporting.airutility import yesno
from abstractmergetestcase import AbstractMergeTestCase
from airassessmentreporting.testutility import ( integer_compare,
        mixed_compare, to_str, compare_tables )


def truncated_compare( x, y ):
    if x is None:
        return y is None
    if y is None:
        return False
    return x[:12] == y[:12]

_JOIN_NAMES = {
    JOIN_TYPE_LEFT:'LEFT',
    JOIN_TYPE_INNER:'INNER',
    JOIN_TYPE_FULL:'OUTER'               
}

OUT_COLUMNS = (
    ( 'char_1',       'char_1',       None ),
    ( 'char_2',       'char_2',       None ),
#    ( 'ethnicity',    'ethnicity',    None ),
    ( 'gender',       'gender',       None ),
    ( 'n1',           'n1',           None ),
    ( 'n2',           'n2',           None ),
    ( 'num_1',        'num_1',        integer_compare ),
    ( 'num_2',        'num_2',        integer_compare ),
    ( 'studentfnm',   'studentfnm',   truncated_compare ),
    ( 'studentid',    'studentid',    None ),
    ( 'studentlnm',   'studentlnm',   None ),
    ( 'barcode_char', 'barcode_char', None ),
    ( 'barcode_num',  'barcode_num',  integer_compare ),
)

FUZZY_COLUMNS_A = (
    ( 'barcode_num',   'tmp1barcode_num', integer_compare ),
    ( 'lfuzzykey_1_1', 'tmp1studentlnm',  mixed_compare ),
    ( 'lfuzzykey_1_2', 'tmp1studentfnm',  mixed_compare ),
    ( 'rfuzzykey_1_1', 'tmp2studentlnm',  mixed_compare ),
    ( 'rfuzzykey_1_2', 'tmp2studentfnm',  mixed_compare ),
)

FUZZY_COLUMNS_B = (
    ( 'primary1',      'tmp1barcode_num', integer_compare ),
    ( 'lfuzzykey_1_1', 'tmp1studentlnm',  mixed_compare ),
    ( 'lfuzzykey_1_2', 'tmp1studentfnm',  mixed_compare ),
    ( 'rfuzzykey_1_1', 'tmp2studentlnm',  mixed_compare ),
    ( 'rfuzzykey_1_2', 'tmp2studentfnm',  mixed_compare ),
)


'''A python implementation of the checks that are performed in MergeMacro_test1.sas
'''
class MergeTest01( AbstractMergeTestCase ):
    
    def test_01(self):
        '''Run the same set of tests as were performed in SAS, and compare the
        results
        '''
        answer_dir = os.path.join( self.run_context.logs_dir, 'merge_test_01' )
        if not os.path.exists( answer_dir ):
            os.makedirs( answer_dir )
        answer_file = os.path.join( answer_dir, 'log' )
        succeed = self._doMergePermutations( None, answer_file, 0.8, 'A' )
        self.assertTrue( succeed, "Merge tests failed. See logs in {}".format( answer_dir ) )


    def test_01b( self ):
        '''Repeat the test using a merge spec read from Excel instead of the one
        created in the constructor 
        '''
        answer_dir = os.path.join( self.run_context.logs_dir, 'merge_test_01b' )
        if not os.path.exists( answer_dir ):
            os.makedirs( answer_dir )
        answer_file = os.path.join( answer_dir, 'log' )
        
        spec_file = os.path.join( self.run_context.tests_safe_dir, 'merge_spec.xls' )
        read_spec_file( spec_file, self.merge_def )
        succeed = self._doMergePermutations( None, answer_file, 0.8, 'B' )
        self.assertTrue( succeed, "Merge tests failed. See logs in {}".format( answer_dir ) )

    def _doMergePermutations( self, spec_file, answer_file, similarity_threshold, fuzzy_version ):
        # merge.createSpecFileIfNotExists( spec_file )
        # self.merge_def.fieldSpec = merge.readSpecFile( spec_file )
        self.merge_def.similarity_thresholds = similarity_threshold
        succeed = True
        
        for allow_dups_left in ( True, False ):
            self.merge_def.allow_dups_left = allow_dups_left
            
            for allow_dups_right in ( True, False ):
                self.merge_def.allow_dups_right = allow_dups_right

                dups_both_permutations = ( True, False ) if ( allow_dups_left and allow_dups_right ) else ( False, )
                for allow_dups_both in dups_both_permutations:
                    self.merge_def.allow_dups_both = allow_dups_both
                    for join_type in ( JOIN_TYPE_LEFT, JOIN_TYPE_INNER, JOIN_TYPE_FULL ):
                        self.merge_def.join_type = join_type
                        case_name = "_".join( ( yesno(allow_dups_left),
                                                yesno(allow_dups_right),
                                                yesno(allow_dups_both),
                                                _JOIN_NAMES[ join_type ] ) )
                        self.merge_def.table_name = 'mergeOut_' + case_name
                        self.merge_def.fuzzy_report_table = 'fuzzy_'+ case_name
                        self.merge_def.left_remain_table = 'left_remain_' + case_name
                        self.merge_def.right_remain_table = 'right_remain_' + case_name
                        if type == JOIN_TYPE_FULL:
                            self.merge_def.left_remain_table = self.merge_def.right_remain_table = None
                        elif type == JOIN_TYPE_LEFT:
                            self.merge_def.left_remain_table = None
                
                        self.merge_def.execute()
                        del self.merge_def['fk_right_1']
                        del self.merge_def['fk_left_1']
                        
                        result = self.compare_output_tables( case_name, answer_file )
                        if self.merge_def.left_remain_table is not None:
                            result = result and self.compare_remain_tables( case_name, answer_file, 1, 'left' )
                        if self.merge_def.left_remain_table is not None:
                            result = result and self.compare_remain_tables( case_name, answer_file, 2, 'right' )
                        if fuzzy_version == 'A':
                            result = result and self.compare_fuzzy_tables_a( case_name, answer_file )
                        else:
                            result = result and self.compare_fuzzy_tables_b( case_name, answer_file )
                        succeed = succeed and result
                        self.run_context.info( "{1}: Merge test 01 for case {0}".format( case_name, 'PASSED' if result else 'FAILED' ) )
        return succeed

    def compare_output_tables( self, case_name, answer_file ):
        log_name = answer_file + '_OUTPUT_' + case_name
        specimen_name = 'DS_OUT_{}.xls'.format( case_name )
        specimen_name = os.path.join( self.run_context.tests_safe_dir,
                'merge_outputs', specimen_name )
        sort_fun = lambda row: ( None if row.barcode_num is None else int( float( row.barcode_num ) + 0.5 ),
                                   row.n1,
                                   row.n2 )
        return compare_tables( log_name, self.merge_def.table_name, specimen_name,
                OUT_COLUMNS, sort_fun, sort_fun, self.db_context, 0 )
        
    def compare_remain_tables(self, case_name, answer_file, specimen_side, output_side ):
        return True
        
    def compare_fuzzy_tables_a(self, case_name, answer_file ):
        log_name = answer_file + '_FUZZY_REPORT_' + case_name
        table_name = 'fuzzy_{}'.format( case_name )
        specimen_name = 'FUZZY_{}.xls'.format( case_name )
        specimen_name = os.path.join( self.run_context.tests_safe_dir, 'merge_outputs', specimen_name )
        
        def table_sort( row ):
            barcode_num = None if row.barcode_num is None else int( float( row.barcode_num ) + 0.5 )
            return ( barcode_num, to_str( row.lfuzzykey_1_1 ), to_str( row.lfuzzykey_1_2 ), to_str( row.rfuzzykey_1_1 ),
                     to_str( row.rfuzzykey_1_2 ) )

        def specimen_sort( row ):
            barcode_num = None if row['tmp1barcode_num'] is None else int( row['tmp1barcode_num'] )
            return ( barcode_num, to_str( row['tmp1studentlnm'] ), to_str( row['tmp1studentfnm'] ),
                     to_str( row['tmp2studentlnm'] ), to_str( row['tmp2studentfnm'] ) )

        return compare_tables( log_name, table_name, specimen_name,
                FUZZY_COLUMNS_A, table_sort, specimen_sort, self.db_context, 0 )

    def compare_fuzzy_tables_b(self, case_name, answer_file ):
        log_name = answer_file + '_FUZZY_REPORT_' + case_name
        table_name = 'fuzzy_{}'.format( case_name )
        specimen_name = 'FUZZY_{}.xls'.format( case_name )
        specimen_name = os.path.join( self.run_context.tests_safe_dir, 'merge_outputs', specimen_name )
        
        def table_sort( row ):
            barcode_num = None if row.primary1 is None else int( float( row.primary1 ) + 0.5 )
            return ( barcode_num, to_str( row.lfuzzykey_1_1 ), to_str( row.lfuzzykey_1_2 ), to_str( row.rfuzzykey_1_1 ),
                     to_str( row.rfuzzykey_1_2 ) )

        def specimen_sort( row ):
            barcode_num = None if row['tmp1barcode_num'] is None else int( float( row['tmp1barcode_num'] ) + 0.5 )
            return ( barcode_num, to_str( row['tmp1studentlnm'] ), to_str( row['tmp1studentfnm'] ),
                     to_str( row['tmp2studentlnm'] ), to_str( row['tmp2studentfnm'] ) )

        return compare_tables( log_name, table_name, specimen_name,
                FUZZY_COLUMNS_B, table_sort, specimen_sort, self.db_context, 0 )



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()