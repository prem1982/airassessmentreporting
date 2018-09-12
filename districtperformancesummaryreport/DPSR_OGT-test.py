#
# TODO - coding!!
# TODO - testing
#

"""
Created on September 17, 2013

@author: temp_tprindle
"""

import os.path
import airassessmentreporting.districtperformancesummaryreport as dpsr
from airassessmentreporting.testutility import SuiteContext

RUN_CONTEXT_NUMBER = 1

UNITTEST_SUBDIR = 'district_performance_summary_report'
SPECFILE = os.path.join( UNITTEST_SUBDIR, 'DPSR-spec-20130917.xls' )

def main( ):
    """
    This runs a test of the district_performance_summary_report wrapper/glue code.
    """
    run_context = SuiteContext( 'OGT_test{}'.format( RUN_CONTEXT_NUMBER ) )
    log = run_context.get_logger( 'DistrictPerformanceSummaryReport' )

    specfile = os.path.join( run_context.tests_safe_dir, SPECFILE )
    log.debug( "main - specfile[{}]".format( specfile ) )

    # sch_type = H = 12 records
    # sch_type = P = 10495 records

    dpsr.district_performance_summary_report(run_context=run_context, specfile=specfile, input_table_name='student' )

if __name__ == '__main__':
    main( )
