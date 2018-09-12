import unittest
import os
import inspect

from airassessmentreporting.datacheck.dataset import (Dataset, data) 
import airassessmentreporting.datacheck.longcomp as dcl
import airassessmentreporting.datacheck.rescore as dcr
import airassessmentreporting.datacheck.raw_converter as dcc

class TestDatacheck(unittest.TestCase):
    """ Contains methods to test SAS-similar datacheck()-related functions """
    context = None

    @classmethod
    def setUpClass(cls):
        """
        Set up class variable 'context' for testing environment.
        Note that the export_test.ini file is in the same directory as this test script so they can be coordinated within the project.
        
        Future: SuiteContext() could accept absolute pathname of ini file and calculate and expose realpath of ini file
        """
        # Consider to add realpath and dirname to SuiteContext
        cls.dirname = os.path.dirname(os.path.realpath(__file__))
        #cls.context = SuiteContext(cls.dirname + "\\datacheck_test")
        # open a pyodbc dataset 

    def test_datacheck_context(self):
        """
        Simple sanity checks on the context derived from our ini file. 
        
        If we need to adopt related standards, such checks may best raise exceptions in the code and/or go into SuiteContext __init__(). 
        """
        return 1
        c = self.context
        
        self.assertIsNotNone(c,"self.context should be set, but it is None.")
        
        # Verify variable value of otherwise unused variable.
        tsd = ("""H:\Assessment\CSSC\AnalysisTeam"""
          """\AssessmentReporting\PythonUnitTestData""")
        self.assertEqual(c.tests_safe_dir,tsd,
            "Ini file tests_safe_dir '%s' is not correct." % c.tests_safe_dir)
        
        #datacheck(context=c)
        #self.assertEqual (1, 1,"1 does not equal 1")
        
    def test_longcomp_001(self):
        """ 
        Test longcomp() 
        
        NOTE: c.tests_dir may contain ad hoc test data that is not sensitive.
        Finally, non-sensitive input data like the ini file, and layout files should be kept under project source code control for reliable continuous integration.
        
        """
        # set up longcomp input datasets
        tdir = ("H:/Assessment/CSSC/AnalysisTeam/AssessmentReporting"
          "/PythonUnitTestData/longcomp/")
        
        fn_time0=tdir + "OGT_longcomp_time0.csv"
        dsr_time0 = Dataset(name=fn_time0,open_mode='rb')       

        fn_time1=tdir + "OGT_longcomp_time1.csv"
        dsr_time1 = Dataset(name=fn_time1,open_mode='rb')
        
        fn_longcomp_wb= tdir + "OGT_longcomp.xls"
        dsr_longcomp = Dataset(
          dbms='excel_srcn', workbook_file=fn_longcomp_wb,
          sheet_name=None, open_mode='rb')
        
        #set up longcomp output datasets
        fnw_full_report = tdir + "longcomp_full_report.csv"
        dsw_full_report = Dataset(name=fnw_full_report,open_mode='wb') 
                         
        brief_name = tdir +"longcomp_brief_report.csv"
        dsw_brief_report = Dataset(name=brief_name, open_mode='wb')     
        
        #Call longcomp
        output = dcl.longcomp(
          dsr_time0=dsr_time0, dsr_time1=dsr_time1, dsr_longcomp=dsr_longcomp,
          dsw_full_report=dsw_full_report,
          dsw_brief_report=dsw_brief_report )
        
        # test the assertions       
        self.assertEqual ( 
          len(output), 102, "Length of output is %d, not 102" % len(output))
       
           
    def test_rescorecheck_001(self):
        """ 
        Test rescorecheck() 
                
        """
        # set up longcomp input datasets
        tddir = (
          "H:/Assessment/CSSC/AnalysisTeam/AssessmentReporting/"
          "PythonUnitTestData/rescorecheck/")        
        
        #input datasets
        fn_input = tddir + "rescorecheck_input.csv"
        ds_input = Dataset(dbms='csv', name=fn_input, open_mode='rb') 
        #dr = ds_input.DictReader()
        #dr2 = ds_input.DictReader()
        #print "input fields='%s'" % repr(dr2.fieldnames)   
        bookmaplocs_filename = ( 
          tddir + "OGT_SP13_Op_DataLayout_bookmapLocations.xls")

        ds_bookmaplocs = Dataset(
          dbms='excel_srcn', 
          workbook_file=bookmaplocs_filename,
          sheet_name="Bookmap", open_mode='rb')
        
        #output datasets
        out_filename = tddir+"rescore_out.csv"        
        ds_out = Dataset(name=out_filename, open_mode='wb')
        
        report2_filename = tddir+"rescore_report2.csv"
        ds_report2 = Dataset(name=report2_filename, open_mode='wb')
        
        dcr.rescorecheck(grade='10', subject="Math", 
          ds_input=ds_input, 
          ds_bookmaplocs=ds_bookmaplocs,
          ds_out=ds_out, ds_report2=ds_report2)

        # test the assertions 
        del ds_report2
        with open(report2_filename) as f:
            for nl, l in enumerate(f):
                pass
        nl += 1
      
        self.assertEqual ( 
          nl, 168, "Lines in rescore_report2 is %d, not 168" % nl)                 
           
    def test_raw_converter_001(self):
        """ 
        Test raw_converter() 
               
        """
        # Define the test data directory
        tddir = ("H:/Assessment/CSSC/AnalysisTeam/AssessmentReporting/"
          "PythonUnitTestData/"
          "raw_score_converter/2013_March_Final/")        
       
        # input datasets
        fn_input_csv = tddir + "OGT_raw_converter_input.csv"
        ds_raw_scores = Dataset(name=fn_input_csv, open_mode='rb')
        
        fn_semantic_workbook = tddir + "OGT_semantic_workbook.xls"
        ds_standards = Dataset(
          dbms='excel_srcn', 
          workbook_file=fn_semantic_workbook, 
          sheet_name="Standards", open_mode='rb')
        #&ctpath_breach
        # output datasets
        fn_converter_out = tddir + "converter_out.csv"
        ds_out = Dataset(name=fn_converter_out, open_mode='wb')

        fn_converter_report2 = tddir +"converter_report2.csv"
        ds_report2 = Dataset(name=fn_converter_report2, open_mode='wb')
        
        fn_sumcheck_out = tddir + "sumcheck_out.csv"
        ds_sumcheck_out = Dataset(name=fn_sumcheck_out, open_mode='wb')
        
        fn_sumcheck_report2 = tddir + "sumcheck_report2.csv"
        ds_sumcheck_report2 = Dataset(name=fn_sumcheck_report2, open_mode='wb')
       
        print "Using ds_standards = '%s'" % repr(ds_standards)
        print "Using ds_raw_scores = '%s'" % repr(ds_raw_scores)
        print "Using ds_out = '%s'" % repr(ds_out)
        print "Using ds_report2= '%s'" % repr(ds_report2)
        dict_loc_subs = {r'&ctpath.\\': tddir}
        print "Using dict_loc_subs= '%s'" % repr(dict_loc_subs)                
        print "Calling raw_converter(...)"
        # 
        dcc.raw_converter(
          grade='10', subject="Math", 
          ds_raw_scores=ds_raw_scores,
          ds_standards=ds_standards,
          odict_loc_subs=dict_loc_subs,
          ds_sumcheck_out=ds_sumcheck_out,
          ds_sumcheck_report2=ds_sumcheck_report2,
          ds_out=ds_out, 
          ds_report2=ds_report2) 
        
        del ds_report2
        with open(fn_converter_report2) as f:
            for nl, l in enumerate(f):
                pass
        nl += 1
      
        self.assertEqual ( 
          nl, 222, "Lines in converter_report2 is %d, not 222" % nl)                 

    def test_data_001(self):
        tdd = (
          "H:/Assessment/CSSC/AnalysisTeam/AssessmentReporting/"
          "PythonUnitTestData/"
          "data_copier/")
        
        print "\ntest_data_001: Starting: Assigning datasets for first run of data()"
        dsr=Dataset(open_mode='rb', name=tdd+"students_time0.csv")
        dsw=Dataset(open_mode='wb', name=tdd+"students_data2.csv")
        
        print "Run 1 of data() to copy from csv to csv"
        
        column_names = data(dsr,dsw)
        print "Done run 1."
        
        # Write new csv file to table tmptable1
        #server = 'DC1PHILLIPSR\SQLEXPRESS'
        #database = 'testdb' 
        print "Assigning datasets for run 2 of data()"
        
        server = "38.118.83.61"
        database = 'ScoreReportingTestData'
        
        dsr = Dataset(open_mode='rb', name=tdd+"students_data2.csv")
        
        dsw = Dataset(dbms='pyodbc', table='tmp_data_test1', replace=True, columns=column_names,
          server=server, db=database, open_mode='wb')
        
        print "Run 2 of data() to copy from csv to pyodbc table 'tmp_data_test1'"
        data(dsr,dsw)
        print "Done run 2."
    
        print "Assigning datasets for run 3 of data()"
     
        dsr = Dataset(dbms='pyodbc', table='tmp_data_test1',
          server=server, db=database, open_mode='rb')
        fn_data3 = tdd+"students_data3.csv"
        dsw = Dataset(name=fn_data3, open_mode='wb')
        
        print "Run 3 of data() to copy from pyodbc table to csv"
        
        data(dsr,dsw)
        
        del dsw
        with open(fn_data3) as f:
            for nl, l in enumerate(f):
                pass
        nl += 1
      
        self.assertEqual ( 
          nl, 14339, "Lines in students_data3.csv is %d, not 14339" % nl)                 

        
        print "Done run 3. All done testing test_data_001().\n"        
        return 
    def test_rvp_001(self):
        tdd = (
          "H:/Assessment/CSSC/AnalysisTeam/AssessmentReporting/"
          "PythonUnitTestData/"
          "data_copier/")
        iam = inspect.stack()[0][3]

        print(
          "\n%s: Starting: Create and copy dbms='tvp' Dataset." % iam)
        dsr=Dataset(open_mode='rb', name=tdd+"students_time0.csv")
        dsw=Dataset(open_mode='wb', name=tdd+"students_data2.csv")
        
        print "Run 1 of data() to copy from csv to csv"
        
        column_names = data(dsr,dsw)
        print "Done run 1."
        
        # Write new csv file to table tmptable1
        #server = 'DC1PHILLIPSR\SQLEXPRESS'
        #database = 'testdb' 
        print "Assigning datasets for run 2 of data()"
        
        server = "38.118.83.61"
        database = 'ScoreReportingTestData'
        
        dsr = Dataset(open_mode='rb', name=tdd+"students_data2.csv")
        
        dsw = Dataset(dbms='pyodbc', table='tmp_data_test1', replace=True, columns=column_names,
          server=server, db=database, open_mode='wb')
        
        print "Run 2 of data() to copy from csv to pyodbc table 'tmp_data_test1'"
        data(dsr,dsw)
        print "Done run 2."
    
        print "Assigning datasets for run 3 of data()"
     
        dsr = Dataset(dbms='pyodbc', table='tmp_data_test1',
          server=server, db=database, open_mode='rb')
        fn_data3 = tdd+"students_data3.csv"
        dsw = Dataset(name=fn_data3, open_mode='wb')
        
        print "Run 3 of data() to copy from pyodbc table to csv"
        
        data(dsr,dsw)
        
        del dsw
        with open(fn_data3) as f:
            for nl, l in enumerate(f):
                pass
        nl += 1
      
        self.assertEqual ( 
          nl, 14339, "Lines in students_data3.csv is %d, not 14339" % nl)                 

        
        print "Done run 3. All done testing test_data_001().\n"        
        return 
        
        
if __name__ == '__main__':
    unittest.main()
