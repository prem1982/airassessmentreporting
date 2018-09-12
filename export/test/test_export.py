import unittest
import os

from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.export import export

class TestExport(unittest.TestCase):
    """ Contains methods to test SAS-similar export()function """
    context = None

    @classmethod
    def setUpClass(cls):
        """
        Set up class variable 'context' for testing environment.
        Note that the export_test.ini file is in the same directory as this test script so they can be coordinated within the project.
        
        Future: SuiteContext() could accept absolute pathname of ini file and calculate and expose realpath of ini file
        For now, can manually copy the current directory's export_test.ini file to a directory where it will be sought by RunContext, eg ~/air_python/export_test.ini, but it's a step we could otherwise skip.
        """
        # Consider to add realpath and dirname to SuiteContext
        cls.dirname = os.path.dirname(os.path.realpath(__file__))
        cls.context = SuiteContext("export_test")


    def test_export_context(self):
        """
        Simple sanity checks on the context derived from our ini file. 
        
        If we need to adopt related standards, such checks may best raise exceptions in the code and/or go into SuiteContext __init__(). 
        """
        c = self.context
        
        self.assertIsNotNone(c,"self.context should be set, but it is None.")
        
        # Verify variable value of otherwise unused variable.
        tsd = ("""H:\Assessment\CSSC\AnalysisTeam"""
          """\AssessmentReporting\PythonUnitTestData""")
        self.assertEqual(c.tests_safe_dir,tsd,
            "Ini file tests_safe_dir '%s' is not correct." % c.tests_safe_dir)

    def test_export_csv(self):
        """ 
        Test export to csv files 

        NOTE: air_python project policy is to keep test table 'science_cut_1000' 
        in development test server database.

        NOTE: air_python project policy is to keep sensitive data secure under c.tests_safe_dir (defined in the ini file), so it is outside of source code control and safe from accidental exposure.
        
        NOTE: export_test.ini may have set c.tests_dir where they want, but it is not safe for sensitive data. 
        It is probably a good place for output logs that tested functions produce that emit no sensitive data.
        
        NOTE: c.tests_dir may contain ad hoc test data that is not sensitive.
        Finally, non-sensitive input data like the ini file, and layout files should be kept under project source code control for reliable continuous integration.
        
        """
        #notational convenience
        c = self.context
        
        table_name='science_cut_1000'

        out_file = ( c.tests_safe_dir + os.sep + 
          "\\export_tests\\%s_exp.csv" % table_name )
        # test that output file handle can be opened
        ofh = open(out_file,"w")
        self.assertIsNotNone(ofh,"Output file %s not openable for 'w'" 
          % out_file)
        ofh.close() # close it to prep for export() call.
      
        nrows = export(context=c, table_name=table_name, out_file=out_file, 
          replace=1)
        self.assertEqual(nrows,999)
    def test_export_csv2(self):
        """ 
        Test export to csv files 

        NOTE: air_python project policy is to keep test table 'science_cut_1000' 
        in development test server database.

        NOTE: air_python project policy is to keep sensitive data secure under c.tests_safe_dir (defined in the ini file), so it is outside of source code control and safe from accidental exposure.
        
        NOTE: export_test.ini may have set c.tests_dir where they want, but it is not safe for sensitive data. 
        It is probably a good place for output logs that tested functions produce that emit no sensitive data.
        
        NOTE: c.tests_dir may contain ad hoc test data that is not sensitive.
        Finally, non-sensitive input data like the ini file, and layout files should be kept under project source code control for reliable continuous integration.
        
        """
        #notational convenience
        c = self.context
        
        table_name='OGT_ConversionCheck'

        out_file = ( c.tests_safe_dir + os.sep + 
          "\\export_tests\\%s_exp_conv_check.csv" % table_name )
        # test that output file handle can be opened
        ofh = open(out_file,"w")
        self.assertIsNotNone(ofh,"Output file %s not openable for 'w'" 
          % out_file)
        ofh.close() # close it to prep for export() call.
      
        nrows = export(context=c, table_name=table_name, out_file=out_file, 
          replace=1)
        self.assertEqual(nrows,1000)

    
    def test_export_sel_1_fxd(self):
        """ Test export to file with fixed width column values
        """
        c = self.context
        # set basic parameters
        table_name='science_cut_1000'
        out_file = ( c.tests_safe_dir + os.sep + 
          "\\export_tests\\%s_exp_sel_1.fxd" % table_name )
        
        #Put non-sensitive test data (eg, layouts) into source-code control
        layout = self.dirname + '\\science_cut_layout.xlsx'
        where = "final_gender = 'F' and final_grade=10"
        orderby = "final_ssid"
        
        # test that output file handle can be opened.
        ofh = open(out_file,"w")
        self.assertIsNotNone(ofh,"Output file %s not openable for 'w'" 
           % out_file)
        ofh.close() # close it to prep for export() call.
        
        nrows = export(context=c, table_name=table_name, dbms='fixed',
          out_file=out_file, lspec='spec1', layout=layout, replace=1,
          where=where, orderby=orderby)
        
        self.assertEqual(nrows,211, 
          "Rather than the expected 211, nrows is %d." % nrows)
        
    def test_export_sel_1_csv(self):
        """ Test export to select to csv output.
        """
        c = self.context
        table_name='science_cut_1000'
        out_file = ( c.tests_safe_dir + os.sep + 
          "\\export_tests\\%s_exp_sel_1.csv" % table_name )
        columns = ("final_districtid, final_districtname, final_firstname,"
          "final_lastname, final_schoolid, final_ssid")        
        where = "final_districtid = 942"
        orderby = "final_ssid"
        
        # Test that output file handle can be opened.
        ofh = open(out_file,"w")
        self.assertIsNotNone(ofh,"Output file %s not openable for 'w'" 
          % out_file)
        ofh.close() # close it to prep for export() call.
    
        # Test that export selects proper number of rows
        nrows = export(context=c, table_name=table_name,
          out_file=out_file, replace=1,
          columns=columns, where=where, orderby=orderby)
        self.assertEqual(nrows,45, 
          "Rather than the expected 45, nrows is %d." % nrows)
        

if __name__ == '__main__':
    unittest.main()
