import inspect
import datetime
from collections import OrderedDict
from airassessmentreporting.datacheck.dataset import Dataset, data
from airassessmentreporting.studyglue.study import Study
from airassessmentreporting.studyglue.studytools import (hvp2bcp, tvp2bcp) 

class StudyContext(object):
    def __init__(self, 
        study_name=None, 
        cvsroot="C:/CVSProjects/",
        tdroot = "C:\\Users\\temp_rphillips\\",
        run_idbms='csv', run_odbms='hvp', 
        new_data=True, grade='10', form='A', 
        sagtable=None,
        verbosity=None):
        """
        The high level context in which to create a Study() and run it.
        
        See the end of this file for an example of how to run a Study() via
        a StudyContext().
        
        The study() uses as input the output, or a formatted version of it, 
        of the complementery merge process.
        
        The source data for the input data is typically in a table name like 
        rc1final_cmrg or rc2final_cmrg.
        
        The study() outputs various datasets including 
        student_aggregation,  state, district, school, school intervention.
        
        Parameters control determination of 
        (1) input source datasets or databases, 
        (2) temporary and output directories, 
        (3) specification of input table names that may vary among clients, 
        (4) and other flexible StudyContext settings.
        
        Parameters:
        ===========
        study_name: string
        ------------------
        -- See set_db() for list of allowed study_names.
        
        run_idbms: string and run_odbms: string
        ----------------------------------------
        -- These are the dbms styles (See Dataset()) to use with the method 
           run().
        -- Depending on them, varying pre and post-processing services are
           performed by this code to setup data for a subsequent run(). 
           See run() for details.
        
        new_data: boolean
        -----------------
        -- Whether the input tables are newly changed data different from 
           the 'cached' data in 'csv' files in our temporary storage directory.
           
        verbosity: boolean string or integer
        ----------------------------
        -- Current code uses simple True or False values to determine
           whether to print output messages for development. 
        -- In production, verbosity should be False.
        
        NOTES:
        ======
    
    Initial implementation seems fastest from testing, and initally this 
    supports only combinations: 
    (1) run_idbms='csv' and run_odbms ='hvp
    (2) run_idbms='tvp' and run_odbms='tvp' 
    Possible near future:
    (-) run_idbms='pyodbc' 
    (-) run_idbms='pyodbc' and run_odbms='hvp"
       
    Notes:
    ======
    When only one run is anticipated for a set of input data it might be 
    faster to use 'pyodbc' (when implemented) than 'csv' or 'tvp' for idbms. 
    
    But when multiple runs on the same input are done, as in testing, pyodbc 
    connecting to SQL Server takes 10 seconds for each time and 'csv' 
    and 'tvp'do not incur this penalty for subsequent runs, so 
    run_idbms='tvp' is faster. 
  
    But for run_odbms it seems it is fastest to always use 'tvp' followed 
    by a subsequent  call to tvp2bcp, which does a Windows OS command-line 
    subprocess call to the bcp utility.
    
    Actually writing to 'tvp' style filesystem files and then 
    running the bcp utility to load those into SQL server database tables 
    has tested faster than directly writing to SQL Server 2012 database 
    tables using run_odbms='pyodbc'. 
    
    Perhaps future developmens with the SQL Server code or with the Python 
    pyodbc module use will counteract this puzzling performance pheneomenom, 
    and then it will be faster to use 'pyodbc' for both idbms and odbms to 
    make it faster. 

        """
        iam = inspect.stack()[0][3]

        requireds = ['cvsroot', 'tdroot','study_name', 'run_idbms','run_odbms']
        if (cvsroot is None or study_name is None or run_idbms is None 
            or run_odbms is None or tdroot is None):
            for rparam in requireds:
                if rparam is None:
                    raise ValueError(
                      "%s:Param '%s' is missing. %s are all required"
                      % (iam, rparam, repr(requireds) ))
        self.tdroot = tdroot        
        if verbosity:
            print (
              "%s:got study_name=%s, cvsroot=%s, tdroot=%s, run_idbms=%s, "
              "run_odbms=%s\n"
              % (iam, study_name, cvsroot, tdroot, run_idbms, run_odbms))
        self.verbosity = verbosity    
        self.study_name = study_name
        self.cvsroot = cvsroot
        self.run_idbms = run_idbms
        self.run_odbms = run_odbms
        self.grade = grade
        self.form = form
        self.delimiter = '\t' 
        self.delimiter_suffix = '.tsv'
        self.new_data = new_data
        self.sagtable = sagtable
                
        #Call set_db 
        self.set_db()
        
        # Modify iodbmses here as more combos are supported...
        self.iodbmses = [ ('csv','hvp'), ('tvp','tvp')]
        if  (self.run_idbms, self.run_odbms) not in self.iodbmses:
            raise ValueError(
              "%s: idbms=%s, odbms=%s combination  must be in %s\n"
              % (iam,self.run_idbms, self.run_odbms, repr(self.iodbmses)))
        
        return
    # end init
    
    def set_db(self, verbosity=0):
        """
        For  study_name, set server, db, tddir.
        
        Also set self.od_table_copy: 
        Key is table name, value is 2-tuple of [0] output filename and 
        [1] progress chunksize to report.
        
        """
        iam = inspect.stack()[0][3]

        study_names=("OGT Summer 2012", "OGT Summer 2012_tsv"
          ,"OGT Fall 2012", "OGT Spring 2012")
          
        study_name = self.study_name
        ds = self.delimiter_suffix
        delimiter = self.delimiter   
        if verbosity:
            print (
              "%s:Setting db params for study_name='%s'" 
              % (iam, study_name))
        """
        # Here's a possible od_table_copy dictionary here for reference 
        # in case we want to do items table copying too. We may want to 
        # also copy items here.
        # For MC items tables, when reporting rows copied, use chunk of 50000
        # because there are about 30 items rows for every '*_cmrg row, and
        # it may have up to 200k or more rows.
        
        rcitems = 50000
       
        od_table_copy  = OrderedDict({
               'rc1final_cmrg' : ("study_input%s" % ds, 5000)
              ,'mc_table_c' : ('mc_table_c%s' % ds, rcitems)
              ,'mc_table_m' : ('mc_table_m%s' % ds, rcitems)
              ,'mc_table_r' : ('mc_table_r%s' % ds, rcitems)
              ,'mc_table_s' : ('mc_table_s%s' % ds, rcitems)
              , 'mc_table_w' : ('mc_table_w%s' % ds, rcitems)
              })
        """
        hl="\n------------------------------------------------\n"
        home = self.tdroot
        self.input_table_name='rc2final_cmrg'
        if verbosity:
            print ("%s%s:Setting study_name='%s' with delimiter='%s',"
               "suffix=%s\n%s" 
               % (hl, iam, study_name, delimiter, ds, hl))
        
        if study_name == "OGT Summer 2012":
            server ='38.118.83.61'
            tddir = home+"testdata/OGT_2012_Summer/"
            bcpdir = home+"testdata\\OGT_2012_Summer\\"
            db = 'python_OGT_12SU'
            # For summer, different input_table_name
            self.input_table_name='rc1final_cmrg'
            od_table_copy = OrderedDict({
               'rc1final_cmrg' : ("study_input%s" % ds, 5000)
               })
        
        elif study_name == "OGT Fall 2012":
            server ='38.118.83.61'
            tddir = home+"testdata/OGT_2012_Fall/"
            bcpdir = home+"testdata\\OGT_2012_Summer\\"

            db = 'python_OGT_12FA'
            od_table_copy = OrderedDict({
               'rc2final_cmrg' : ("study_input%s" % ds, 5000)
               })
            
        elif study_name == "OGT Spring 2012":
            server ='38.118.83.61'
            tddir = home+"testdata/OGT_2012_Spring/"
            bcpdir = home+"testdata\\OGT_2012_Summer\\"

            db = 'python_OGT_12SP'
            od_table_copy = OrderedDict({
               'rc1final_cmrg' : ("study_input%s" % ds, 5000)
               })
        
        else:
            raise ValueError(
              "%s:study_name='%s' is unknown. Study_names=%s" 
              % (iam, study_name, repr(study_names)))
            
        self.server = server
        self.tddir = tddir
        self.bcpdir = bcpdir
        self.db = db
        self.od_table_copy = od_table_copy
        self.od_column_type = None
        
        return 
    # end set_db()
    
    def study_db_to_cache(self, verbosity=None):
        """
        COPY Study TABLE(s) TO files to be used as input for a study().
        
        Note: takes 9 secs to connect to remote db, then a few more to copy 
        the table.
        """
        iam = "study_db_to_csv()"
        
        study_name = self.study_name
        server = self.server
        db = self.db
        tddir = self.tddir
        delimiter = self.delimiter
        od_table_copy = self.od_table_copy
        if verbosity is None:
            verbosity = self.verbosity
        
        now = datetime.datetime.now()  
        if study_name is None or od_table_copy is None:
            raise ValueError("study_name and od_table_copy must be given")
        if verbosity:
            print ( 
              "%s:now=%s:\nCopying tables to input files for study_name='%s'" 
              % (iam, now, study_name)  )
       
        # Run the dataset copying functions...
        # copy from pyodbc to csv 
        conn = None
        for dt, copy in od_table_copy.items():
            fn = copy[0]
            rows_chunk = copy[1]
            if not conn:
                dsr = Dataset(open_mode='rb',dbms='pyodbc',table=dt, 
                        server=server, db=db)

            else:
                dsr = Dataset(open_mode='rb',dbms='pyodbc',table=dt, 
                        conn=conn)
            dsw = Dataset(open_mode='wb', dbms=self.run_idbms, 
              name="%s%s" % (tddir,fn),  delimiter=delimiter)
            now = datetime.datetime.now()   
            if verbosity:
                now = datetime.datetime.now()
                print (
                  "%s: now=%s:\nCalling data(): dsr=%s, dsw=%s, delimiter=%s" 
                  % (iam, now, repr(dsr), repr(dsw), repr(delimiter)))
                
            data(dsr=dsr, dsw=dsw, od_column_type=None
             ,verbosity=verbosity, rows_chunk=rows_chunk)
            
            #dsr now has a connection, so save it
            if not conn:
                conn = dsr.conn
            
            if verbosity:
                now = datetime.datetime.now()            
                print (  "\n%s:now=%s:\n"
                  "Return from data(), dt=%s, used dsr=%s, dsw=%s\n" % 
                  (iam, now, dt, dsr, dsw) )
        #end: for dt, copy in od_table_copy.items():
        
        if verbosity:    
            now = datetime.datetime.now()   
            print ( 
              "%s:now=%s:\nDONE copying tables to input files for "
              "study_name='%s'\n\tod_dt_fn=%s" 
              % (iam, now, study_name,repr(od_table_copy))  )
        
    #end def study_db_to_csv()
    
    def run_prepare(self):
        """
        Set up data, datasets, for a new study.
        """
        iam = inspect.stack()[0][3]
        if (self.run_idbms, self.run_odbms) not in self.iodbmses:
            raise ValueError(
              "%s: self.run_idbms=%s, self.run_odbms=%s combination  "
              "must be in %s\n"  
              % (iam, self.run_idbms, self.run_odbms, repr(self.iodbmses)))
        study_name = self.study_name
        home = self.tdroot
        verbosity = self.verbosity
        if self.run_idbms in ('csv','tvp') :
            if self.new_data:
                # New_data is in the database. Dump the SQL Server input 
                # tables into local csv files (tab delimited) for reading 
                # by study()
                self.study_db_to_cache()
                        
            # Create datasets to use for a new study. 
            # Here, for csv, inputs are based on the csv files in tddir.

            cvsroot = self.cvsroot
            self.cvspath=( 
              "%s/CSSC Score Reporting/OGT Spring 2012/Code/Development/"
              % self.cvsroot)
            
            if self.study_name == 'OGT Spring 2012':
                self.admin="Spring"
                self.tddir = self.tdroot+"testdata/OGT_2012_Spring/" 
            
                self.fn_layout=(
                    cvsroot + 'CSSCScoreReporting2/OGT Spring 2012/' + 
                    "Input Layout/OGT_SP12_Op_DataLayout_IntakeLayout.xls")
                
                #glue.sas lines 26-29
                self.fn_bookmap_loc_xls = (
                  cvsroot + 'CSSCScoreReporting2/OGT Spring 2012/' +
                  "Input Layout/OGT_SP12_Op_DataLayout_BookmapLocations.xls") 
            
                self.bml_grade_column = 'grade'
                self.bml_location_column = 'bookmap_location'
                self.bml_form_column='form'
                self.odict_loc_subs=None
               
                self.fn_aggregation_xls=( 
                  cvsroot + "CSSCScoreReporting2/OGT Spring 2012/" 
                  + "Code/Development/Superdata/AggregationSheet.xls")
                
            elif self.study_name == "OGT Fall 2012":
                self.admin="Fall"
                self.tddir = home+"testdata/OGT_2012_Fall/"
            
                self.fn_layout=(
                    cvsroot + 'CSSC Score Reporting/OGT Fall 2012/' + 
                    "Intake Layout/OGT_FA12_Op_DataLayout_IntakeLayout.xls")
                #glue.sas lines 26-29
                self.fn_bookmap_loc_xls = (cvsroot +
                  "CSSC Score Reporting/OGT Fall 2012/Code/Development/"
                  "Intake/BookMapLocations1.xls")
                self.bml_grade_column = 'grade_values'
                self.bml_location_column = 'location'
                self.bml_form_column='form_values'
                self.odict_loc_subs=None
                     
                self.fn_aggregation_xls=( cvsroot +
                  "CSSC Score Reporting/OGT Fall 2012/Code/Development/"
                  "Superdata/AggregationSheet.xls")  
                
            elif study_name == "OGT Summer 2012":
                self.admin="Summer"
                self.tddir = home+"testdata/OGT_2012_Summer/"
                self.fn_layout=(
                    cvsroot + 'CSSC Score Reporting/OGT Summer 2012/' + 
                    "IntakeLayout/OGT_SU12_Op_DataLayout_IntakeLayout.xls")
                #glue.sas lines 26-29
                self.fn_bookmap_loc_xls = (cvsroot +
                  "CSSC Score Reporting/OGT Summer 2012/Code/Development/"
                  "Intake/BookMapLocations1.xls")
            
                self.bml_grade_column = 'grade_values'
                self.bml_location_column = 'location'
                self.bml_form_column='form_values'
                self.odict_loc_subs=OrderedDict({ 
                  '&bkmap_breach_basepath.': 
                  "H:/share/Ohio Graduation Tests/Test Development/"
                  "Bookmaps and 1x1s/Breach 2012"
                  })
                self.fn_aggregation_xls=( cvsroot +
                  "CSSC Score Reporting/OGT Spring 2012/Code/Development/"
                  "Superdata/AggregationSheet.xls")
                
                # Aggregation sheet -- we reuse Spring.
                self.fn_aggregation_xls=( 
                  cvsroot + "CSSCScoreReporting2/OGT Spring 2012/" 
                  + "Code/Development/Superdata/AggregationSheet.xls")
            
            else:
                raise ValueError( 
                    "ERROR: Study_name '%s' unknown" % study_name)
                
            # Commonly derived settings
            self.dsr_layout = Dataset(
              open_mode='rb', dbms="excel_srcn", workbook_file=self.fn_layout)
            
            self.dsr_bookmap_locs = Dataset(open_mode='rb', dbms='excel_srcn', 
                workbook_file=(self.fn_bookmap_loc_xls) )
            self.fn_input="study_input%s" % self.delimiter_suffix
            self.dsr_input = Dataset(
              open_mode='rb', dbms=self.run_idbms, delimiter=self.delimiter, 
              name="%s%s" % (self.tddir, self.fn_input ) )
               
            if verbosity:
                print ("%s: Study '%s' is set to run with fn_input='%s'" 
                   % (iam,self.study_name, self.fn_input))
                now = datetime.datetime.now()
                print "Now = " ,now  
        # end  if run_idbms == 'csv'
        
        # process run_odbms 
        
        odbms = self.run_odbms
        tddir = self.tddir
        delimiter = self.delimiter
        ds = self.delimiter_suffix
        if odbms in ( 'hvp','tvp') :
            self.dsw_galludet = Dataset(open_mode="wb", dbms=odbms
                , name=tddir+"ogt_galludet%s" % ds
                , delimiter=delimiter, replace=True)
            self.dsw_student  = Dataset(open_mode="wb", dbms=odbms
                , name=tddir+"student%s" % ds
                , delimiter=delimiter, replace=True)
            self.dsw_student_dummy = Dataset(open_mode="wb", dbms=odbms
                , name=tddir+"student_dummy%s" % ds
                , delimiter=delimiter, replace=True)     
            self.dsw_district  = Dataset(open_mode="wb", dbms=odbms
                , name=tddir+"district%s" % ds
                , delimiter=delimiter ,replace=True)
            self.dsw_state = Dataset(open_mode="wb", dbms=odbms
                , name=tddir+"state%s" % ds
                , delimiter=delimiter ,replace=True)
            self.dsw_school = Dataset(open_mode="wb", dbms=odbms
                , name=tddir+"school%s" % ds
                , delimiter=delimiter ,replace=True)
            self.dsw_school_intervention = Dataset(open_mode="wb", dbms=odbms
                , name=tddir+"school_intervention%s" % ds
                , delimiter=delimiter, replace=True)
        # end if run_odbms in ( 'hvp', 'tvp')
        return
    # end def run_prepare()
        
    def run(self,new_data=None,verbosity=None):
        """
        """
        iam = inspect.stack()[0][3]
        if verbosity is None:
            verbosity = self.verbosity
        if new_data is not None:
            #allow reset of new_data via this optional run param
            self.new_data = new_data
   
        # Do any required run preparation of data.
        self.run_prepare()
        
        # Create the study.
        self.study = Study( 
          study_name=self.study_name, 
          admin=self.admin, grade=self.grade, form=self.form,
          dsr_bookmap_locs=self.dsr_bookmap_locs,
          bml_grade_column=self.bml_grade_column,
          bml_form_column=self.bml_form_column,
          bml_location_column=self.bml_location_column,
          odict_loc_subs=self.odict_loc_subs,
          fn_aggregation_xls=self.fn_aggregation_xls,
          dsr_input=self.dsr_input,
          dsr_layout=self.dsr_layout,
          
          dsw_galludet=self.dsw_galludet,
          dsw_student=self.dsw_student,
          dsw_district=self.dsw_district, dsw_state=self.dsw_state,
          dsw_school=self.dsw_school, 
          dsw_school_intervention=self.dsw_school_intervention,
          verbose=self.verbosity
          )
        now = datetime.datetime.now()
        print "%s:Running study_name=%s at %s" % (iam, self.study_name,now)
        
        # Run study analysis. Now OGT is first client implemented. 
        # When finish other clients, can put if-elif clauses here instead.
        self.study.ogt_student(verbose=1) 
        #Try to close files so they can be re-opened in windows subprocesses
        try:
            if self.dsw_student.dbms == 'hvp' or self.dsw_student.dbms=='tvp':
                self.dsw_student.csvfile.close()
        except:
            pass
        del (self.dsw_student, self.dsw_galludet, 
             self.dsw_district, self.dsw_state, self.dsw_school, 
             self.dsw_school_intervention)
 
        now = datetime.datetime.now()
        print ("%s:Finished main analysis for study_name=%s at %s" 
           % (iam, self.study_name,now))

        # Post processing
        now = datetime.datetime.now()
        print ("%s:Posting output for study_name=%s at %s " 
          % (iam, self.study_name,now))
        self.run_post()
        
        now = datetime.datetime.now()
        print "%s:Done running %s at %s" % (iam, self.study_name,now)
        
        return
        
    def run_post(self):
        """
        Upload study output datasets into db tables if needed.
        
        If output of study() run is in the 'hvp' style, then we must upload 
        the Datasets in these pairs of files to target destination db tables.
        """
        iam = inspect.stack()[0][3]
     
        if self.run_odbms == "hvp":
            # study() output Datasets are in hvp output file pairs. 
            # Load them into the target database.
            
            #Student aggregation - sag - set default and specific target 
            # db column types
            sag_defcolspec='nvarchar(24) null'
            od_sagcolumn_spec=OrderedDict({
                   'grade': 'float'
                  ,'id': 'int'
                 # ,'upcx_score': 'float'
                  , 'dob_day': 'float'
                  , 'dob_month' : 'float'
                  , 'dob_year': 'float'
                  , 'preidflag': 'float'
                  , 'ucxx_admin_date_mo' : 'float'
                  , 'ucxx_admin_date_yr' : 'float'
                  , 'ufcx_attempt' : 'float'
                  , 'ufcx_breach' : 'float'
                  , 'ufcx_invalid': 'float'
            
                  , 'ufsx_attempt' : 'float'
                  , 'ufsx_breach' : 'float'
                  , 'ufsx_invalid': 'float'
                  
                  , 'ufmx_attempt' : 'float'
                  , 'ufmx_breach' : 'float'
                  , 'ufmx_invalid': 'float'
                  
                  , 'ufrx_attempt' : 'float'
                  , 'ufrx_breach' : 'float'
                  , 'ufrx_invalid': 'float'
                  
                  , 'ufwx_attempt' : 'float'
                  , 'ufwx_breach' : 'float'
                  , 'ufwx_invalid': 'float'
                  
                  , 'ufxx_sample' : 'float'
                  , 'ufxx_test_type' : 'float'
                  , 'upcx_dictionary' : 'float'
                  ,'inclusionflagr': 'float'
                  ,'inclusionflagm': 'float'
                  ,'inclusionflagw': 'float'
                  ,'inclusionflags': 'float'
                  ,'inclusionflagc': 'float'
                  ,'stateinclusionflag': 'float'
                  ,'districtinclusionflag': 'float'
                  ,'complementary_merge_report': 'nvarchar(256)'
                  ,'dcrxnm_attend': 'nvarchar(256)'
                  ,'bcrxnm_attend': 'nvarchar(256)'
                  ,'dcrxnm': 'nvarchar(256)'
                  ,'bcrxnm': 'nvarchar(256)'
                  ,'dcrxnm_home': 'nvarchar(256)'
                  ,'bcrxnm_home': 'nvarchar(256)'
                  ,'ucmx_teachername': 'nvarchar(40)'
                  ,'ucwx_teachername': 'nvarchar(40)'
                  ,'ucrx_teachername': 'nvarchar(40)'
                  ,'ucsx_teachername': 'nvarchar(40)'
                  ,'uccx_teachername': 'nvarchar(40)'
                  ,'dob': 'nvarchar(24)'
                  ,'ucrx_preid': 'nvarchar(24)'
                  ,'student_name': 'nvarchar(64)'
                  ,'birthdate': 'nvarchar(24)'
                  ,'uccx_coursecode': 'nvarchar(24)'
                  ,'ucrx_coursecode': 'nvarchar(24)'
                  ,'ucsx_coursecode': 'nvarchar(24)'
                  ,'ucmx_coursecode': 'nvarchar(24)'
                  ,'ucwx_coursecode': 'nvarchar(24)'
                  ,'dcxx_county' : 'nvarchar(32)'
                  ,'ucrxlnm' : 'nvarchar(32)'
                  ,'ucrxfnm' : 'nvarchar(32)'
                  ,'upmnlev' : 'nvarchar(32)'
                  ,'upspscal': 'nvarchar(32)'
                  ,'proforhigherr' : 'float'
                  ,'advaccr' : 'float'
                  ,'proforhigherm' : 'float'
                  ,'advaccm' : 'float'
                  ,'proforhigherw' : 'float'
                  ,'advaccw' : 'float'
                  ,'proforhighers' : 'float'
                  ,'advaccs' : 'float'
                  ,'proforhigherc' : 'float'
                  ,'advaccc' : 'float'
                  ,'dummy_record_flag': 'nvarchar(5)'
            
                  })
            
            # Define dict with key as basename of each hvp pair to load to 
            # a table, and value is 3-tuple of: destination table name, 
            # default table column spec, and dict of any table column spec 
            # overrides.
            sagfilebase = "student" 
            sagtable = ( self.sagtable if self.sagtable is not None 
                        else "student_aggregation" )
            od_basename_tableinfo = OrderedDict({
              sagfilebase : (
                sagtable, sag_defcolspec, od_sagcolumn_spec ) 
              })
            # Delete dsw.student so bcp can read it to upload it.
            # Upload to database tables from the paired files for the hvp 
            # basenames.
            # NOTE: BCP will fail with error S1000 if dsw_student (or its 
            # file) has not been closed first; and this particular error 
            # will be in the 'out' file, not the errors.txt output file.
            hvp2bcp(server=self.server, db=self.db
              , inputsdir=self.bcpdir, outputsdir=self.bcpdir
              , verbosity=self.verbosity
              , od_basename_tableinfo=od_basename_tableinfo)
            
        elif self.run_odbms == 'tvp':
            # study() output Datasets are in tvp output file pairs. 
            # Load them into the target database.
            
            #Student aggregation - sag - set default and specific target 
            # db column types
            
            od_sagcolumn_spec=OrderedDict({
                   'grade': 'float'
                  ,'id': 'int'
                 # ,'upcx_score': 'float'
                  , 'dob_day': 'float'
                  , 'dob_month' : 'float'
                  , 'dob_year': 'float'
                  , 'preidflag': 'float'
                  , 'ucxx_admin_date_mo' : 'float'
                  , 'ucxx_admin_date_yr' : 'float'
                  , 'ufcx_attempt' : 'float'
                  , 'ufcx_breach' : 'float'
                  , 'ufcx_invalid': 'float'
            
                  , 'ufsx_attempt' : 'float'
                  , 'ufsx_breach' : 'float'
                  , 'ufsx_invalid': 'float'
                  
                  , 'ufmx_attempt' : 'float'
                  , 'ufmx_breach' : 'float'
                  , 'ufmx_invalid': 'float'
                  
                  , 'ufrx_attempt' : 'float'
                  , 'ufrx_breach' : 'float'
                  , 'ufrx_invalid': 'float'
                  
                  , 'ufwx_attempt' : 'float'
                  , 'ufwx_breach' : 'float'
                  , 'ufwx_invalid': 'float'
                  
                  , 'ufxx_sample' : 'float'
                  , 'ufxx_test_type' : 'float'
                  , 'upcx_dictionary' : 'float'
                  ,'inclusionflagr': 'float'
                  ,'inclusionflagm': 'float'
                  ,'inclusionflagw': 'float'
                  ,'inclusionflags': 'float'
                  ,'inclusionflagc': 'float'
                  ,'stateinclusionflag': 'float'
                  ,'districtinclusionflag': 'float'
                  ,'complementary_merge_report': 'nvarchar(128)'
                  ,'dcrxnm_attend': 'nvarchar(129)'
                  ,'bcrxnm_attend': 'nvarchar(130)'
                  ,'dcrxnm': 'nvarchar(131)'
                  ,'bcrxnm': 'nvarchar(132)'
                  ,'dcrxnm_home': 'nvarchar(256)'
                  ,'bcrxnm_home': 'nvarchar(256)'
                  ,'ucmx_teachername': 'nvarchar(40)'
                  ,'ucwx_teachername': 'nvarchar(40)'
                  ,'ucrx_teachername': 'nvarchar(40)'
                  ,'ucsx_teachername': 'nvarchar(40)'
                  ,'uccx_teachername': 'nvarchar(40)'
                  ,'dob': 'nvarchar(24)'
                  ,'ucrx_preid': 'nvarchar(24)'
                  ,'student_name': 'nvarchar(64)'
                  ,'birthdate': 'nvarchar(24)'
                  ,'uccx_coursecode': 'nvarchar(24)'
                  ,'ucrx_coursecode': 'nvarchar(24)'
                  ,'ucsx_coursecode': 'nvarchar(24)'
                  ,'ucmx_coursecode': 'nvarchar(24)'
                  ,'ucwx_coursecode': 'nvarchar(24)'
                  ,'dcxx_county' : 'nvarchar(32)'
                  ,'ucrxlnm' : 'nvarchar(32)'
                  ,'ucrxfnm' : 'nvarchar(32)'
                  ,'upmnlev' : 'nvarchar(32)'
                  ,'upspscal': 'nvarchar(32)'
                  ,'proforhigherr' : 'float'
                  ,'advaccr' : 'float'
                  ,'proforhigherm' : 'float'
                  ,'advaccm' : 'float'
                  ,'proforhigherw' : 'float'
                  ,'advaccw' : 'float'
                  ,'proforhighers' : 'float'
                  ,'advaccs' : 'float'
                  ,'proforhigherc' : 'float'
                  ,'advaccc' : 'float'
                  ,'dummy_record_flag': 'nvarchar(5)'
            
                  })
            
            # Define dict with key as basename of each hvp pair to load to 
            # a table, and value is 3-tuple of: destination table name, 
            # default table column spec, and dict of any table column spec 
            # overrides.
            sagfilebase = "student" 
            sagtable = ( self.sagtable if self.sagtable is not None 
                        else "student_aggregation" )
            od_basename_tableinfo = OrderedDict({
              sagfilebase : (
                sagtable,  od_sagcolumn_spec ) 
              })
            # Delete dsw.student so bcp can read it to upload it.
            # Upload to database tables from the paired files for the hvp 
            # basenames.
            # NOTE: BCP will fail with error S1000 if dsw_student (or its 
            # file) has not been closed first; and this particular error 
            # will be in the 'out' file, not the errors.txt output file.
            tvp2bcp(server=self.server, db=self.db
              , inputsdir=self.bcpdir, outputsdir=self.bcpdir
              , verbosity=self.verbosity
              , od_basename_tableinfo=od_basename_tableinfo)            
        # end: elif run_odbms == 'tvp'
                  
        if self.verbosity:
            now = datetime.datetime.now()
            print "%s: Done at %s" % (iam, now) 
    
        return
    # end def run_post()
    
if  __name__ == "__main__" and 1 == 1:
    # TEST StudyContext
    sagtable="student_aggregation_20130930"
    sagtable=None #defaults to 'student_aggregation'
    
    #NB: Must use new_data = True once at start of receiving new data in
    # the db or after changing run_idbms. Then use 'false' to speed up
    # rerunning a study when no new data has been received in the db.
    sc = StudyContext(
        study_name="OGT Summer 2012", 
        run_idbms='tvp',
        run_odbms='tvp',
        verbosity=1, new_data=False,
        sagtable=sagtable)
     
    sc.run(verbosity=1)
    pass