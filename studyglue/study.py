import csv
import string
import datetime as datetime
import re
from airassessmentreporting.datacheck.dataset import *
from airassessmentreporting.studyglue.studytools import *

class Study(object):
    """ See init
    """
    def __init__( self, 
      study_name="OGT TEST STUDY", 
      dsr_bookmap_locs=None, 
      odict_loc_subs=None,
      bml_grade_column = 'grade',
      bml_form_column = 'form',
      bml_location_column='location',
      admin=None, grade=None, form=None, 
      fn_aggregation_xls=None,
      dsr_input=None,
      dsr_layout=None,
      dsw_galludet=None, dsw_student=None, dsw_student_dummy=None,
      dsw_district=None, dsw_state=None,
      dsw_school=None, dsw_school_intervention=None,
      missing_value=None,
      verbose=None):
        """
        Output dsw_student 
        
        Note: That outupt dataset is named student_aggregation in OGT 
        code...superdata/glue.sas.
        
        Also other output datasets are created here.
        
        Parameters:
        ===========
        
        study_name : string, optional
        -----------------------------
        Simple string for the study name, optional.
        
        dsr_bookmap_locs: dataset
        -------------------------------------
        - dsr of the  'bookmap locations' workbook sheet      
        
        grade: integer, optional
        -------------------------
        
        - Grade to study. Value is from 10-15 or 99
        
        form: string
        ------------
        - Should match a from value in the bookmap(s) 
        
        dsr_input : Dataset
        -------------------
        
        - a dataset like OGT_RC2_FINAL for Ohio Graduation Tests, in that it has the
          basic column names that are sought by this code, and it has data ready to 
          be processed, of course.
          
        dsr_layout : Dataset
        --------------------
        
        - a dataset with the columns expected in a time-honored 'layout' excel 
          worksheet.
    
        dsw_galludet : Dataset
        ----------------------
        - special output for Galludet, as done in glue.sas
        
        dsw_student: Dataset
        --------------------
        - The primary output. Was called student_aggregation in glue.sas
        
        dsw_district, dsw_state, dsw_school: Datasets
        ---------------------------------------------
        
        - These mimic the output datasets, again, of glue.sas that are used by 
          statistical compuations and ascii report writer.
        
        Extended Summary:
        =================
        Read various inputs 
        Study object has methods like ogt_student to prepare OGT data for 
        ttest(), Means(), peer_compare(), cascade(), etc.
        
        Issue: iron out the common usages for params grade, form, admin. 
        Should they always be specified by caller, and what about 
        subject?
        
        Notes:
        ======
        - The parameters roughly parallel those used by glue.sas. 
        - This code emulates much of what is done by OGT 2012 code/superdata/glue.sas
          to prepare datasets/tables for procesing by other statistics functions.
        
        """
        # See input parameters in glue.sas, bkmap and also see lines 33-37. Validate them.
        iam = "Study init()"
        self.study_name = study_name
        csv.field_size_limit(256789)
        
        expected = ("spring", "summer", "fall")
        if (admin is None):
            admin = "spring"
        elif admin.lower() not in expected:
            raise ValueError(
              "%s: Form value '%s' is not in expected list %s." 
              % (iam,form,expected))
        admin = admin.lower()     
            
        expected= ('A','B','1','2')
        if (form is None):
            form = 'A'
        elif form.upper() not in expected:
            raise ValueError(
              "%s: Form value '%s' is not in expected list %s." 
              % (iam,form,expected))
        form = form.upper()

        expected = ('10','11','12','13','14','15')    
        if (grade is None):
            grade='10'
        elif grade.upper() not in expected:
            raise ValueError(
              "%s: Form value '%s' is not in expected list %s." 
              % (iam,form,expected))
        
        if verbose:
            print (
              "%s: Params:\n\tname=%s, admin='%s',grade='%s',form='%s'\n"
              "\tdsr_bookmap_locs='%s'" %
              ( iam, study_name, admin, grade, form, 
                repr(dsr_bookmap_locs) ))
        self.dsw_district = dsw_district
        self.dsw_state = dsw_state
        self.dsw_school = dsw_school
        self.dsw_school_intervention = dsw_school_intervention
        self.admin = admin
        self.grade = grade
        self.form = form
        
        self.dsr_bookmap_locs = dsr_bookmap_locs
        self.bml_grade_column = bml_grade_column
        self.bml_form_column = bml_form_column
        self.bml_location_column = bml_location_column
        self.odict_loc_subs = odict_loc_subs
        self.fn_aggregation_xls = fn_aggregation_xls
        self.dsr_input = dsr_input
        self.dsr_layout = dsr_layout
        self.dsw_galludet = dsw_galludet
        self.dsw_student = dsw_student
        if self.dsw_student.dbms.lower().startswith('pyodbc'):
            self.student_column_specs = True
        else:
            self.student_column_specs = False
        self.dsw_student_dummy = dsw_student_dummy
        self.missing_value = missing_value
        self.verbose = verbose
              
        self.bookmap_loc_required_column_names = [
           'location', 'grade', 'grade_values', 'subject', 
           'subject_values', 'form_values' ]
        # Add code to read Bookmaps to set and return info of glue.sas-like vars for 
        # MC_item.. 
        # see annotated glue code - add here or just before using 
    
        # Read aggregation sheets, as they are small,  into dictionaries. 
        # Remember sheet names are case-senstive, and the names with case differences
        # here match the data we have (so far).
        # May need to implement a case-insensitve sheet_name matching if these exact 
        # sheet_names do not endure unchanged over future administrations.
        # First, read the agg info sheet into a small dict for later use. 
        # SAS glue.sas read it into a Dataset line 44. It is 5 rows small, so a 
        # dict is faster.
        # 
        self.od_agg_subject_info = DictExcelSheet(file_name=fn_aggregation_xls, 
          sheet_name="Info",keys=['subject'],verbose=1)
        if self.verbose:
            print ("Read %s items into od_agg_subject_info" 
                  % len(self.od_agg_subject_info) )
        # create contentstrand initials list per subject
        # see glue.sas line 64-67: 
        # Here it is done via self.dict_agg_subject_info
        
        for subject, info in self.od_agg_subject_info.items(): 
            info['contentstrand'] = info['contentstrand'].lower().split(" ")
            
        # Read agg means sheet into odict, use row number as key as data has no 
        # key column.
        # glue.sas line 45 reads it into a DS.
        self.od_agg_id_means = DictExcelSheet(file_name=fn_aggregation_xls, 
          sheet_name="Means",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_means" 
                   % len(self.od_agg_id_means) )
        
        self.od_agg_id_percent = DictExcelSheet(file_name=fn_aggregation_xls, 
          sheet_name="Percent",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_percent" 
                   % len(self.od_agg_id_percent) )
    
        self.od_agg_id_ttest = DictExcelSheet(file_name=fn_aggregation_xls, 
          sheet_name="Ttest",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_ttest" 
                   % len(self.od_agg_id_ttest) )
        
        self.od_agg_id_cascade = DictExcelSheet(file_name=fn_aggregation_xls, 
          sheet_name="cascade",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_cascade" 
                   % len(self.od_agg_id_cascade) )
     
        self.od_agg_id_peer = DictExcelSheet(file_name=fn_aggregation_xls, 
          sheet_name="PeerCompare",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_peer" 
                   % len(self.od_agg_id_peer) )
    
        self.od_agg_id_suppression_district = DictExcelSheet(
          file_name=fn_aggregation_xls, 
          sheet_name="Suppression_district",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_suppression_district" 
              % len(self.od_agg_id_suppression_district) )
      
        self.od_agg_id_suppression_state = DictExcelSheet(
          file_name=fn_aggregation_xls, 
          sheet_name="Suppression_State",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_suppression_state" 
              % len(self.od_agg_id_suppression_state) )
        
        self.od_agg_id_suppression_school = DictExcelSheet(
          file_name=fn_aggregation_xls,
          sheet_name="Suppression_School",verbose=0)
        if self.verbose:
            print ("Read %s items into od_agg_id_suppression_school" 
              % len(self.od_agg_id_suppression_school) )
        
        self.dict_lvars_info = None
        # self.d_vtypes_varnames is populated by layout info.
        # the value is a dict d_varnames_vtype
        self.d_vtypes_varnames = {}
        
        return
    
    def nom_input_vars(self, fieldnames=None, verbosity=None):
        """ 
        Return var names that layout has as nominal and are also in given fieldnames.
        """
        # Prepare nom_var_list, list of Nominal and Nominal_ID1 variables for later 
        # processing which serves the function of varlist in glue.sas line 116:
        iam = "nom_input_vars"
        dr_layout = self.dsr_layout.DictReader()
        nom_input_var_names=[]
        nnvar = 0
        nlrow = 0
        d_vtypes_varnames = self.d_vtypes_varnames
        for row in dr_layout:
            nlrow += 1
            # Append to nom_var_list if apt
            vtype = row['type'].lower().strip()
            varname = row['variable_name'].lower()
            if vtype is not None and vtype != "" and varname in fieldnames:
                if verbosity > 1:
                    print "%s:vtype=%s, varname=%s" % (iam, vtype, varname)
                if d_vtypes_varnames.get(vtype, None) is None:
                    # New vtype, so create dictionary for its varnames
                    d_vtypes_varnames[vtype] = {}
                d_varnames_vtype = d_vtypes_varnames[vtype]
                if d_varnames_vtype.get(varname, None) is None:
                    # assume unique varnames so key error will reveal nondups.
                    if verbosity > 1:
                        print ("%s: adding key varname=%s, value vtype=%s"
                               % (iam, varname, vtype))
                    d_varnames_vtype[varname] = vtype
            
            if  (vtype == 'nominal' or vtype == 'nominal_id1'):
                if row['variable_name'].lower() in fieldnames:
                    nnvar +=1
                    nvname = row['variable_name'].lower()
                    if 1==2:
                        print ("row =%d, vtype='%s' Nominal var name = '%s'" 
                          % (nlrow, vtype, nvname))
                    nom_input_var_names.append(str(nvname))
                else:
                    #may later include option here to flag a warning or error
                    pass
        if verbosity:
            print("%s: Found %d nominal vars" % (iam, nnvar))

        return nom_input_var_names
    # end def nom_input_vars
    
    def get_input_reader(self):
        me = "get_input_reader"
        dsr_input = self.dsr_input
        dr_input = dsr_input.DictReader()
        input_required_columns = [
          'ufrx_attempt', 'ufmx_attempt', 'ufsx_attempt','ufcx_attempt'
        , 'ufwx_attempt'
        , 'ucrxfnm', 'studentmid', 'ucrxlnm'  
        , 'dob_month', 'dob_day', 'dob_year', 'schtype_attend'
        , 'distrtype_attend'
        , 'dcrxid_attend', 'bcrxid_attend', 'lithocode'
        , 'uprx_read_aloud','upmx_read_aloud','upsx_read_aloud'
        , 'upcx_read_aloud','upwx_read_aloud'
        , 'uprx_extended','upmx_extended','upsx_extended','upcx_extended'
        , 'upwx_extended'
        , 'uprx_dictionary','upmx_dictionary','upsx_dictionary'
        , 'upcx_dictionary','upwx_dictionary'
        , 'uprx_scribe','upmx_scribe','upsx_scribe','upcx_scribe'
        , 'upwx_scribe'
     #   , 'uprx_calculator','upmx_calculator','upsx_calculator','upcx_calculator'
     #   ,'upwx_calculator'
        , 'uprx_other','upmx_other','upsx_other','upcx_other','upwx_other'
        ]
        columns_missing = 0;
        for rqd_col in input_required_columns:
            if rqd_col not in dr_input.fieldnames:
                columns_missing = columns_missing + 1
                print("%s: Required column '%s' not in input dataset."
                  % (me, rqd_col))
            if (columns_missing > 0):
                raise ValueError("Missing %d columns from dataset %s" 
                  % (columns_missing, dr_input))
        return dr_input
    # end def get_input_reader
    
    def init_content_strands(self,verbose=None):
        """
        Read subjects content strands info from workbook agg sheet info.
        
        Per glue.sas line 60 - make d_s1_cs1, key is 1-letter subject abbrev
        and value is 1 letter content-strand abbreviation
        """
        me="init_content_strands"
        self.dict_subject_cstrands = OrderedDict()
        for subject, info in self.od_agg_subject_info.items():
            cs_list = info['contentstrand']
            self.dict_subject_cstrands[subject] = cs_list
            if verbose:
                print ("%s dict_subject_cstrands subject=%s, cs_list='%s'" 
                  % (me, subject, cs_list))
        return
    # end def init_content_strants
    
    def student_extra_columns(self,verbose=None):
        """
        Return 'extra' column names above normal input and 'galludet' columns.
        """
        me = "student_extra_columns"
        ocn = []
        
        od_column_type = {
             'myid': 'varchar(17)'              
            }
        # Note: for student, do not need to add ucrxfnm, as it is already in development test
        # input data.
        # For dsw_student, extend some names by hand here, and just below this, 
        # add more programmatically.
        # glue.sas lines 142-149 creates many of these columns, and the rest 
        # are scattered about in glue.sas
        # added elsewhere:'student_name', 'schtype', 'distrtype' ,'birthdate'
        ocn.extend([
           'myid'
          ,'bcrxid', 'bcrxnm', 'dcrxid','dcrxnm'
          ,'rgrade'
          ,'scrxid'
          ,'g_dcrxid', 'g_bcrxid', 'g_scrxid'
          ,'dcxx_county'
          ,'pass_count','reported_flag'
          ,'ucrotherdnp', 'ucmotherdnp', 'ucsotherdnp', 'ucwotherdnp'
          ,'uccotherdnp'
          ,'ucxxpon','attempt_all','writing_prompt1','writing_prompt2'
          ,'districtinclusionflag', 'stateinclusionflag'
          ])
        # Function student_cs_vars_add() does the work of glue.sas loop 
        # lines 244-253 
        # to create some content_strand-related output var names.
        ocn.extend(student_cs_vars_add(
          dict_subject_cstrands=self.dict_subject_cstrands, verbose=False))
        if verbose:
            print " student output_column_names=%s" % (repr(ocn))
        # Add dummy_record_flag
        ocn.extend([ 'dummy_record_flag' ])
        return ocn
    #def student_extra_columns
    
    def ogt_student(self, verbose=None):
        """ 
        Serve the purposes of SAS glue code lines 68 - 308, and beyond.
        
        Notes:
        ======
        This method emulates the OGT code/superdata/glue.sas beyond lines 308 
        there  because it: 
        (1) also generates output datasets for district, state, school, 
            school_intervention
            
          This function serves the purposes of these parts in glues.sas:
           - glue.sas lines 1-62
           - glue.sas lines 63-67 data step to read info_table
           - glue.sas lines 68-85: Data OGT_data output.ogt_Galludet;
           - glue.sas lines 86-118: Comments and convert_to_dot_missing()
           - glue.sas lines 119: ProperCase for names
           - glue.sas lines 120-138: NOTE: Commands and caller must pre-sort 
             dsr_input by lithocode
           - glue.sas lines 137-308: Data output Student
          
        -- NB: Not all intermediate output datasets are written by 
           ogt_student(), but they can easily be inserted as/when testing 
           reveals they are required.
            
        -- The main outputs produced now are datasets for 'student', 
           'galludet', 'district', state, school, school_intervention.
        
         - data step glue.sas lines 86 - 308: Data output student.
         
        -- Development reference inputds is in 
        libname input "H:\share\Ohio Graduation Tests\Technical\2012 October
          \ScoreReports\PostComplimentaryMergeData\ogt.sasb7dat";
          and looked at it via SAS 9.2 .. 60k rows of 1-2k columns.
          
        -- My input is now in the form of: 
        () a'flat table' and some associated subject-tables in SQL Server
        
        Overview:
        ---------
        From inputds, do: 
        --() set new column student_name to concat of other parts of student 
             name
        --() set new columns schtype and distrtype
        --() recode column ucrxfnm to removing blanks from input col of same 
             name
        --() set new column birthdate from other col vals
        --() delete rows where ufX*attempt is 0 for all subjects X.
        --(1) output dataset row to EITHER 
        --(1a) output ogt_galludet from all rows where bcrxid_attend or 
        dcrxid_attend in  ("000001","000002","000003") 
        -- (1b) output to ogt_data otherwise.
        those steps could be done prior to calling ogt, but might as will do 
        it here in one nice central place.
        
        
        Design Goal: 
        -------------
        To reduce slow io-bound passes through the big inputds dataset and 
        multiple byproduct datasets of it (as SAS glue.sas code does, by 
        nature), try to hold off until we can collect a lot of stuff that 
        can be filtered, recoded, etc in a single pass through the inputds.
  
        """
        verbose = 1
        me = "ogt_student()"
        missing_value = self.missing_value
        my_start = datetime.datetime.now()
        print ("%s: my_start time=%s:  . . ." 
                   % (me, my_start))
        
        self.init_content_strands(verbose=verbose)
        dr_input = self.get_input_reader()
        
        # Read the bookmaps into d_sgf where key is a tuple of (subject, grade, 
        # form) string values and value is a dictionary; where that 
        # dictionary has key of item (item or bookmap position in a bookmap) 
        # and value is another dictionary; with key of bookmap column name, 
        # where its value is string value from the bookmap sheet.  
        
        d_sgf = get_dict_sgf_bookmap(dsr_bookmaplocs=self.dsr_bookmap_locs, 
                column_grade=self.bml_grade_column,
                column_form=self.bml_form_column, 
                column_location=self.bml_location_column,
                odict_loc_subs=self.odict_loc_subs, verbose=verbose)
        
        # Per subject initial, grade, form: comprise named bookmap item values: 
        # items_mc_released (list of item ids) items_not_released (id list), 
        # max_item_num (integer).
        # Consider returning sets of ids instead of lists
        d_sgfl = get_dict_sgfl_bmids(d_sgf_bookmap=d_sgf, verbose=verbose)
          
        if verbose:
            print ( 
            "%s: using dsr_input='%s' with %d fieldnames, "
            "d_sgf with %d bookmaps" 
              % (me, self.dsr_input, len(dr_input.fieldnames), len(d_sgf)))
        # Set the out_column_names (ocn) in "ocn"  from input column names 
        # and requirements to create new ones on output for galludet.
        ocn = dr_input.fieldnames
        print ("dr_input.fieldnames = %s" % (repr(ocn)))
        # See glue.sas lines 73-79 for extra column names (beyond input columns)
        # for galludet output and student output
        ocn.extend(['student_name', 'schtype', 'distrtype' ,'birthdate'])
        
        # initialize dw_galludet now with current ocn column set.
        # ulist() cleans out any pesky dups that have crept into it via 
        # misc code revisions, preserving order as much as possible 
        # to aid visual output inspection.
        ocn = ulist(ocn)
        dw_galludet = self.dsw_galludet.DictWriter(column_names=ocn)
        dw_galludet.writeheader()
        
        # Add 'non-galludet' extra output columns required for the 
        # dsw_student output.
        ocn.extend(self.student_extra_columns())
        
        # Create student dict_writer. DictWriter(s) will write only 
        # column_names and they will ignore if extra key columns (with values)  
        # exist in the dict that is given to writerow, 
        # due to DictWriter() using setting extrasaction='ignore'.
        # This allows us to add extra columns to use in output
        # student_aggregation easily.
        # Note: use ulist() to remove duplicate names that may creep in 
        # with code updates.
        ocn=ulist(ocn)
        dw_student = self.dsw_student.DictWriter(column_names=ulist(ocn)
          ,od_column_type={"myid": 'varchar(17)'} )
        print ("\n-----------------------------------------\n"
          "dw_student COLUMN NAMES=%s\nocn='%s'" 
          % (repr(ulist(ocn)),repr(ocn)))
        dw_student.writeheader()
        # glue.sas line 206 outputs a row to student dummy right after it 
        # outputs a row to student, so it adds no custom column names, 
        # so we use the same ocn set here as for dw_student.
        if self.dsw_student_dummy:
            dw_student_dummy = self.dsw_student_dummy.DictWriter(
              column_names=ulist(ocn))
            dw_student_dummy.writeheader()
        else:
            dw_student_dummy = None
            
        # Add any new output columns for student aggregation here, before 
        # initializing its DictWriter with ocn.   
        
        # Glue lines 113-117:
        # Get the list of input variable names of type 'nominal and nominal*' 
        # per the layout.
        nom_input_var_names = self.nom_input_vars(
          fieldnames=dr_input.fieldnames, verbosity=verbose)
          
        # glue.sas line 32 (except we use lowercase)- 
        # Revise these to later 
        # get these from content_strands, so can vary by client or agg workbook
        subject_list = ['r', 'm', 'w', 's', 'c']
        # glue.sas line 172: unused in glue.sas? put it here for now.
        subject_list2 = ['r', 'm', 'w', 'sc', 'so']
        
        # Note: total execution may be a bit better by saving one dcut_union 
        # table for some or all of the columns of the four dcut tables, 
        # then after this  main input loop, scan that set of fewer rows 
        # and produce the other dcut_xxx objects from it. 
        # But the time savingsare probably on the order of 2-4 seconds tops for 
        # 100k input rows, so left to do as/if ever needed. 
        
        # Initialize dcut dictionary for district info for output dataset 
        # district. 
        # See glue.sas lines 447-449
        
        dcut_district = DictCut(
          fieldnames=ulist(ocn), key_columns=["g_dcrxid"], 
          keep_columns=['g_scrxid', 'dcrxid', 'dcrxnm', 'grade', 'rgrade'])
        
        # Dict dcut_state for state; glue.sas line 450
        # Distinct combos of 3 columns selected, so here all columns are key 
        # (so dup combos for potential key values will be skipped), so none 
        # are 'keep_columns'.
        dcut_state = DictCut(
          fieldnames=ulist(ocn), key_columns=["g_scrxid","grade","rgrade"])
        
        # Dict for school: glue.sas lines 433-439. Another 'distinct' selection.
        dcut_school = DictCut(
          fieldnames=ulist(ocn), key_columns=["g_bcrxid", "g_dcrxid"
            , "g_scrxid", "bcrxid", "bcrxnm", "grade", "rgrade", "schtype"])
  
        # glue.sas lines 440-446. Another distinct selection.
        dcut_school_intervention = DictCut(
          fieldnames=ulist(ocn), key_columns=[
          'bcrxid_attend', 'bcrxnm_attend', 
          'dcrxid_attend', 'dcrxnm_attend', 'scrxid','schtype'])
        # Initialize the bookmap info dictionaries.
        
        # Some handy variables used in the input loop
        v0 = '0'; v1 = '1'; v2 = '2'; v3 = '3'
        v3_5 = ('3', '4', '5')
        v1_2 = ('1', '2')
            
        # Main for loop to process input rows
        n_student = 0
        n_dummy = 0
        n_unattempteds = 0
        n_out_student = 0
        
        if True:
            print ("Reading input dsr = '%s'" % repr(self.dsr_input))
        for nrows, row in enumerate(dr_input, start=1): 
            if nrows % 1000 == 0:
                now = datetime.datetime.now()
                print ("%s: Time=%s: Processed %d input rows so far . . ." 
                   % (me, now, nrows))
    
            # glue.sas line 82: skip such rows asap in the loop
            if (    int(float(row['ufrx_attempt'])) == 0 
                and int(float(row['ufmx_attempt'])) == 0 
                and int(float(row['ufsx_attempt'])) == 0
                and int(float(row['ufcx_attempt'])) == 0 
                and int(float(row['ufwx_attempt'])) == 0) :
                n_unattempteds += 1
                if verbose:
                    print "Skipping row %d. Unattempted." % nrows
                continue
            # For output, reuse the input row dictionary, saving copying 
            # space and time.
            ostudent = row
            
            # glue line 72
            # This uses the original name parts from input, still capitalized
            # in  most cases.
            name = ""
            delim = ""
            for col in ['ucrxfnm','studentmid','ucrxlnm']:
                part = ostudent[col].strip()
                if part != "":
                    name = name + delim + part
                    delim = " "
            ostudent['student_name'] = name
            # glues lines 73-74. Set schtype ... but it is a single
            # char that is overwritten by convert to dot missing,
            # so save original value from row, aka ostudent now. 
            orig_schtype_attend = ostudent['schtype_attend']
            
            ostudent['schtype'] = row['schtype_attend']
            
            ssid = ""
            if verbose:
                ssid = ostudent['ssid']
            ostudent['distrtype'] = row['distrtype_attend']

            #glue lines 76,77: remove blanks from first and last names
            # Todo: compile regexes before input loop for minor speedup.
            ostudent['ucrxfnm'] = re.sub(' +',' ',ostudent['ucrxfnm'])
            ostudent['ucrxlnm'] = re.sub(' +',' ',ostudent['ucrxlnm'])
            
            # glue lines 78-79: compose birthdate value
            try:
                mm = int(float(row['dob_month']))
                dd = int(float(row['dob_day']))
                y4 = int(float(row['dob_year']))
            except:
                mm = None
            if mm is not None and dd is not None and y4 is not None:
                # glue.sas line 73 
                birthdate = "%2.2d/%2.2d/%4.4d" % (mm, dd, y4)
            else:
                birthdate = missing_value
                #print("birthdate not given")
            row['birthdate'] = birthdate 

            # Review - Why is next commented out?
            # ostudent['birthdate'] = birthdate
            
            # glue.sas line 83: Separate some output for Galludet 
            if ( row['bcrxid_attend'] in ["000001", "000002", "000003"]
                 or row['dcrxid_attend'] in ["000001", "000002", "000003"]):
                dw = dw_galludet
            else:
                n_student += 1
                # else main dw is for dw_student
                dw = dw_student
                if verbose and nrows < 2:
                    print "Output is to dw_student = %s" % repr(dw_student)
                # Continue to mimic other glue.sas processing for student 
                # dataset.
                
                # glue.sas lines 114-
                # Next section: like SAS convert_to_dotmissing, we translate 
                # single-uppercase letter values in many fields to 
                # missing_value.
                # See glue.sas line 118 call to convert_to_dotmissing().
                # "SAS thinks missing values are uppercase letters... 
                # BUG: missings=list(string.uppercase) will overwrite 
                # the 'H' in schtype, and maybe other useful values! 
                # Now we have 'A' as missing in our data, and no others.
                missings = ['A']
                # For future, should we also do missings.extend=['.'] ?
                for column_name in nom_input_var_names:
                    if ostudent[column_name] in missings:
                        ostudent[column_name] = missing_value
                       
                # Next section: Glue.sas line 119 - Do the processing of the 
                # glue.sas  call to properCase()
                name_columns = ['dcxx_county', 'ucrxfnm', 'ucrxlnm']
                for colname in name_columns:
                    ostudent[colname] = ( proper_case(row[colname]) 
                      if proper_case(row[colname]) is not None else "" )
                                                    
                # Next section; See glue.sas line 140 Data output Student
                # line 140 used to rename some columns. 
                # Review this later, and for now just keep any 
                # unneeded old columns and add the needed new ones.
                # Set columns: myid, bcrxid, dcrxid, bcrxnm, dcrxnm, 
                # schtype,distrtype, 
                # per glue lines 142-149.
                ostudent['myid'] = row['lithocode']
                ostudent['bcrxid'] = row['bcrxid_attend']
                ostudent['dcrxid'] = row['dcrxid_attend']
                ostudent['bcrxnm'] = row['bcrxnm_attend']
                ostudent['dcrxnm'] = row['dcrxnm_attend']
                #glue line 148-re-save schtype from convert-dot-missing
                #override. 
                ostudent['schtype'] = row['schtype_attend']
                
                #glue line 149
                ostudent['distrtype'] = row['distrtype_attend']
                
                # Also keep variable 'rgrade' for "raw" grade (before changing
                # nulls to 99) like we do below and glue.sas also does.
                ostudent['rgrade'] = ostudent['grade']
                                
                # glue.sas line 150 set missing grade specifically to "99"
                #if ostudent['grade'] == missing_value:
                #    ostudent['grade'] = "99"
                try:
                    igrade = int(float(ostudent['grade'].strip()))
                    g = str(igrade)
                except:
                    g = '99'
                ostudent['grade'] = g
                #lines 152-163 glue.sas
                ostudent['scrxid'] = "1"
                # ostudent['dummy_record_flag'] = '0' -- done later
                ostudent['g_dcrxid'] = (
                    "%s_%s" % (g, ostudent['dcrxid'].strip()))
                #print "row=%d, set g_dcrxid=%s" % (nrows,ostudent['g_dcrxid'])
                ostudent['g_bcrxid'] = (
                    "%s_%s" % (g, ostudent['bcrxid'].strip()))
                ostudent['g_scrxid'] = (
                    "%s_%s" % (g, ostudent['scrxid'].strip()))
                
                # glue.sas lines 164 - 165
                if ostudent.get('upwx_oe_final_1', None) is not None:
                    ostudent['writing_prompt1'] = ostudent['upwx_oe_final_1']
                else:
                    ostudent['writing_prompt1'] = ""
                    # OPEN: should I also create upwx_oe_final_1 here?
            
                if ostudent.get('upwx_oe_final_4', None) is not None:
                    ostudent['writing_prompt2'] = ostudent['upwx_oe_final_1']
                else:
                    ostudent['writing_prompt2'] = ""
                    
                # glue.sas lines 167 - 170
                if ostudent['ufwx_attempt'] == '0':
                    ostudent['writing_prompt1'] = ""
                    ostudent['writing_prompt2'] = ""
                       
                # glue.sas line 171
                ostudent['ucxxpon'] = '1'
                # Next 5 lines used in glue.sas 173-177, but use lowercase here
                t1 = tmp_ucrotherdnp=['m', 'w', 's', 'c']
                t2 = tmp_ucmotherdnp=['r', 'w', 's', 'c']
                t3 = tmp_ucwotherdnp=['r', 'm', 's', 'c']
                t4 = tmp_ucsotherdnp=['r', 'm', 'w', 'c']
                t5 = tmp_uccotherdnp=['r', 'm', 'w', 's']
                # glue.sas line 179: except here use list of tmp var names
                tmp_otherdnp_names = ['tmp_ucrotherdnp', 'tmp_ucmotherdnp'
                  , 'tmp_ucsotherdnp', 'tmp_ucwotherdnp', 'tmp_uccotherdnp' ]
                tmp_otherdnps = (tmp_ucrotherdnp, tmp_ucmotherdnp
                  , tmp_ucsotherdnp, tmp_ucwotherdnp, tmp_uccotherdnp )
                # glue.sas line 180: create list otherdnp_names similar to 
                # glue  otherdnp array
                otherdnp_names = ['ucrotherdnp', 'ucmotherdnp', 'ucsotherdnp'
                  , 'ucwotherdnp', 'uccotherdnp' ]
                # glue.sas line1    
                ostudent['reported_flag'] = '0'
                # glue line 184:
                ostudent['pass_count'] = '0'

                # This for loop - see glue.sas 185-218: 
                #   %do %while (%scan(&subject_list,&i) ne);
                # NB: glue reference line numbers will be removed from comments 
                # after some sanity testing - they now refer to an annotated 
                # development reference listing.
                for idx_subject, subject in enumerate(subject_list):
                    if nrows < 2 :
                        print "nrows=%d, subject=%s" % (nrows, subject)
                    # subject2 = subject_list2[idx_subject] - not used in 
                    # this loop.
                    # glue line 189:
                    if (  (ostudent['uf%sx_attempt' % subject] == '1')
                      and (ostudent['uf%sx_invalid' % subject] == '0')):
                        # This row is 'reported' because it is for a valid 
                        # attempt
                        ostudent['reported_flag'] = '1'
                    # glue lines 190-200:
                    # Handy variable:
                    vp = 'up%sx' % subject
                    # Create new column for accomodation flag for this subject 
                    # dependent on sub-accomodation variables.
                    if (ostudent['%s_read_aloud' % vp] == v1
                     or ostudent['%s_extended' % vp] == v1
                     or ostudent['%s_dictionary' % vp] == v1
                     or ostudent['%s_scribe' % vp] == v1
                     # or (ostudent.get(('%s_calculator' % vp), None)) is not 
                     # None and ostudent['%s_calculator' % vp] == v1
                     or ostudent['%s_other' % vp] == v1              
                        ):
                        ostudent['%s_accom' % vp] = v1
                    # glue lines 201-203: create IEP and LEP flags per 
                    # subject based on the matching upxx* value
                    if ostudent['upxxlep'] == v1:
                        ostudent['up%sxlep' % subject] = v1
                    else:
                        ostudent['up%sxlep' % subject] = v0
                    if ostudent['upxxiep'] == v1:
                        ostudent['up%sxiep' % subject] = v1
                    else:
                        ostudent['up%sxiep' % subject] = v0
                    # glue lines 204-206: set some vars to missing_value
                    ostudent['%spscal' % vp] = missing_value
                    ostudent['%splev' % vp] = missing_value
                    ostudent['%smerged' % vp] = missing_value
                    # glue line 207 pass_count adjustment
                    if ostudent['%slev' % vp] in v3_5:
                        ostudent['pass_count'] = str(
                            int(ostudent['pass_count']) + 1)
                    # glue lines 208-210: adjust xpass dependent on xlev
                    if ostudent['%slev' % vp] in (v3_5):
                        ostudent['%spass' % vp] = v1
                    elif ostudent['%slev' % vp] in (v1_2):
                        ostudent['%spass' % vp] = v2
                    else:
                        ostudent['%spass' % vp] = v3
                    #glue line 211: adjust ucxxpon 
                    if ( (ostudent['%slev' % vp] in v1_2)
                      or (ostudent['uf%sx_attempt' % subject] == v0)
                      or (ostudent['uf%sx_invalid' % subject] == v1)
                        ):
                        ostudent['ucxxpon'] = v0
                            
                    # glue lines 212 - 215: adjust otherdnp -- 
                    # revise 'tmp_otherdnp' values
                    if ( (ostudent['%slev' % vp] in v3_5)
                      or (ostudent['uf%sx_attempt' % subject] == v0)
                      or (ostudent['uf%sx_invalid' % subject] == v1)
                      # temp code to test inner loop
                      # or (1 == 1)
                         ):
                        # Subject was not attempted or invalid, or lev was 
                        # 3, 4 or 5,  so remove it from the 'otherdnp' lists.
                        # Also see glue lines 213-215 - k loop over this 
                        # subject's tmp_otherdnp to remove current subject 
                        # from each.
                        # Note: given the current settings of the involved 
                        # arrays, this loop is equivalent to 
                        # continuing/skipping if the itod is for the current 
                        # subject and otherwise deleting index 0 ... 
                        # This may not be elegant, but this initial current 
                        # loop's code logic is a good start to help document 
                        # in the code base history that what is being done is 
                        # like the reference glue.sas code
                        deli = None
                        for itod, tod in enumerate(tmp_otherdnps):
                            try:
                                deli = tod.index(subject)
                            except:
                                continue
                            del tod[deli]
                    # end if clause 
                    # also see glue.sas line 212-215: xlev in (3, 4, 5)
                # end loop over subjects. Glue loop ends at line 218
              
                # Glue subjects loop line 219-232: 
                # Set 'other_dnps[]' to tmp_otherdnps[] lists, except use 
                # style2 abbreviations
                other_dnps=[]
                for i_dnp, todnp in enumerate(tmp_otherdnps):
                    # Put the 'style 2' abbreviations to 'otherdnps[] from 
                    # tmp_otherdnps[]
                    # NB: these vars are not used again in glue.sas -- but 
                    # maybe they are accessible from macro "Means()" as 
                    # it uses similar vars later.
                    # Keep this open.
                    odnp = []
                    for dnp_idx in xrange(len(todnp)):
                        subject = todnp[dnp_idx]
                        if subject == 's':
                            odnp.append("sc")
                        elif subject == 'c':
                            odnp.append("so")
                        else:
                            odnp.append(subject)
                    other_dnps.append(odnp)
                    
                # end loop over tmp_otherdnps to set other_dnps[], see 
                # glue.sas 219-232
                
                # Glue loop lines 235-298: call subject_vals_set() to set 
                # various subject and content_strand based output values.
                subject_vals_set(dict_ovar_val=ostudent, 
                  dict_subject_cstrands=self.dict_subject_cstrands) 
                 
            # end else clause: for dw = dw_student
    
            # See schtype at glue line 299
            if  (  ostudent['schtype'] is None 
                or ostudent['schtype'].strip().upper() not in ('N', 'H', 'D')
                ):
                ostudent['stateinclusionflag'] = '1'
            else:
                ostudent['stateinclusionflag'] = '0'

            #glue line 300:
            if ( ostudent['schtype'].strip().upper() 
                not in ('N', 'H', 'C', 'U')
                or not ostudent['schtype']):
                ostudent['districtinclusionflag'] = '1'
            else:
                ostudent['districtinclusionflag'] = '0'
           
            # glue.sas line 328.
            if (    ostudent['distrtype'] is not None
                and ostudent['distrtype'].strip().upper() == 'J'
                and ostudent['distrtype_home'] is not None
                and ostudent['distrtype_home'].strip().upper() in ('N', 'D')
                ):
                ostudent['stateinclusionflag'] = '0'
                if self.verbose:
                    print("Distrtype makes stateinclusionflag 0")

            #glue 300-301
            if (   ostudent['ufrx_attempt'] == '1'
               and ostudent['ufmx_attempt'] == '1'
               and ostudent['ufwx_attempt'] == '1'
               and ostudent['ufsx_attempt'] == '1'
               and ostudent['ufcx_attempt'] == '1'
               ):
                ostudent['attempt_all'] = 1
            else:
                ostudent['attempt_all'] = 0
                
            # See glue line 351 skip all records with schtype == 'H'
            # Could do this earlier, making the checks for 'H' in some
            # clauses above moot, but keeping that as it is now because it
            # matches the glue.sas code lines quoted above to facilitate
            # comparison of code logic in early versions of this code.
            if ostudent['schtype'] == 'H':
                print ("Skipping lithocode '%s' because schtype is H" 
                  % ostudent['lithocode'] )
                continue
            # Now, ostudent is (mostly) populated for copying and 
            # writing.
            ostudent['dummy_record_flag'] = '0'
            dcut_district.add(row=ostudent)
            dcut_state.add(row=ostudent)
            # dict for school. see glue.sas line 437 - exclude some rows
            if (    ostudent['bcrxid'] != ""
                and (  ostudent['schtype'] is None
                    or ostudent['schtype'].strip().upper() != 'H'
                    )
                ):
                dcut_school.add(row=ostudent)
                dcut_school_intervention.add(row=ostudent)
            
            # Write student row
            n_out_student += 1
            dw_student.writerow(ostudent)
            
            if ostudent['dcrxid_home'] != "":
                # For students with dcrxid_home non-null,
                # produce "dummy_rows" with dummy_record_flag of '1' 
                n_dummy += 1
                # Also see glue.sas Data student_dummy lines 309-320.
                if 1 == 2:
                    print (
                     "Input row %d, dcrxid_home='%s' creates "
                     " dummy_record %d" 
                     % (nrows, ostudent['dcrxid_home'], n_dummy))
                ostudent['dummy_record_flag'] = '1'
                # SAS glue.sas lines 323 and 325 and no further settings in 
                # that code block of stateinclusionflag to 1 imply that 
                # stateinclusion flag must be 0 for all 'dummy' records. 
                ostudent['stateinclusionflag'] = 0
                ostudent['dcrxid'] = ostudent['dcrxid_home']
                ostudent['dcrxnm'] = ostudent['dcrxnm_home']
                ostudent['distrtype'] = ostudent['distrtype_home']
                ostudent['g_dcrxid'] = ( 
                  ostudent['grade'].strip() + '_' 
                  + ostudent['dcrxid'].strip())               
                ostudent['dcrxnm_attend'] = ostudent['dcrxnm_home']
                ostudent['bcrxid_attend'] = ''
                ostudent['schtype'] = ''
                # Now schtype is changed in here in a to-be-output 
                # "student_dummy" row, so re-set 
                # districtinclusionflag per glue lines 317 and 326.
                ostudent['districtinclusionflag'] = '1'
                ostudent['g_bcrxid'] = ''
                g = ostudent['grade']
                if g is None: 
                    # Check where glue.sas sets this?
                    g = "99"
                    
                sid = ostudent['scrxid']
                if sid is None:
                    sid=""
                ostudent['g_scrxid'] = ("%s_%s" % (g, sid.strip()))
                # write the "dummy_row" as did glue.sas
                n_out_student +=1 
                dw_student.writerow(ostudent)
            
                # Again, register this dummy row with the dcut dictionaries.
                dcut_district.add(row=ostudent)
                dcut_state.add(row=ostudent)
                # dict for school. see glue.sas line 437 - exclude some rows
                if (    ostudent['bcrxid'] != ""
                    and ostudent['schtype'].strip().upper() != 'H'):
                    dcut_school.add(row=ostudent)
                    dcut_school_intervention.add(row=ostudent)
            # end 'dummy_row' processing output
           
            # Proceed to do additional processing for dw_student_aggregation.
            # glue lines 321-352
            
            """ aggregation_vals_set(dict_ovar_val=ostudent
              , dict_subject_cstrands=self.dict_subject_cstrands
              , dict_bookmap=dict_bookmap)
            
            # write student_aggregation row
            dw_aggregation.writerow(ostudent)
           
            
            # glue lines 304 - 306
            if dw_student_dummy and ostudent['dcrxid_home'] != "":
                dw_student_dummy.writerow(ostudent)"""
                
            # end else clause for writing to dw_student
        # end main for loop to process input rows 
        
        if verbose:
            print ("Len dcut_district = %d" % len(dcut_district.odict) )
            print ("Len dcut_state = %d" % len(dcut_state.odict) )
            print ("Len dcut_school = %d" % len(dcut_school.odict) )
            print ("Len dcut_school_intervention = %d" 
                   % len(dcut_school_intervention.odict) )
            
        # Write the dcut_x dictionaries, each to its own dsw outupt dataset.
        
        dcut_district.write(dsw=self.dsw_district)
        dcut_state.write(dsw=self.dsw_state,verbose=1)
        dcut_school.write(dsw=self.dsw_school)
        dcut_school_intervention.write(
            dsw=self.dsw_school_intervention)
        
        # Close the files of the written-to datasets. 
        # TODO: open-close the files in the DictReader and DictWriter,
        # (not the Dataset container) and decorate them with context_manager
        # so they can be used individually with 'with' statement.
        # 
        dw_student.dsw.close()
        self.dsw_district.close()
        self.dsw_state.close()
        self.dsw_school.close()
        self.dsw_school_intervention.close()
        
        print ("%s: done, read %d input rows, found %d dummy rows,"
               " skipped %d unattempteds." 
               % (me, nrows, n_dummy, n_unattempteds))
        print ("\tWrote %d student rows" % (n_out_student))
        my_end = datetime.datetime.now()
        print ("%s:\n\tstudy_name='%s',\n\tstart=%s: end=%s,\n\t"
          "Total elapsed time (min, sec, microsec) =%s.\n\tGood-bye."  % 
          (me, self.study_name, my_start, my_end, repr(my_end - my_start)))

        # end coverage of ogt/.../code/superdata/glue.sas code lines: 
        return
    # end def ogt_student