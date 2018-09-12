import re
from collections import OrderedDict
from dataset import Dataset
import datetime

# rescorecheck()
def rescorecheck(subject=None, grade=None, admin_term="SP13", 
  ds_input=None,
  ds_bookmaplocs=None,
  odict_loc_subs=None,  
  ds_out=None, 
  ds_report2=None,
  bml_form_style=None):
    """ Verify, correct, or create raw_score conversion values.

Extended Summary:
=================
Output scored test data per bookmap_location and bookmap Datasets.
Scored data goes to Dataset ds_out.

Params:
=======
subject: String
    - subject area to which to restrict rescoring (reading, writing, science, social-studies, math)

grade: String
    - grade as a string '10','11','12'... or 'all' to restrict rescoring by grade

admin_term: String 
    - admin term to which to restrict selection of bookmap_location rows.
    - example: SP13, FA14, etc. Should match the admin_term given in the bookmap_locations file in subfield 3 of the column "form".

ds_input: Dataset
    - Dataset of the input data to be rescored. 
    - The dataset must have these column names: ['test_id', 'form_id', 'item_id', 'finalraw_item', 'score_item']
    - See docs on Dataset for more.

ds_bookmaplocs: Dataset
    - Dataset of the bookmaplocations (traditionally stored in an excel workbook, with sheet named "BookMaps").
    - The required column names are long-standing: ['grade', 'subject', 'form', 'bookmap_location']
    - The column bookmap_location identifies an excel sheet with bookmap info for scoring test items. 
    - In the code, required bookmap columns are listed in list "bookmap_required_columns". 
    - It should have some rows of interest that match the parameter "admin_term" in the column "form", in its third "::"-delimited subfield.
    - Rows should exist for 5 subject area values, 
      - and in each there normally are 3 rows, one for each form_id, as presented in column "form", 
      - for bml_form_style == 0, 
         - the first subfield's suffix value to prefix value "Form" is the form_id. 
         - Minor parsing is done here to primp the form_id value to prefix the integer-looking form_id values with a 0 so they match the presentation in the subsidiary 'bookmaps files' as named in the bookmap_locations sheet in column "bookmap_location".
      - for bml_form_style == 1, the form column simply has the form_id with no special parsing required    
      - Each bookmaplocations row provides a bookmap (answer key sheet, basically),  where such bookmap is applied to all rows in the input data that match the observed answers for the given subject and form and specific items.
        
odict_loc_subs: OrderedDict
    - An ordered dictionary where key is a regular expression pattern to match a location value substring and value is the replacement value. 
    - Order is important, so if caller did not use an ordered dictionary, it is converted into one. 
    - Each substitution is tried in order.

ds_out: Dataset
    - Output dataset of the rescored data, according to the bookmap locations info.
    - columns are same as input, excep the score values are computed: ['test_id', 'form_id', 'item_id', 'finalraw_item', 'score_item']

ds_report2: Dataset
    - all input-data related rescorecheck errors go to this dataset
    - columns are; report2_fieldnames=[
      'id','test_id','item_id','test_answer','test_score','correct_answer',
      'correct_score','message']  
      
bml_form_style: integer
    - style 1 works for OGTSP12 data
    - style 0 works for OGTSP13 data
    - may need to add a style per year as conventions change.
    
bkmap_base_path: String
    - String to substitute to translate conversion sheet location field;
    - rloc = repr(std_location)
        # By tradition, "&ctpath.\\" is to be replaced by given parameter.
        std_location = rloc.replace(
          "&ctpath."'\\''\\', ctpath).replace("'","")
   
    """
    me = "rescorecheck:" 
    time_start = datetime.datetime.now()
    
    if subject is None:
        raise ValueError("A subject must be specified")
    # Normalize the subject name for more friendly matching
    subject = subject.lower().replace(' ','_').strip()
    subjects = ("math", "reading", "science", "writing", "social_studies")
    if subject not in subjects:
        raise ValueError(
          "Got subject='%s', subject must be in %s" % (subject, str(subjects)))
        
    if grade is None:
        raise ValueError("A grade must be specified")
    grade = str(grade).lower()
    igrade = 0
    if (grade != 'all'):        
        igrade = int(float(grade))
        # May want to narrow limits later, but now leave them a bit lax.
        if igrade < 8 or igrade > 15:
            raise ValueError("Given grade %s is out of range" % grade)
    if admin_term is None:
        raise ValueError(
          "An admin_term must be specified (eg sp13, fa14) as in bookmap "
          "locations sheet column 'form', third '::'-delimited subfield")
    admin_term = admin_term.lower().strip()
        
    # ds_input checks
    if ds_input is None:
        raise ValueError("ds_input must be given")
    reader_input = ds_input.DictReader()
    if reader_input is None:
        raise ValueError(
          "ds_input reader for dataset '%s' cannot open" % repr(ds_input) )
    if odict_loc_subs is not None:
        # if not an ordered dict, convert it into one
        if not isinstance(odict_loc_subs, OrderedDict):
            odict_loc_subs = OrderedDict(odict_loc_subs)  
    #Check required columns on reader_input
    required_input_columns = [
      'test_id', 'form_id', 'item_id', 'finalraw_item', 'score_item']
    for req_col in  required_input_columns:
        if req_col not in reader_input.fieldnames:
            raise ValueError (
              "Required column name '%s' is missing in ds_input='%s'"
              % (req_col, repr(ds_input)) )
      
    if ds_bookmaplocs is None:
        raise ValueError( "A dataset ds_bookmaplocs must be specified.")
        
    print ( 
      "\nSTART %s, time=%s, "
      "\n\t params: subject=%s,grade=%s, admin_term='%s'"
      "\n\tds_raw_scores=%s,"
      "\n\tds_bookmaplocs='%s'"
      "\n\tds_out='%s'"
      "\n\tods_report2='%s'"
      "\n\todict_loc_subs='%s'"
      % (me, time_start,subject,grade, admin_term, repr(ds_input), 
         repr(ds_bookmaplocs),repr(odict_loc_subs),repr(ds_out),
         repr(ds_report2) ) )
    
    print ( "Reading bookmaplocs for grade='%s', and subject='%s'..."
      % (grade, subject ))
    
    # Get reader for bookmap locations (bml) worksheet. 
    reader_bml = ds_bookmaplocs.DictReader()
    # Per Datacheck manual section 2.4.2, required columns.
    required_columns=[
      'grade', 'subject', 'form', 'bookmap_location']
    for rq_col in required_columns:
        if rq_col not in reader_bml.fieldnames:
            ValueError(
              "Required column name '%s' is not in %s" 
              % (rq_col, repr(ds_bookmaplocs)))
       
    if bml_form_style is None:
        # Style of the 'form' field found in development test data:
        # "Formxx:OGTX:SSYY" where xx is 1,2 or SV, 
        # and X is in R,M,C,S,W,
        # and SS is in SP, FA 
        # and YY is 20YY value: 12 for 2013, etc.
        bml_form_style = 0   
        
    else:
        # Another style found in some test data circa 20130701:
        #Form field is simply 01,02 or SV.
        bml_form_style = 1
 
    # Read and set up the bookmaploc info for this grade and subject 
    # (already should be constant through input rows) and form_id 
    # ( a value in the input row).
    # Dict formid_bookmap: (1) key is a form_id value that occurs 
    # in the bookmap locations sheet and also in the data input file
    # and (2) value is the bookmap dict for the formid value.
    formid_bookmap = OrderedDict()

    num_bml = 0;
    for idx_bml, row_bml in enumerate(reader_bml):
        bml_grade = row_bml['grade'].lower().strip()
        bml_subject = (row_bml['subject'].lower().replace(" ","")
                       .replace("_","").strip())
        bml_form = row_bml['form'].lower().strip()
        
        if (  (    (grade != bml_grade) 
               and (bml_grade != "all") 
               and (bml_grade != "g" )
              )
           or ( subject != bml_subject)
           ):
            # Skip irrelevant row for these params
            # print "SKIPPIING row, idx_bml=%d" % idx_bml
            continue
        
        # Form field
        if bml_form_style == 0:
            bml_form_fields = bml_form.split("::")
            print (
              "idx_bml=%d,formfields='%s'" 
              % (idx_bml, repr(bml_form_fields)))
            
            if ( len(bml_form_fields) != 3 ):
                raise ValueError(
                  "Dataset='%s', row %d, field form='%s' has %d subfields, not 3."
                  % ( repr(ds_bookmaplocs), idx_bml + 2, bml_form, 
                      len(bml_form_fields)) )
            bml_formid = bml_form_fields[0].replace("form","")
            # if formid is integer, prefix it with a 0 and keep it a string, 
            # to match values presented in the traditional input files.
            try:
                bml_formid = ("0" + bml_formid if (len(bml_formid) == 1) 
                              else bml_formid)
            except:
                pass
            bml_admin = bml_form_fields[2]
            if (bml_admin != admin_term):
                #skip irrelevant row
                print (
                  "\nSkipping bookmaplocations row id=%d, admin_term='%s' "
                  "but bml_admin='%s'\n"
                  % (idx_bml +2 , admin_term, bml_admin))
                continue
        elif bml_form_style == 1:
            #
            bml_formid = bml_form
        else:
            raise ValueError(
              "Parameter bml_form_style=%d unknown."
              % bml_form_style )
        # Read the bookmap at this bookmap location
        num_bml += 1
        print (
          "idx_bml=%d, grade='%s',subject='%s',form='%s'" 
          % (idx_bml, bml_grade, bml_subject, bml_form))

        bml_location = row_bml['bookmap_location']
        # Revise location using odict_loc_subs
        if odict_loc_subs is not None:
            for key, val in odict_loc_subs.iteritems():
                print (
                  "Calling re.sub(varname_pattern='%s', "
                  "replacement='%s',location_value='%s'" 
                  % ( key, val, bml_location))
                #test_str = locsub(varname=key, replacement=val, origstring=test_str)
                bml_location = re.sub(key, val, bml_location)
                print ("New location string='%s'" % bml_location)
            
        print "idx_bml=%d, row='%s'" % (idx_bml, repr(row_bml) )
            
        # For this row_bml's bml_form_id as the key, create a formid entry
        # whose value is a dictionary named itemid_info. 
        # The itemid_info dictionary key is an itemid and the info value 
        # is an OrderedDict of bookmap column-value pairs.
        itemid_info = OrderedDict()
        # Consider: raise error here if key bml_formid already exists.
        formid_bookmap[bml_formid] = itemid_info
        
        # Populate the itemid_info dictionary from the bookmap. 
        # First, init the dataset and reader for this bookmap.
        ds_bookmap = Dataset(dbms='excel_srcn', workbook_file=bml_location,
          sheet_name="BookMap", open_mode='rb')
        reader_bookmap = ds_bookmap.DictReader()
        bookmap_required_columns = ['item_position', 'book_position', 
          'its_id','grade', 'subject',
          'form', 'session', 'description', 'reporting_subscore', 
          'role', 'item_format',
          'point_value', 'answer_key', 'numeric_key', 'weight', 
          'tagged_for_release',
          'ohio_code', 'test', 'graphic', 'benchmark', 'indicator', 
          'content_standard',
          'grade_level']
        for rcol in bookmap_required_columns:
            if rcol not in reader_bookmap.fieldnames:
                raise ValueError(
                  "Required column '%s' not in dataset '%s'" 
                  % (rcol, repr(ds_bookmap)))
        bm_num_rows = 0;
        info = "----- Storing MC item_id_info for map_itemid vals: " 
        delim=""
        for (idx_map, row_map) in enumerate(reader_bookmap):
            if row_map['item_format'] != "MC" :
                continue
            map_itemid = (str(int(float(row_map['item_position'])))
                         .lower().strip())
            # For this itemid, save a copy of the row of bookmap data,
            # because row_map is overwritten each time thru this loop.
            info += ("%s %s" % (delim, map_itemid))
            delim = ","
            itemid_info[str(map_itemid)] = row_map.copy()
            bm_num_rows += 1
        print info
        print (
          "\nBookmap idx=%d, read%d rows from ds_bookmap='%s'" 
          % (idx_bml, bm_num_rows, repr(ds_bookmap)))

    # end: for idx_bml, row_bml in enumerate(reader_bml):
    
    print (
      "Finished ingesting ds_bookmaplocs (%s) information. Using %d rows." 
      % (repr(ds_bookmaplocs), num_bml))
    if num_bml < 1:
        print "Finished: No ds_bookmaplocs rows of interest found."
        return

    print ("Reading and processing input Dataset ds_input=%s" 
      % repr(ds_input))
    
    # Prepare normal output for ds_out: Same columns as input row, but with 
    # authoritative score found in bookmap rather than raw score in input.
    output_columns = required_input_columns
    print "Rescore output column names: %s" % repr(output_columns)
    writer_out = ds_out.DictWriter(output_columns)
    writer_out.writeheader()
    # dict for normal output 
    d_out = OrderedDict()
    
    # report2 - misc errors, incorrect scores in input.
    report2_fieldnames=[
      'id','test_id','item_id','test_answer','test_score','correct_answer',
      'correct_score','message']
    writer_report2 = ds_report2.DictWriter(report2_fieldnames)
    print "Rescore report2 column names: %s" % repr(report2_fieldnames)
    writer_report2.writeheader()
    # dict for error report output
    r2 = OrderedDict()

    # Read reader_input for the main input dataset with raw scores    
    print ( 
      "Rescore input data column names: %s" % repr(reader_input.fieldnames))
    n_input = 0
    n_errors = 0
    now = datetime.datetime.now()
    print ("Time=%s: Reading input item rows . . ."  % (now))
    for (idx, row_input) in enumerate(reader_input):
        # Each input row basically represents one test-takers response data 
        # for a particular item.
        # NB: caller must already have filtered ds_input by the correct
        # subject, grade.
        n_input += 1 
        if n_input % 100000 == 0:
            now = datetime.datetime.now()
            print ("Time=%s: Processed %d input item rows so far . . ." 
                   % (now, n_input))
        # 'test_id': may be called something else in the database, but a 
        # 'test_id' identifies metadata and a complete set of answers by 
        # one test-taker for a test that covers multiple subject areas.
        inp_test_id = str(int(float(row_input['test_id'])))
        r2['test_id'] = inp_test_id
        d_out['test_id'] = inp_test_id
        
        inp_item_id = str(int(float(row_input['item_id'])))
        r2['item_id'] = inp_item_id
        d_out['item_id'] = inp_item_id
        
        # Form id string: Examples: 01, 02, sv
        inp_form_id = str(row_input['form_id']).lower().strip()
        d_out['form_id'] = inp_form_id
        
        # finalraw_item is the test-takers answer for this item
        fri = row_input['finalraw_item']
        if fri == "-" or fri == ""  or fri == "*":
            #Convention, I think is to use 99 in these fields as missing.
            inp_finalraw = str(99)
        else:
            try:
                inp_finalraw = str(int(float(row_input['finalraw_item'])))
            except:
                print (
                  "Input row %d: finalraw_item='%s' not a float. Using 99."
                  % (idx+1, row_input['finalraw_item'] ))
                inp_finalraw = str(99)
        d_out['finalraw_item'] = str(inp_finalraw)
        
        # score_item: is the score already given the test-taker in the 
        # input data
        inp_score_str = row_input['score_item']
        if ( inp_score_str is None 
            or inp_score_str == "" 
            or inp_score_str == 'None'):
            inp_score = 99
        else:
            try:
                inp_score = int(float(inp_score_str))
            except:
                r2['message'] = (
                  "Form='%s', score_item='%s' not a float on input row %d "
                  "in the bookmap file. Skipping"
                  % (inp_form_id, inp_score_str,  idx+1) )
                print r2['message']
                n_errors += 1
                r2['id'] = str(n_errors)
                writer_report2.writerow(r2)
                continue
                
        inp_item_id = str(int(float(row_input['item_id'])))
        
        if itemid_info.get(inp_item_id) is None:
            # Oops, this input has a test answer for an item not in 
            # the bookmap.
            r2['message'] = (
              "Form='%s', item_id='%s' on input row %d is not an MC item "
              "in the bookmap file. Skipping"
              % (inp_form_id, str(inp_item_id), idx+1) )
            n_errors += 1
            r2['id'] = str(n_errors)
            writer_report2.writerow(r2)
            continue
        # Got important data input values so now work with them.
        # For this form and item_id, get bookmap info for this item
        if formid_bookmap.get(inp_form_id) is None:
            # Oops - this input form_id is an unknown bookmap formid
            r2['message'] = (
              "Form='%s' on input row %d is not "
              "in the bookmap file. Skipping"
              % (inp_form_id, idx+1) )
            n_errors += 1
            r2['id'] = str(n_errors)
            writer_report2.writerow(r2)
            continue
            
        itemid_info = formid_bookmap[inp_form_id]
        iteminfo = itemid_info[str(inp_item_id)]
        # get correct answer - normalized in case of float '.'
        try:
            numeric_key = str(int(float(iteminfo['numeric_key'])))
        except:
            # Effectively set a bad key by setting sentinel value here.
            numeric_key = str(98)
        
        score_error = 0
        correct_score = 0
        #compare correct_key with actual 'raw' answer and input data's score
        if (int(numeric_key) == int(inp_finalraw) ):
            # test-taker got this correct. Check if proper score was credited.
            correct_score = int(float(iteminfo['point_value']))
            if int(inp_score) != int(correct_score):
                score_error = 1
        else:
            if int(inp_score) != 0:
                score_error = 1
        if score_error == 1:
            r2['test_answer'] = inp_finalraw
            r2['correct_answer'] = numeric_key
            r2['test_score'] = inp_score
            r2['correct_score'] = correct_score
            r2['message'] = ( "Score = '%s', but input shows '%s'"
                         % (str(correct_score), str(inp_score) ) )
            n_errors += 1
            r2['id'] = str(n_errors)
            writer_report2.writerow(r2)
            
        d_out['score_item'] = str(correct_score)
        writer_out.writerow(d_out)
    #end loop for reader_input
    time_end = datetime.datetime.now()

    print ("%s: "
      "Processed %d total data input rows from dataset=%s."
      "Done at time=%s.\n\nTime elapsed=%s\n"
      "================================================\n"
      % (me, idx+1, repr(ds_input),
         time_end,str(time_end - time_start) 
         )
      )
    return

# temp unit tests 
if __name__ == '__main__':
    # See also: the integration/regression tests that are in 
    # .../test/test_datacheck.py in method test_rescore_001()
    # The below are usually useful to primp for local testing during 
    # code editing.
    
    print "Starting local main code. . . "
    
    #test ds_input-This takes 18 seconds, so nicer to do it
    # once and independently from separate unittest(s) below
    server='38.118.83.61'
    database='ScoreReportingTestData'
    #server='DC1PHILLIPSR\SQLEXPRESS'
    #database='testdb' - Math subject table is 'mc_table_5 s'
    ds_input = Dataset(dbms='pyodbc', server=server, db=database,
      query="""select t.id, t.ucmx_form, s.id, s.upmx_finalraw_item,
       s.upmx_score_item
       from pre_qc_flat_table t, mc_table_5 s
       where t.id = s.flat_table_id
       order by s.flat_table_id, t.id
       """,
      columns = [
        'test_id', 'form_id', 'item_id', 'finalraw_item', 'score_item'],
      open_mode='rb')
    # Basic test data 
    tddir = (
      "H:/Assessment/CSSC/AnalysisTeam/AssessmentReporting/"
      "PythonUnitTestData/rescorecheck/")        
        
    #input datasets
    fn_input = tddir + "abc.csv"
    ds_input = Dataset(dbms='csv',name=fn_input,open_mode='rb') 
    print "Got ds_input = '%s'" % ds_input
    # Open more datasets for rescorecheck parameters.
    import os
    home = os.path.expanduser("~")+ os.sep
    tddir = home+"testdata/rescorecheck/OGT_2013SP/"
    bookmaplocs_filename = tddir+"OGT_SP13_Op_DataLayout_bookmapLocations.xls"
    report2_filename = tddir +"output/rescore_report2.csv"
    out_filename = tddir + "output/rescore_out.csv"
    
    print "Using ds_input = '%s'" % ds_input
    
    ds_out = Dataset(name=out_filename,open_mode='wb')
    ds_bookmaplocs = Dataset(dbms='excel_srcn', 
      workbook_file=bookmaplocs_filename,
      sheet_name="Bookmap", open_mode='rb')
    print "Using ds_out = '%s'" % repr(ds_out)
    ds_report2 = Dataset(name=report2_filename,open_mode='wb')
    print "Using ds_report2= '%s'" % repr(ds_report2)
                    
    print "Calling rescorecheck(...)"
    rv = rescorecheck(grade='10', subject="Math", 
         ds_input=ds_input, 
         ds_bookmaplocs=ds_bookmaplocs,
         ds_out=ds_out, ds_report2=ds_report2)
    
