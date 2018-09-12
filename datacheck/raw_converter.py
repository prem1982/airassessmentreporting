from dataset import Dataset
from collections import OrderedDict
import re

def raw_converter(subject=None, grade=None, form=None, 
  ds_raw_scores=None,
  ds_standards=None,
  odict_loc_subs=None,
  std_grade_column = 'grade',
  std_form_column = 'form',
  std_location_column='location',

  ds_out=None,
  ds_report2=None,
  ds_sumcheck_out=None,
  ds_sumcheck_report2=None):
    """ Verify, correct, or create raw_score conversion values.

    Extended Summary:
    =================
    converter() - "convert" raw score to scaled score and possibly other values via lookups 
    into conversion tables (CT) embodied in excel worksheets.

    Parameters:
    ===========
    ds_out: Dataset

      - The output is the modified dataset restricted to the raw score variables and 
        conversion outptut variables
        listed in the semantic standards sheet for the subject and grade. The conversion 
        variable values are the correct values given by the semantic standards sheet 
        and the spreadsheets that it references.

    ds_report2: Dataset (writeable)
        - Any errors found are reported here
        
    odict_loc_subs: OrderedDict
        - An ordered dictionary where key is a regular expression pattern to match a location value substring and value is the replacement value. 
        - Order is important, so if caller did not use an ordered dictionary, it is converted into one. 
        - Each substitution is tried in order.
    """
    me = "raw_score_converter:" 
    print (
      "\n%s: Starting with subject=%s,grade=%s,form=%s,ds_raw_scores=%s\n" 
      % (me, subject, grade, form, repr(ds_raw_scores) ))
    # NB: may want to add params for 'conversion_column_names' and 'standard_column_names',
    # maybe subjects or other names if such names ever vary, and set default values. 
    # For now, this method hardcodes several dataset column names and some other names.
    
    if subject is None:
        raise ValueError("A subject must be specified")
    subject = subject.lower().replace(' ','_')
    subjects = (
      "math", "reading", "science", "writing", "soc_stud")
    if subject not in subjects:
        raise ValueError("subject must be in %s" % str(subjects))
        
    if grade is None:
        raise ValueError("An integer grade must be specified")
    grade = str(grade).lower()
    igrade = 0
    if (grade != 'all'):        
        igrade = int(float(grade))
        if igrade < 8 or igrade > 15:
            raise ValueError("Given grade %s is out of range" % grade)
        
    if form is None:
        form = "all"
    else:
        form = str(form).lower()
        
    if ds_raw_scores is None:
        raise ValueError("ds_raw_scores must be given")
  
    if ds_sumcheck_out is None:
        raise ValueError("ds_sumcheck_out must be given")
    
    if ds_sumcheck_report2 is None:
        raise ValueError("ds_sumcheck_report2 must be given") 
        
    if ds_standards is None:
        raise ValueError("A ds_standards dataset must be specified.")
    if odict_loc_subs is not None:
        # If not an ordered dict, convert it into one
        if not isinstance(odict_loc_subs, OrderedDict):
            odict_loc_subs = OrderedDict(odict_loc_subs)  
    print ( 
      "\nStarting %s params: subject=%s,grade=%s, form=%s, \n\tds_raw_scores=%s, "
      "\n\tds_standards=%s, \n\tds_out=%s, \n\todict_loc_subs=%s"
       % (me, subject, grade, form, repr(ds_raw_scores), repr(ds_standards),
          repr(ds_out), repr(odict_loc_subs) ) )
  
    print ( "Reading standards for grade='%s', form='%s' and subject='%s'..."
      % (grade, form, subject ))
    
    # Get reader for standards worksheet. It has about 5 or 6 'standards' rows for each of 5 subjects.
    # Each row provids a raw_score_variable name and 4 "output" conversion variable names as found in the
    # input dataset. 
    # None of the variable names is (none should be) duplicated among all rows of the standards worksheet.
    # Each row also provides a workbook and sheetname to identify a worksheet of a conversion table that
    # is used to take the raw_score_variable value from the input and map it to each of the 'output conversion'
    # values that correspond to the output conversion variable names in the standards sheet. 
    # If a value is found in the input for a raw_score_variable  that is not keyed in its conversion table, 
    # an error is logged to ds_report2. 
    
    reader_standards = ds_standards.DictReader()
    required_standards_columns=[
      std_grade_column, 'subjects', std_form_column, 
      std_location_column, 'sheetname',
       'raw_score_variable']
    for rq_col in required_standards_columns:
        if rq_col not in reader_standards.fieldnames:
            ValueError(
              "Required column name '%s' is not in %s" 
              % (rq_col, repr(ds_standards)))
                               
    # Read and set up the standards info for this grade, form, subject.
    # Dict rawscorevar_ctable: (1) key is a rawscore variable name in the 
    # data input file and (2) value is conversion table for it.
    rawscorevar_ctable = OrderedDict()

    num_standards = 0;
    standard_raw_varnames=[]
    standard_total_varname=""
    for idx, row_std in enumerate(reader_standards):
        # row_std is next row in semantic workbook, sheet standards
        # use only those rows with/for given grade, subjet, form
        std_grade = row_std[std_grade_column].lower()
        std_subject = row_std['subjects'].lower()
        std_form = row_std[std_form_column].lower()
         
        if (  ((grade != std_grade) and (std_grade != "all") )
           or ( form != std_form)
           or ( subject != std_subject)
           ):
            # Skip irrelevant row for these params
            continue
            
        # This standards(standard form) row, via composite columns location
        # and sheetname, identifies a conversion sheet/table (CT).
        # The CT is used to convert/map input data values for the given
        # subject, grade and form in the named 'raw_score_variable' to 
        # output values for any variable named in scaled_score_variable', 
        # 'level_variable', 'label_variable', 'stderror_variable')
        std_orig_location = row_std[std_location_column]
        
        # Revise location using odict_loc_subs
        std_location = std_orig_location
        if odict_loc_subs is not None:
            for key, val in odict_loc_subs.iteritems():
                print (
                  "Calling re.sub(varname_pattern='%s', "
                  "replacement='%s',location_value='%s'" 
                  % ( key, val, std_location))
                #test_str = locsub(varname=key, replacement=val, origstring=test_str)
                std_location = re.sub(key, val, std_location)
                print ("New location string='%s'" % std_location)
        std_sheetname = row_std['sheetname']

        print (
          "\nStandard idx=%d, std_orig_location='%s',\n"
          "and std_location='%s', sheetname='%s'" 
          % (idx, std_orig_location, std_location, std_sheetname))
        
        std_raw_name = row_std['raw_score_variable']
        
        # For sumcheck outputs: manage standard raw and total variables
        if std_sheetname == "TO":
            standard_total_varname = std_raw_name
        else:
            standard_raw_varnames.append(std_raw_name)
        print ("Reading standard at idx=%d, std_raw_name='%s', sheetname='%s'"
          % (idx, std_raw_name, std_sheetname))
        
        # Set up and read conversion table for this standard (grade,subject,form).
        ds_ct = Dataset(dbms='excel_srcn', workbook_file=std_location, 
          sheet_name=std_sheetname,open_mode='rb')
        print (
          "For standard idx=%d, Reading Conversion Table/Sheet - ds_ct=%s"
          % (idx,repr(ds_ct)))

        ct_reader = ds_ct.DictReader()
        print ("ct_reader = %s" % (repr(ct_reader)) )
        
        # Interesting column names for preparing conversion dictionaries
        standard_float_names = [
          'scaled_score_variable', 'level_variable','stderror_variable']
        standard_str_names = ['label_variable']
        std_names = standard_float_names + standard_str_names
        
        conversion_float_names = [
          'scalescore','proficiencylevel','unroundedstandarderrorss']
        conversion_str_names = ['proficiencylabel']
        conv_names = conversion_float_names + conversion_str_names
        
        required_ct_names = conv_names
        for rq_col in required_ct_names:
            if rq_col not in ct_reader.fieldnames:
                raise ValueError(
                  "Required column '%s' not in conversion table %s" 
                  % (rq_col, repr(ds_ct)))

        # Dict ctrawval_converions: 
        # (1) key is a CT raw_score value (eg values 0.0, 0.5...)
        # (2) and value is inpcvar_cval. 
        # Ideally, the CT keys values have been populated to cover all 
        # possible values encountered in the raw input file for the referring 
        # input raw score variable.
        ctrawval_conversions = OrderedDict()
        if rawscorevar_ctable.get(std_raw_name):
            raise ValueError(
              "Standards row idx has duplicate raw_score_variable name ='%s'" 
              % (idx, std_raw_name) )
        #For this key of std_raw_name save the main conversions table
        rawscorevar_ctable[std_raw_name] = ctrawval_conversions
        print ( 
          "\nFor standard idx=%d, Set rawscorevar_ctable for key( "
          "std_raw_name)='%s' to dict ctrawval_conversions.\n" 
          % (idx, std_raw_name))
        print (
          "Populating ctrawval_conversions with keys of rawscorevalues, "
          "each value with dict with 4 conversions:") 
        ct_reader = ds_ct.DictReader()
        print ("ct_reader = %s" % (repr(ct_reader)) )

        for (idx_ct, row_ct) in enumerate(ct_reader):
            # For dict inpcvar_cval key is the data input's outvar name for a
            # converted value and value is the conversion table value.
            inpcvar_cval=OrderedDict()
            ctrawval = str(row_ct['rawscore'])
            #reject duplicate value
            if ctrawval_conversions.get(ctrawval):
                raise ValueError(
                  "Conversions table row idx %d has duplicate rawscore ='%s'" 
                  % (idx_ct, ctrawval) )
            
            ctrawval_conversions[ctrawval] = inpcvar_cval
            """print (
              "ctrawval_conversions: idx_ct=%d, set key ctrawval = '%s'" 
              % (idx_ct, ctrawval))"""
            for (std_conv_name, ct_conv_name) in zip(std_names, conv_names):
                # inpvcvar_cval: set key as the ultimate input data column
                # name for an 'output' conversion variable, 
                inpcvar = row_std[std_conv_name]
                # and value is the correct string conversion value for it.
                cval = row_ct[ct_conv_name]
                inpcvar_cval[inpcvar] = cval
                
                """
                print (
                  "inpcvar_cval: using base std_conv_name=%s, set key "
                  "inpcvar=%s, (ct_conv_name=%s), val cval=%s" 
                    % (std_conv_name, inpcvar, ct_conv_name, cval))
                """
        print ("Got %d Conversion Sheet rows" % idx_ct)
        
        print ( 
          "standards idx %d, grade=%s, subject='%s', form='%s', location='%s'"
           % (idx,std_grade,std_subject,std_form,std_location))
        num_standards += 1
    # end - for idx,row_std in enumerate(reader_standards) 
    print ("%s: Found %d relevant standards rows" % (me,num_standards))
    if num_standards < 1:
        raise ValueError(
          "With grade='%s',subject='%s',form='%s',\n"
          "standards sheet '%s' has no rows. "
          % (repr(grade),repr(subject),repr(form), repr(ds_standards) )
          )
        return
    if (standard_total_varname ==""):
        raise ValueError(
          "With grade='%s',subject='%s',form='%s',\n"
          "standards sheet '%s',\nis missing a row"
          " having a total 'raw_score_variable' with 'sheetname' = 'TO'.\n"
          % (repr(grade),repr(subject),repr(form), repr(ds_standards) )
          )
        return
       
    print ("Finished ingesting conversion table information. "
           "Got num_standards=", num_standards)
    
    ###########################################################
    
    print (
      "Reading and processing input Dataset ds_raw_scores=%s" 
      % repr(ds_raw_scores))
    # Print header row for ds_out:
    prefix = ","
    output_header="obs"
    for rsvar,ctable in rawscorevar_ctable.iteritems():
        output_header += prefix + rsvar
        for rval, cvtable in ctable.iteritems():
            for cvar in cvtable.keys():
                if cvar != "":
                    output_header += prefix + cvar
            # just needed the conversion var names for the first rval key
            break
            
    # set up two output datasets for sumcheck processing
    
    sumcheck_output_fieldnames = (
      ['id','obs'] + standard_raw_varnames + [standard_total_varname] +  
      ['correct_total']) 
    print "sumcheck_output_fieldnames = %s" % repr(sumcheck_output_fieldnames)
    writer_sumcheck_out = (
      ds_sumcheck_out.DictWriter(sumcheck_output_fieldnames))
    writer_sumcheck_out.writeheader()
    d_sc_out = OrderedDict()

    sumcheck_report2_fieldnames = ['id','obs','message']
    print ( 
      "sumcheck_report2_fieldnames = %s" % repr(sumcheck_report2_fieldnames))
    writer_sumcheck_report2 = (
      ds_sumcheck_report2.DictWriter(sumcheck_report2_fieldnames))
    writer_sumcheck_report2.writeheader()
    d_sc_r2 = OrderedDict()
    n_sum_error = 0
    
    # Set up two output datasets for conversion processing
    output_fieldnames = output_header.split(',')
    print "converter_out.csv fieldnames: %s" % repr(output_fieldnames)
    writer_out = ds_out.DictWriter(output_fieldnames)
    writer_out.writeheader()
    # report2
    report2_fieldnames=['id','obs','message']
    writer_report2 = ds_report2.DictWriter(report2_fieldnames)
    writer_report2.writeheader()

    # Get reader_input for the main input dataset with raw_scores    
    reader_input = ds_raw_scores.DictReader()
    """for (idx,cn) in enumerate(reader_input.fieldnames):
        print "Input data column index %d, column_name='%s'" % (idx,cn)
    """
    n_input = 0
    # Conversion output dicts. 
    d_r2 = OrderedDict()
    d_out = OrderedDict()
    n_error = 0
    n_sumcheck = 0
    for (idx, row_input) in enumerate(reader_input):
        in_obs = str(int(float(row_input['id'])))
        d_sc_out['obs'] = in_obs
        d_sc_r2['obs'] = in_obs
        d_r2['obs'] = in_obs
        d_out['obs'] = in_obs
        
        # Allow a common data error.
        in_grade = str(row_input['grade']).replace("'","")
        if grade != 'all':
            try:
                in_igrade = int(float(in_grade))
            except:
                message = (
                  "Error: Skipping input row. Could not convert "
                  "in_grade='%s' to float" 
                  % (str(in_grade)))
                d_r2['message'] = message
                n_error += 1
                d_r2['id'] = str(n_error)
                writer_report2.writerow(d_r2)
                continue
        if ( grade != 'all' and in_igrade != igrade ):
            # Normal operation. Skip rows with non-matching grade value.
            continue
        in_ssid = row_input['ssid']
        
        if (in_ssid is None or in_ssid == ""):
            # Warn for missing ssid, but can continue processing.
            d_r2['message'] = ( "Warning: Missing ssid value.")
            n_error += 1
            d_r2['id'] = str(n_error)
            writer_report2.writerow(d_r2)
        n_input += 1 
        
        # First, dispatch with the relatively simple sumcheck processing.
        total_raw = 0.0
        badrow = 0
        # Summate the input row's scores for the content strands (standards)
        for vn in standard_raw_varnames:
            strval = row_input[vn]
            d_sc_out[vn] = strval
            try:
                total_raw += float(strval)
            except:
                badrow = 1
                d_sc_r2['message'] = (
                  "Error: Could not convert input variable='%s' value '%s' "
                  "to float" % (vn, strval))
                n_sum_error += 1
                d_sc_r2['id'] = str(n_sum_error)
                writer_sumcheck_report2.writerow(d_sc_r2)
                # Simply do not sum anything more for this total value
                continue
        if badrow == 1:
            # could not compute 'correct' total from input data, and an error
            # was already written, so skip further sumcheck processing
            pass
        else:
            # got a good total_raw value, so use it for further processing.
            n_sumcheck += 1
            d_sc_out['id'] = str(n_sumcheck)
            d_sc_out['correct_total'] = str(total_raw)
                    
            # standard_total_varname is supposed to have the correct total
            d_sc_out[standard_total_varname] = (
              str(float(row_input[standard_total_varname])))
            # If input has incorrect total, report a sumcheck error.
            if float(row_input[standard_total_varname]) != total_raw:
                n_sum_error += 1
                d_sc_r2['id'] = str(n_sum_error)
                d_sc_r2['message'] = (
                  "Error: Input total varname='%s', total value ='%s' "
                  "but correct total='%s'"
                  % (standard_total_varname,
                  row_input[standard_total_varname], str(total_raw)))
                writer_sumcheck_report2.writerow(d_sc_r2)
        # Write the 'correct' output, regardless of any errors on input
        writer_sumcheck_out.writerow(d_sc_out)
        # Done with sumcheck
        
        # For registered raw_score variables (from standards sheet), find 
        # conversion values and check for mismatches in data.
        # Setup and output first line of var names:
        row_cval_errors = 0
        row_skip_errors = 0;
        prefix=","
        for (rawscorevar, ctrawval_conversions) in rawscorevar_ctable.iteritems():
            # Get normalized string for raw_score
            try:
                raw_score = str(float(row_input[rawscorevar].strip()))
            except:
                d_r2['message'] = ( 
                  "Error: rawscore variable '%s' value '%s' "
                  "is not a float. Skipping."
                  % (str(rawscorevar), str(row_input[rawscorevar])) )
                n_error += 1
                d_r2['id'] = str(n_error)
                writer_report2.writerow(d_r2)
                row_skip_errors = 1
                continue
            # Report an error message if rawscore value is not in the
            # conversions table
            if ctrawval_conversions.get(raw_score) is None:
                d_r2['message'] = (
                  "Error: raw_score_var = '%s' value = '%s' "
                  "has no conversion data."
                  % (rawscorevar, raw_score))
                n_error += 1
                d_r2['id'] = str(n_error)
                writer_report2.writerow(d_r2)
                row_skip_errors = 1
                continue
                
            # We have conversion data for this data row's rawscore 
            # variable and score
            d_out[rawscorevar] = raw_score
            inpcvar_cval = ctrawval_conversions[raw_score]
            # For each conversion var with a value, calculate the input 
            # row's value.
            # Check proper float conversion values
            # for (conv_var_name) in conversion_float_names:
            for (inp_cvar_name, conv_value) in inpcvar_cval.iteritems():
                if not inp_cvar_name:
                    # This is OK.
                    continue
                if conv_value is None:
                    d_r2['message'] = ( 
                     "WARN:rawvar=%s, no inpcvar_cval entry for '%s'. "
                     "Skipping." 
                      % (rawscorevar, inp_cvar_name))
                    n_error += 1
                    d_r2['id'] = str(n_error)
                    writer_report2.writerow(d_r2)
                    row_skip_errors = 1;
                    continue
                    
                # "Normalize" the string for this float value
                try:
                    conv_value = str(float(conv_value))
                except ValueError:
                    raise ValueError(
                      "idx %d, column %s value=%s is not a float." 
                      % (idx, inp_cvar_name,conv_value))
                    
                d_out[inp_cvar_name] = conv_value
                #"Normalize" the string data value
                data_value = row_input[inp_cvar_name]
                data_value = str(float(data_value))
                #Detect conversion value error in data
                if data_value != conv_value:
                    d_r2['message'] = ( 
                      "Variable '%s' data value is '%s' but correct "
                      "value is '%s'"
                      % (inp_cvar_name, data_value, conv_value) )
                    n_error += 1
                    d_r2['id'] = str(n_error)
                    writer_report2.writerow(d_r2)
                    row_cval_errors += 1
            # end loop for inpcvar_name 
        #end loop for rawscorevar
                     
        if (row_skip_errors > 0):
            continue
        writer_out.writerow(d_out)

    print (
      "%s: Done processing %d data input rows from the ds_raw_scores dataset."
      % (me, idx+1) )
    print (
      "See (1) ds_out='%s' \nand (2) ds_report2='%s'" % (ds_out, ds_report2))
    return


if __name__ == '__main__':
    print "Start local testing"
    # Unittest raw_score_converter
    import os
    home = os.path.expanduser("~")+ os.sep
    tddir = home+"testdata/converter/"
    cc_input_csv = tddir+"OGT_ConversionCheck_exp_conv_check.csv"
    out_filename = tddir+"output/converter_out.csv"
    report2_filename = tddir+"output/converter_report2.csv"
    sumcheck_out_filename = tddir+"output/sumcheck_out.csv"
    sumcheck_report2_filename = tddir+"output/sumcheck_report2.csv"
    #sem_xls = tddir+"OGT_Semantic.xls"
    #and arg ctpath was ctpath=tddir+"2012 March/"
    workbook_file = tddir+"20130613/OGT_Semantic_20130613_from_vijay.xls"
    
    ds_standards = Dataset(dbms='excel_srcn', workbook_file=workbook_file, 
      sheet_name="Standards", open_mode='rb')
    print "Using ds_standards = '%s'" % repr(ds_standards)
    ds_input = Dataset(name=cc_input_csv,open_mode='rb')
    print "Using ds_input = '%s'" % repr(ds_input)
    ds_sumcheck_out = Dataset(name=sumcheck_out_filename,open_mode='wb')
    ds_sumcheck_report2 = Dataset(name=sumcheck_report2_filename,open_mode='wb')
    ds_out = Dataset(name=out_filename,open_mode='wb')
    print "Using ds_out = '%s'" % repr(ds_out)
    ds_report2 = Dataset(name=report2_filename,open_mode='wb')
    print "Using ds_report2= '%s'" % repr(ds_report2)
                    
    print "Calling converter(...)"
    rv = raw_converter(grade='10', subject="Math", 
         ds_raw_scores=ds_input,
         ds_standards=ds_standards,
         ctpath=tddir+"20130613/",
         ds_sumcheck_out=ds_sumcheck_out,
         ds_sumcheck_report2=ds_sumcheck_report2,
         ds_out=ds_out, ds_report2=ds_report2)
    print "Done local testing"
    