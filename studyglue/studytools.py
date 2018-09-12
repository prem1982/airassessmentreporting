from airassessmentreporting.datacheck.dataset import *
from collections import OrderedDict
from titlecase import titlecase
import re
def composite_value(row=None, column_names=None, delim=None):
    """ 
    Return a composite of the row's string values ordered in column_names.

    Params:
    ======
    row: Dict
    ----------
    Row is a dictionary of string column_name keys and paired string values.

    column_names: List of strings
    ------------------------------
    One or more strings in the list, where each is a column name used 
    in the row param.

    delim: String
    --------------
    A string used to delimit multiple values in the composite concatenated 
    composite value.
     
    """
    if column_names is None:
        raise ValueError("Column_names must not be None")
    if delim is None:
        delim=','
    cur_delim = ""
    comp_val = ""
    for cname in column_names:
        #print "cname=%s" % cname
        #print "row[cname]=%s" % row[cname]
        colval = "" if row[cname] is None else row[cname]
        comp_val = comp_val + cur_delim + colval
        cur_delim = delim
    return comp_val

class DictCut(object):
    def __init__(self, fieldnames=None, key_columns=None, keep_columns=None,
      no_dups=None, delim=None):
        """
        Make a dict from a row with key_columns as key, keep_columns as value. 
        
        When considering a table as a 'rectangle' of horizontally-distinct rows 
        and vertically-distinct columns, a 'cut' is a selection of vertical columns.
        
        See method add() which interprets an input row of data and adds an entry to
        this dictionary from the row's data.
        
        Parameters:
        ===========
        Fieldnames: List of strings
        ----------------------------
        --This is a list of column names for a data relation.
        
        key_columns: list of strings
        ----------------------------
        -- Each string is a column name in param fieldnames.
        
        keep_columns: list of strings
        -----------------------------
        -- list of zero or some column_names, all in fieldnames.
        -- The value of each dict entry will be a composite key of the column values.
        
        TODO: could simplify and speed this up without changing the interface by managing
        and storing tuple values instead of constructing composite values. May be worth
        doing if needed to apply this for millions of input data rows, probably not if keep 
        just using this to encode data in smaller excel files under 10 thousand rows.
        
        """
        self.nkey = 0
        self.nkeep = 0
        self.key_columns = key_columns
        self.keep_columns = keep_columns
        self.no_dups = no_dups
        self.delim = ',' if delim is None else delim
        if (key_columns is None and keep_columns is None):
            raise ValueError("Either or both key_columns or keep_columns must be given")
        if key_columns is not None:
            for cn in key_columns:
                self.nkey += 1
                if cn not in fieldnames:
                    raise ValueError("Key column '%s' not in input fieldnames" % cn)
        if keep_columns is not None:
            for cn in keep_columns:
                self.nkeep += 1
                if cn not in fieldnames:
                    raise ValueError("Keep column '%s' not in input fieldnames" % cn)
            
        self.num_entries = 0
        self.odict = OrderedDict()
        
    def add(self,row):
        """ Add an entry to this dict from the data in this row.
        
        If key_columns is None, the entry's key is a serial number of the added entry,
        otherwise the entry key is a composite value of the delim-separated string of the column values of 
        the columns named in parameter key_columns,
        
        The entry's value is the composite value of the row values of the keep_columns.
        
        """
        if self.key_columns is not None:
            composite_key = composite_value(
              row=row, column_names=self.key_columns, delim=self.delim)
            if self.odict.get(composite_key, None) is not None:
                if self.no_dups:
                    raise ValueError(
                      "Duplicate key error for key_columns=%s, value=%s" 
                      % (repr(self.key_columns), repr(composite_key)))
                # Duplicate key is OK here. Just return.
                return
        else:
            # just use string value of num_entries as key
            self.num_entries += 1
            composite_key = str(self.num_entries)
            
        # Store the "composite_keep" value, the composite value of the keep_columns
        composite_keep = ""
        if self.keep_columns is not None:
            composite_keep = composite_value(
                row=row, column_names=self.keep_columns, delim=self.delim)
        #print "dcut.add(): composite_keep=%s" % composite_keep
        self.odict[composite_key] = composite_keep
        return
    
    def write(self, dsw=None, verbose=None):
        """
        Write this DictCut dictionary to an output (writable) dataset.
        
        """
        # Make list of output row column_names
        column_names = []
        if self.key_columns:
            column_names.extend(self.key_columns)
        else:
            column_names.extend(["id"])
        if self.keep_columns:
            column_names.extend(self.keep_columns)
            
        size = len(self.odict)
        
        # Create an output dataset writer with the key and keep column names 
        # of dictionary self.odict.
        dw = dsw.DictWriter(column_names=column_names)
        # Register the column names to the output dataset
        dw.writeheader()
        # Output each data row
        orow = OrderedDict()
        n_entry = 0
        for key in sorted(self.odict):
            value = self.odict[key]
            n_entry += 1
            # Variable key is the composite value of the 'real' dataset key 
            # column values, named in self.key_columns.
            # Variable value is the composite value of the self.keep_columns.
            # Set the output row dict entry for this key to this value.
            key_values = key.split(self.delim)
            if self.key_columns:
                for idx,key_column in enumerate(self.key_columns):
                    orow[key_column] = key_values[idx]
            else:
                orow["id"] = str(n_entry)
            
            # Add the output row dict entries for keep_columns and values
            keep_values = value.split(self.delim)
            if self.keep_columns:
                for idx,keep_column in enumerate(self.keep_columns):
                    orow[keep_column] = keep_values[idx]
            # write the output row
            if verbose:
                print "dcut.write():Writing row='%s'" % repr(orow)
            dw.writerow(orow)    
        del dw
        return size
     
def DictRows(dsr=None, key_columns=None, key_delim=','):
    """ Read all the rows of a dsr dataset into a new dict and return it.

    For each row in the dataset, the dictionary key is the row number if key_columns
    is none, else it is the composite value of the column(s) named in parameter 
    key_columns.
    
    The dictionary key's matching value is an odict of all the 
    column_name:value pairs in the row.

    If multiple key_columns has multiple names, the key is a concatenation of the 
    ordered columns' string values, delimited by key_delim
    """
    if dsr is None:
        raise ValueError("Paramater dsr (Dataset) is required.")
    odict = OrderedDict()
    dr = dsr.DictReader()
    if key_columns is None or len(key_columns) == 0:
        # Assume the user wants to use keys of string values of 1 . . . N.
        # May add a param later to use integer key values if needed.
        nrows = 1;
        for row in dr:
            odict[str(nrows)] = row.copy() 
            nrows += 1
    elif len(key_columns) == 1:
        for row in dr:
            odict[row[key_columns[0]]] = row.copy()
    elif len(key_columns) > 1:
        for row in dr:
            # Create a keyval for multiple key_columns for the odict
            # composite_value(row,key_columns,key_delim)
            key_val = composite_value(row, key_columns, key_delim)
            odict[key_val] = row.copy()
           
    return odict        

def DictExcelSheet(file_name=None,sheet_name=None,keys=None,verbose=None):
    """
    Convenience function to create a dict from an excel sheet.
    """
    dsr = Dataset(
      open_mode='rb', dbms="excel_srcn", workbook_file=file_name, 
      sheet_name=sheet_name)
    dict_out = DictRows(dsr, key_columns=keys)
    if verbose and (verbose==True or verbose==1):
        for key, value in dict_out.items():
            print ("key='%s',\n\tvalue='%s'" % (key, value))
    return dict_out

def proper_case(name=None):
    """ 
    Return a normalized string for a name or title.

    The case of words is adjusted fairly well to look like the 
    English-customary letter-cases used for a book title or a person's name.

    This routine accommodates a string of multiple space-separated 
    'base_words'. 
    For example, a base_word can be a first, middle, or last name or a title 
    like "M.D." that is further parsed into 'words' and handled at a lower 
    level so capitalization can be preserved when there is no space given 
    a period.
    """
    if name is None:
        return None
    rx_split = re.compile(r'\s*')
    rx_roman = re.compile(r"^(i|ii|iii|iv|v|vi|vii|viii|ix|x),*$")
    # re per stack overflow-
    # http://stackoverflow.com/questions/12683201/python-re-split-to-split-by-spaces-commas-and-periods-but-not-in-cases-like
    #for name in names:
    new_name=""
    base_word_prefix=""
    # sas line 60. Insert space after comma before splitting into 
    # separated base_words.
    # also start as lower case and convert select characters to upper.
    name = name.lower().replace(",",", ")
    base_words = rx_split.split(name.lower().strip())
    for base_word in base_words:
        # Parse title-abbreviations 'words' from base words like 'm.d.' 
        # and 'ph.d.'
        words = base_word.split('.')
        word_prefix = ""
        new_base_word = ""
        #nwords = len(words)
        for word in words:
            if re.match(rx_roman, word):
                #print "Got roman %s" % word
                word = word.upper()
            else:
                word = titlecase(word)
            new_base_word += word_prefix + word
            word_prefix = '.'
        new_name +=  base_word_prefix + new_base_word
        base_word_prefix = ' '
    #rint("new_name='%s'" % (new_name))
    return new_name
# end def proper_case(name=None):

def get_dict_sgf_bookmap(dsr_bookmaplocs=None, subject=None, grade=None, 
    form=None, odict_loc_subs=None, verbose=None, 
    column_subject='subject', column_grade='grade', column_form='form',
    column_location='bookmap_location'
    ):
    """
    Return a dict of bookmaps keyed by [Subject, Grade, Form] strings. 

    If any or all of parameters subject, grade, or form are specified, then 
    include entries for only the bookmaps with matching parameter values.

    Note: This code is based on code copied from resorecheck.py so it can be
    generalized for common use by rescorecheck() and newer function 
    study_glue(), modified slightly to  allow non-specification of values 
    parameters subject, grade, form to allow reading of more bookmap data 
    at once.
    
    Parameters:
    ===========

    dsr_bookmap_loc: dataset
    ------------------------
    - required parameter
    - An input dataset of bookmap locations information (location, grade, 
      subject, form)

    odict_loc_subs : ordered dictionary 
    -----------------------------------

    - optional parameter

    - Key is a substring value that, if found in a bookmap_location "location" 
      value, will be replaced with the dictionary value, so the value is 
      used as a substitution string.

    - The substitutions will be attempted in order so be careful. 
      If you want to change key 'cat' to value 'dog' and key 'catch' to 
      value 'throw' within a location value, 
      be sure to set odict_loc_subs['catch'] ='throw' before setting
      odict_loc_subs['cat'] = 'dog'. 

    subject: string
    ---------------------------------------
    - Optional parameter

    - Traditional values found in bookmaps are: 'Reading', 'Math', 'Writing', 
      'Science', 'Soc_stud'. Upper or lower case may be given.

    - The first character of this string is lower-cased and compared with
    the lower-cased value in each row of the bookmap_locations dataset for 
    column 'subjects'.

    - Only bookmap location rows with the given subject value as a row's 
    column subjects value are processed.

    grade: string
    -------------
    - Optional parameter

    - Values '10', through '15' are traditionally used. 

    - Only bookmap location rows with the given grade, OR with the special 
      values of 'all' or 'g' are considered a match with the grade parameter 
      value.

    form: string
    ------------
    - Optional parameter

    - Values typically are '1', '2', 'SV', 'A', 'B'

    bml_form_style: string
    ----------------------
    - When bml_form_style is '0' or None, (1) The bookmap locations dataset, 
      field 'form', must have values like: "FormX::Y:"SSYY" where:
      -- (a) :: is a delimiter that separates the values for X, Y and SSYY, 
      -- (b) and where X is a string value for form, after any leading '0' 
         has been discarded, 
      -- (c) and where SSYY is an admin_term value like "SP12" indicating 
         spring of 2012. 

    - When bml_form_value value is 1' the bookmap_locatations dataset 'form' 
      field has simply '1','2','SV','A' or 'B' or possibly other values 
      to check.
   
    admin_term: string
    ------------------
    - When bml_form_style is 0 or none, this is required, otherwise it is 
      ignored

    - If style is 0, this parses and matches 'admin_term' parameter value 
      (eg "SP13" for spring 2013) in the bookmap_locations dataset and 
      picks only rows with a match there. 
    
    columns_lsgf: list of strings
    -----------------------------
    - Optional parameter
    - If not given, coumn names [ 'location', 'grade', 'subject', 'form' ] 
      will be sought in the bookmap locations file.
    - If given and any name is the empty string, the default name above will 
      be used, but any given column name will be used instead, according to 
      position.
    - example column_names=['','grade_values','',''] will require column 
      names location, grade_values, subjects, form in the bookmap locations 
      file.

    Return value: Ordered Dictionary
    ================================

    Key: list of strings for subject, grade, form, admin_term
    ----------------------------------------------------------
    Example: Tuple ('reading', '10', 'SV') would be a key in the returned 
    dictionary, with the associated value being the bookmap dictionary for
    the bookmap that is identified in the bookmap_locations dataset.
    
    Value: BookMap Dictionary
    -------------------------
    A value is a Dictionary object that has the contents of a bookmap, where:

    - A dictionary key value is string value of an integer 'position', aka 
      item id, for a bookmap item
    - A bookmap dictionary entry value is a dictionary with 
      -- key of column_name, 
      -- value of column_value string 
      -- as found in the bookmap info file for the row with the given integer 
         position, aka item id.

    Selected Entries:
    -----------------

    If no parameters for subject, grade or form were supplied, no selection 
    filters were used to restrict the bookmaps, so all bookmaps will exist 
    in the dictionary.
    If, for example, form was restricted to '2' and subject to 'Writing', 
    then only entries with whose keys match specified parameter value will 
    exist in the BookMap dictionary.

    Key: String
    -----------
    - If parameter "form" is not supplied, the key is a "Form" string value, 
      where traditional form value examples are '1','2','A','B', 'SV'.
    - If neither parameter form nor subject is supplied, they key is a 
      composite value of a single-letter for subject (eg: r,m,w,s,c) followed 
      by a comma then the form value.
    - Other combinations of allowable None values for subject, grade, form 
      may be added with minor code revisions, to support different composite 
      key values.
    - The order of the keys follows the order of rows in the bookmap 
      locations file

    Value: Dictionary
    -------------------

    NOTES:
    ======
    During development, a special style of bookmap (bml_form_style=0) was 
    provided for testing.
    However it has not recurred in 2013 client OGT integration testing, 
    so code is left in place as a reminder, but the parameters 'admin_term' 
    and bml_form_style are semi-retired pending a revival of need for them 
    or ultimate removal.

    """
    iam = "get_dict_sgf_bookmap()"
    # bml_form_style may always be 1, so semi-retired that param and 
    # admin_term
    bml_form_style = 1
    # Get reader for bookmap locations (bml) worksheet. 
    reader_bml = dsr_bookmaplocs.DictReader()
    
    # Per Datacheck manual section 2.4.2, required columns.
    required_columns = [
      column_location, column_subject, column_grade, column_form ]
    for rq_col in required_columns:
        if rq_col not in reader_bml.fieldnames:
            ValueError(
              "Required column name '%s' is not in %s" 
              % (rq_col, repr(dsr_bookmaplocs)))
       
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
        admin_term = ""
 
    # Read and set up the bookmaploc info for this grade and subject 
    # (already should be constant through input rows) and form_id 
    # ( a value in the input row).
    # Dict d_ckey_bookmap: 
    # (1) Key is list of values [Subject, Grade, Form] (SFG). 
    # and (2) Value is the bookmap ordered dict keyed by item position
    d_sgf_bookmap = OrderedDict()

    num_bml = 0;
   
    for idx_bml, row_bml in enumerate(reader_bml):
        # For any bookmap selection parameters, limit the bookmaps for them
        
        bml_grade = str(row_bml[column_grade]).lower().strip()
        bml_subject = (
          row_bml[column_subject].lower().replace(" ","").replace("_","")
          .strip())
        bml_form = row_bml[column_form].lower().strip()
        
        if grade is not None:
            # Ensure that any bookmap matches the given grade
            # Also allow "wildcard" bookmap matches: ('all', and 'g')
            if (  (    (grade != bml_grade) 
                   and (bml_grade != "all") 
                   and (bml_grade != "g" )
                  )
               ):
                continue
        # possible select filter is subject
        if subject is not None:
            if  subject != bml_subject:
                continue
        # Last possible select filter is form
        if form is not None:
            if form != bml_form:
                continue
        # We will store a dictionary for this bookmap. Compose the key from the
        # key columns that were not used as selection criteria.
        # 
        key_sgf = (bml_subject, bml_grade, bml_form)
        # Form field
        if bml_form_style == 0:
            bml_form_fields = bml_form.split("::")
            if verbose:
                print (
                  "idx_bml=%d,formfields='%s'" 
                  % (idx_bml, repr(bml_form_fields)))
            
            if ( len(bml_form_fields) != 3 ):
                raise ValueError(
                  "Dataset='%s', row %d, field form='%s' has %d subfields, "
                  "not 3."
                  % ( repr(dsr_bookmaplocs), idx_bml + 2, bml_form, 
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
                if verbose:
                    #skip irrelevant row
                    print (
                      "\nSkipping bookmaplocations row id=%d, admin_term='%s'"
                      " but bml_admin='%s'\n"
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
        if verbose:
            print ( "idx_bml=%d, grade='%s',subject='%s',form='%s'" 
                  % (idx_bml, bml_grade, bml_subject, bml_form))

        bml_location = row_bml[column_location]
        # Revise location using odict_loc_subs
        if odict_loc_subs is not None:
            for key, val in odict_loc_subs.iteritems():
                if verbose:
                    print (
                      "Calling re.sub(varname_pattern='%s', "
                      "replacement='%s',location_value='%s'" 
                      % (key, val, bml_location))
                    #test_str = locsub(varname=key, replacement=val, 
                    # origstring=test_str)
                    bml_location = re.sub(key, val, bml_location)
                    print ("New location string='%s'" % bml_location)
            
        if verbose:
            print "idx_bml=%d, row='%s'" % (idx_bml, repr(row_bml) )
            
        # For this row_bml's bml_form_id as the key, create a formid entry
        # whose value is a dictionary named itemid_info. 
        # The itemid_info dictionary key is an itemid and the info value 
        # is an OrderedDict of bookmap column-value pairs.
        itemid_info = OrderedDict()
        # Consider: raise error here if key bml_formid already exists.
        d_sgf_bookmap[key_sgf] = itemid_info
        
        # Populate the itemid_info dictionary from the bookmap. 
        # First, init the dataset and reader for this bookmap.
        dsr_bookmap = Dataset(dbms='excel_srcn', workbook_file=bml_location,
          sheet_name="BookMap", open_mode='rb')
        reader_bookmap = dsr_bookmap.DictReader()
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
                  % (rcol, repr(dsr_bookmap)))
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
        if verbose:
            print info
            print (
              "\n%s: Bookmap idx=%d, read%d rows from dsr_bookmap='%s'" 
              % (iam, idx_bml, bm_num_rows, repr(dsr_bookmap)))

    # end: for idx_bml, row_bml in enumerate(reader_bml):    
    return d_sgf_bookmap
    
def get_dict_sgfl_bmids(d_sgf_bookmap=None, verbose=None):
    """
    From d_sgf_bookmap, return lists of selected bookmap ids.

    This function is intended for the caller to call this once before 
    starting to process millions of rows of input data instead of 
    calling it once for each of those input rows.

    A bookmap id is a bookmap position, assumed here to match an 
    item (test item question) id.

    Return Value: Dictionary
    
    =========================
    Key values are 4-tuples of (subject, grade, form, varname) (sgfl) , 
    where: 
    (1) the first 3 key-tuple values are copied from the d_sgf_bookmap 
        parameter, and
    (2) the last tuple value,  key[3], is a 'list name' created in this
    function with a name to convey the contents of the dictionary value: 
    
    Dictionary values: Each value is a list of bookmap ids, also called item 
    positions, that appear in a bookmap file. The 'seleciton logic' used
    in this function is straightforward, so the relation of the "list name" 
    and the actual list contents is easy to deduce from the code.
    
    These lists are custom-created and designed to be created before processing
    millions of input rows and to to be accessed either:
     (1) during processing of each row or 
     (2) to compose an sql query to build a result
    set of rows to use as input rows for further processing - specifically for
    building means score statistics for select test items. 


    Each of the following "list name" values 
    below should/will appear once in a key[3] tuple value of the returned 
    dictionary.

    List name "items_mc_released" :
    -----------------------------
    - When key[3] is "mc_released" the dictionary value is a list of string 
      item ids that the bookmap info indicates are "tagged_for_release" and 
      not "operational" multiple choice items. 

    - Rule used: the dictionary value is a list of bookmap position items, 
      where a bookmap position's row in the bookmap has:
      (1) column "item_format" is 'MC'
      and (2) column "role" is not = "OPERATIONAL" 
      and (3) column "tagged_for_release" is 'YES'.

    List name "items_not_released": 
    ------------------------------------

    - Dictionary value is list of string ids for items for given subject, 
      grade, and form where column "tagged_for_release" value is not 'YES'
     
    List name "max_item_num" 
    ----------------------------
    -- Not implemented: Can implement as needed, but may not be needed. 
    -- The dictionary value is integer value of maximum item number
       existing in the bookmap for key's subject, grade, form. 

    Notes:
    ======
    - The bookmapreader.sas version also returns some other "keys" or named 
      lists of  positiions (item ids) that are not returned  here because 
      this python code is initally written to support 
      OGT/...code/superdata/glue.sas usage, and it does not use any other 
      named lists of bookmap item ids.
    - If other callers need other variable lists to be constructed, this 
      code is easy to modify to add them.
    - Also entries ending with 'set names' are stuffed into the return 
      dictionary, much like the 'list names', except they indicate that the
      dictionary values are sets instead of lists(thus no duplicates will be
      stored). The bookmaps have traditionally been tightly maintained to not
      include duplicate item ids, so use of set_names is probably not really
      necessary, but it's often not a bad idea to be too careful.

    """
    if verbose:
        me = "get_dict_sgfl_bmids()"
        print ("len of d_sgf_bookmap is %d" % len(d_sgf_bookmap) )
        
    for (subject, grade, form), dict_item_info in d_sgf_bookmap.items():
        print (
          "%s:key = %s, len of dict_id_info = %d" 
          % (me, (subject, grade, form,), len(dict_item_info)) )
        items_mc_released = []
        items_not_released = []
        set_mc_released = set()
        set_not_released = set()
        i_maxitem = 0
        for position, bm_row in dict_item_info.items():
            if int(position) > i_maxitem:
                i_maxitem = int(position)
            # check for item_not_released 
            if bm_row['tagged_for_release'].lower() != 'yes':
                # Remember: position is a string
                items_not_released.append(position)
                set_not_released.add(position)
            # check for mc_released 
            if ( bm_row['item_format'].lower() == 'mc'
                and bm_row['tagged_for_release'].lower() == 'yes'
                and bm_row['role'] != 'operational'
                ):
                items_mc_released.append(position)
                set_mc_released.add(position)
            # add code for other types of selected lists of positions
        # end - for dict_item_info.items():   
        # Sort the lists as strings of integers as a nicety.
        items_mc_released.sort(key=lambda x: [int(y) for y in x.split('.')])
        items_not_released.sort(key=lambda x: [int(y) for y in x.split('.')])
        if verbose:
            print ("items_not_released = %s" % repr(items_not_released))
            print ("items_mc_released = %s" % repr(items_mc_released))
            print ("set_not_released = %s" % repr(set_not_released))
            print ("set_mc_released = %s" % repr(set_mc_released))
            print ("max_item_num = %d" % (i_maxitem))
        
        # Save the lists in return dictionary
        dict_return={}
        dict_return[(subject,grade,form,'items_mc_released')] = (
            items_mc_released)
        dict_return[(subject,grade,form,'items_not_released')] = (
            items_not_released)
        dict_return[(subject,grade,form,'max_item_num')] = i_maxitem
        dict_return[(subject,grade,form,'set_mc_released')] = (
            set_mc_released)
        dict_return[(subject,grade,form,'set_not_released')] = (
            set_not_released)
    # end: for key,val in d_sgf_bookmap.items():
    return dict_return
# end def get_dict_sgfl_bmids()

def aggregation_vals_set(dict_ovar_val=None, dict_subject_cstrands=None,
  dict_bookmap=None, missing_value="",verbose=None):
    """
    This emulates OGT code in .../superdata/glue.sas lines 321-352
   
    Parameters:
    ===========
    dict_ovar_val: dictionary 
    - key is output var name, value is the variable's output value

    dict_subject_cstrand : dictionary
    - key is a subject abbreviation, value is a content strand abbreviation

    dict_bookmap: dictionary
    - key is x, value is y

    - missing_value: any
      a value to be used to detect missing value or to assign as a missing 
      value

    verbose: boolean
    - if true, misc log messages are printed to standard output
     

    Notes:
    ======
    -- glue.sas lines 323-326 functionality was already performed by 
       study_ogt(). 
    -- Maybe an oversight in glue.sas allowed this functionality to be 
       done twice.
    
    """
    # glue.sas lines 331 - 342
    # loop over subjects and evaluate variables in dict_bookmap to derive 
    # new aggregate variables.

    for sj, cstrands in dict_subject_cstrands.items():
        subject = sj.lower()
        try:
            grade = dict_ovar_val['grade']
            igrade = int(float(grade))
        except:
            grade = '99'
            
        key_name = "mc_released_%s_A_%s" % subject, grade
        for cs in cstrands:
            pass
        
    pass
# end def aggregation_vals_set()

def subject_extend_columns(dict_subject_cstrands=None):
    """
    Return new column names that will be added by subject_vals_set()
    """
    cnames = []
    for sj, cstrands in dict_subject_cstrands.items():
        # varnames use lowercase.
        subject = sj.lower()
        # level&sub_char : related glue.sas "level_vars",
        # see lines 238, 248, 257, 267, 
        for cs in cstrands:
            cnames += "up%s%slev" % (subject, cs)
            for dx in range(4):
                cnames += "up%s%sdum%d" % (subject,cs,dx)
        for k in xrange(1,6):
            #Init var_name (eg uprxdum3, to 0, per glue lines 278, 280
            cnames += "up%sxdum%d" % (subject, k)
#end def subject_extend_columns
    
def subject_vals_set(dict_ovar_val=None, dict_subject_cstrands=None,
  missing_value="", verbose=None):
    """
    Sets subject-level outputs for an input row.
    
    This is called once after every input row is read, and it sets many 
    per-subject output variables (and within a subject, per content-strand 
    variables) depending  on other values that have been read from each 
    input row and to input param's dict_ovar_val.

    This does the work of glue.sas lines 235-298.
    Also see function student_cs_vars_add() that is supposed to add many of 
    the new 'output variables' used here.

    Parameters:
    ===========
    dict_ovar_val : Ordered Dictionary
    ----------------------------------

    - Ordered dictionary to which the new key variable names, paired with 
      their initial values, will be added.

    - It is assumed that dict_ovar_val already has values for all the 
      variables that will also serve as input to this function. 

    dict_subject_cstramds : Ordered Dictionary
    --------------------------------------

    - dictionary listing subjects (eg: R, M, W, S, C)  as keys.

    - each value is a dictionary, guaranteed to have a key 'contentstrand' 
      with the value being a list of the contentstrand abbreviations that 
      apply for the subject.

    - Example: Traditionally, for subject 'R' the contentstrand has been 
      ['A', 'R', 'L', 'I', 'X'] where X is reserved as a special value for 
      some purposes, usually to mean no single strand but 'ALL' strands.

    """
    me = "subject_vals_set()"
    
    # First, set all cs-related variables to value 0.
    # See glue.sas lines 257-277: Warning - sticky code both there and here.
    for sj, cstrands in dict_subject_cstrands.items():
        # varnames use lowercase.
        subject = sj.lower()
        #level&sub_char : related glue.sas "level_vars" see lines 238, 248, 257, 267, 
        for cs in cstrands:
            if cs == 'x':
                continue
            var_stem = "up%s%s" % (subject, cs)
            var_name_lev = var_stem + "lev"
            val_lev = dict_ovar_val[var_name_lev]
            
            # Default all dum values to '0'
            for dx in range(4):
                var_name_dum= "%sdum%d" % (var_stem,dx)
                dict_ovar_val[var_name_dum] = '0'
            # or reset all or some depending on lev value.    
            if val_lev == missing_value:
                for dx in range(4):
                    var_name_dum= "%sdum%d" % (var_stem,dx)
                    dict_ovar_val[var_name_dum] = missing_value
            elif val_lev in ('1','2','3'):
                dict_ovar_val[var_stem + ("dum%s" % val_lev) ] = '100'
            if val_lev in ('3','2'):
                dict_ovar_val[var_stem + "dum4" ] = '100'
        # glue.sas code lines 278-283: 
        # manage subject-level vars up{subject}_xdum0, ... xdum4
        # value used in glue line 281
        xlev = dict_ovar_val['up%sxlev' % subject] 
        initial_value = missing_value if xlev == missing_value else 0
        # Loop over the variable name suffix integers 1-5
        for k in xrange(1,6):
            #Init var_name (eg uprxdum3, to 0, per glue lines 278, 280
            var_name = "up%sxdum%d" % (subject, k)
            dict_ovar_val[var_name] = initial_value
            
        if xlev in ('1','2','3','4','5'):
            var_name = "up%sxdum%s" % (subject, xlev)
            dict_ovar_val[var_name] = '100'
        # glue.sas lines 284-292 : more settings dependent on xlev value
        if xlev in ('3','4','5'):
            # glue lines 284, 291
            dict_ovar_val['proforhigher%s' % subject] = '100'
            dict_ovar_val['belowprof%s' % subject] = '0'
        elif xlev in ('1', '2'):
            # glue lines 285, 290
            dict_ovar_val['proforhigher%s' % subject] = '0'
            dict_ovar_val['belowprof%s' % subject] = '100'
        # glue line 286, 289, 292  
        elif xlev in ('', missing_value,'A'):
            dict_ovar_val['proforhigher%s' % subject] = missing_value
            dict_ovar_val['advacc%s' % subject] = missing_value
            dict_ovar_val['belowprof%s' % subject] = missing_value
        # could add else clause to print warning of xlev value.
            
        # glue line 287
        if xlev in ('4', '5'):
            dict_ovar_val['advacc%s' % subject] = '100'
        # glue line 288
        elif xlev in ('1', '2', '3'):
            dict_ovar_val['advacc%s' % subject] = '0'
        # glue line 294
        if (  int(float(dict_ovar_val['uf%sx_attempt' % subject])) != 1
           or int(float(dict_ovar_val['uf%sx_invalid' % subject])) == 1
           or dict_ovar_val['schtype'] == 'H'
           ):
            # glue line 295
            dict_ovar_val['inclusionflag%s' % subject] = '0'
        else: 
            # glue line 293
            dict_ovar_val['inclusionflag%s' % subject] = '1'
    # end - glue line 294
    return
# end def subject_vals_set()

def student_cs_vars_add(dict_subject_cstrands=None, verbose=None):
    """ 
    Create a list of content_strand-related (cs) output variable names, 
    mostly for subject and content_strand-related data.

    This does the work of glue.sas lines 244-253.

    Also return the list of new output variable names. 

    The caller should use the output list to include in the names for an 
    output dataset DictWriter. 

    This function emulates a section of the AIR ...superdata/glue.sas code 
    for client OGT for Fall 2012, and it may be be useful for adapting for 
    other clients. 

    This is meant to function like glue.sas code that declares Ldum1-4 
    variables and the  'j' do-while loop that imediately follows to 
    create lists of subject-content_strand  related variable names.

    This function will be called before reading any input rows to prepare 
    the output variable values corresponding to an input row.

    See also function output_vals_set() that is called once after every 
    input row is read, which sets many of these newly added variables 
    depending on other values that have been read from each input row, 
    again to mimic ...superdata/glue.sas behavior.

    Parameters:
    ===========

    dict_subject_info : Ordered Dictionary

    - dictionary listing subjects (R, M, W, S, C)  as keys.

    - each value is a dictionary, guaranteed to have a key 'contentstrand' 
      with the value being a list of the contentstrand abbreviations 
      that apply for the subject.

    - Example: Traditionally, for subject 'R' the contentstrand has been 
      ['A', 'R', 'L', 'I', 'X'] where X is reserved as a special value for 
      some purposes, usually to mean no single strand but ALL' strands.

    verbose: Boolean or any type

    - if not None, some debug info is output via print statements

    NOTE: code here is not the most elegant or efficient, but it maintains 
    in glue.sas variable names that facilitates manual code comparison.
    """
    
    me = "student_cs_vars_add()"
    if dict_subject_cstrands is None:
        raise ValueError(
          "Parameter dict_subject_info  must be given")
    output_vars = []   
    #
    for sj,cstrands in dict_subject_cstrands.items():
        # make subject case_insensitive
        subject = sj.lower()
        # glue.sas loop lines 244-256: loop over content_strands for this 
        # subject.
        # May use lowercase later, but this dict uses upper for subject now...
        # dict_subject_cstrands subject=R, cs_list='[u'a', u'r', u'l', u'i', u'x']'
        if verbose:
            print ("%s: key Subject='%s', cstrands='%s'"
             % (me,subject, repr(cstrands) ))
        # Create some var names for this subject and contentstrand.
        # See var_names in glue.sas lines 284-295:
        prefixes = ['inclusionflag', 'proforhigher', 'belowprof', 'advacc']
        for prefix in prefixes :
            if verbose:
                print "%s: prefix=%s, subject=%s" % (me,prefix,subject)
            output_vars.append('%s%s' % (prefix, subject))
        #glues lines 202-206:
        for suffix in ['xmerged','xplev','xpass','xpscal','xlep','xiep'
           ,'xdum5','xdum1','xdum2','xdum3','xdum4']:
            output_vars.append('up%s%s' % (subject, suffix))
        # See glue.sas lines 238-253:
        #level_vars = []
        #ldum1 = []; ldum2 = []; ldum3 = []; ldum4 = []
        # Loop through contentstrands as glue.sas lines 244-253
        for cs in cstrands:
            # glue.sas line 244-256
            # python loop of line 482 - end482here
            if cs == 'x':
                continue
            # glue.sas lines 248-252
            for suffix in ['lev','dum0','dum1','dum2','dum3','dum4']:
                var_name='up%s%s%s' % (subject, cs, suffix)
                if verbose:
                    print "%s: append var_name=%s" % (me, var_name)
                output_vars.append(var_name)
    # end loop for subjects (python lines 456-end235here). Glue loop 244-256.

    return output_vars
# end def student_cs_vars_add()    

from collections import OrderedDict, Callable

class DefaultOrderedDict(OrderedDict):
    """
    Initial key defaults, prevents dup keys, kesps insertion order.
     
    See http://stackoverflow.com/questions/6190331/can-i-do-an-ordered-default-dict-in-python
    """
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
            not callable(default_factory)):
            raise TypeError('first argument must be callable')
        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))
    def __repr__(self):
        return 'OrderedDefaultDict(%s, %s)' % (self.default_factory,
                                        OrderedDict.__repr__(self))
# end class DefaultOrderedDict(OrderedDict):

    
def ulist(in_list=None):
    """ 
    Given in_list, return a unique list, preserving order as much as 
    possible.
    """
    if in_list is None:
        raise ValueError("Param in_list must be given")
    dod = DefaultOrderedDict()
    for item in in_list:
        dod[item] = True
    return (list(dod.keys()))

def hvp2bcp(server=None, db=None, inputsdir='c:/'
    , od_basename_tableinfo=None
    , outputsdir = 'c:/SAS/OGT/Joboutput/', delimiter='\t'
    , verbosity=None):
    """
    Read header file and paired data file to load sql server tables.
    
    NB: The dbms='hvp' style of Dataset uses a separate Header file and Value 
    file Pair (hvp) files as input data to populate a database table 
    as described below. 
    
    The acronym hvp conveys a 'Header (H) file, Value (V) file, Pair (P),
    an "hvp" pair of files.
    
    Hence this function is named hvp2bcp().
 
    Parameters:
    ===========
    
    server, db: String, String
    -------------------
    -- Server and database(db) that hold the tables to load from hvp file pairs.
    
    
    inputsdir: string
    ------------------
    
    -- absolute path name of the directory with the pairs of hvp input files. 
    -- The hvp paired files here use the tab delimiter (required by bcp) with 
       names of:
       (1) the header file is basename.hdr (hdr is short for 'header') 
       (2) the paired data file is basename.tsv (tsv for 'tab separated 
           values')
       Note: the column names in the header file are also tab-separated names.
    
    od_basename_tableinfo: OrderedDict()
    -----------------------------------  
    -- key is the basename, described below, that is the base name for the 
       basename.hdr and basename.tsv pair of files used to load a table
    -- value tableinfo is a tuple of values
       [0] = tablename to drop and recreate in the database before loading data from 
             an hvp pair of files.
       [1] = default field specification for all table columns.
       [2] = od_field_spec: ordered dictionary of field_name key and value is 
             the custom field specification to use for the field name when 
             creating the table.
             --  If a column name key has the value of None, or any column 
                 name of the header file is  missing, then the default 
                 specification will be used for
             such column names when creating the table.
             -- NB: Key order in this dictionary does not really matter 
                because column order is determined by the 'column names' 
                 in the basename.hdr file.
             
    outputsdir: string
    ------------------
    -- Absolute path name of a directory to hold any 'bcp' output files, 
       including the bcp-errors.txt files, one per table loading via a 
       bcp command.
    
    Extended Summary
    ================
    
    WARNING: give up trying this function now if any of your value file rows 
    has more than 8060 characters.
    
    That seems to be the limit of the bcp utility of the sql server 2012.
    
    Consider a 'basename' is a string value and from it we deduce 2 filenames 
    for a source dataset.
    
    (1) header_file = "%s.hdr" % basename 
        and it has a single line of tab-separated column names for a dataset.
    (2) data_file = "%s.tsv" % basename
        and it must be one or more rows of tab-separated data values, in the 
        order of the column names in the header file.
        
    The header file is hardly "schema" info, that is, it represents only 
    column names and physical order of values in the paired tab-separated 
    value source file. 
    The header file provides only names, not any column lengths nor types.
    
    Note: 'bcp' refers to the sql server utilty, the 'batch control process'.
    
    """

    import subprocess
    import datetime
    
    iam = 'hvp2bcp():' 
    rqd_params=['study_name', 'od_basename_tableinfo' ]
    for rparam in rqd_params:
        if rparam is None:
            raise ValueError("Parameters %s must be set" % repr(rqd_params))
    delimiter = '\t' if delimiter is None else delimiter

    for basename, info3 in od_basename_tableinfo.items():
        fn_tsv = "%s%s.tsv" % (inputsdir, basename)
        fn_header = "%s%s.hdr" % (inputsdir, basename)
        
        if info3[0] is None:
            #no string for tablename at [0], so reuse basename.
            basetable = basename
        else:
            basetable = info3[0]
           
        table = ("dbo.%s" % ( basetable))
        
        if verbosity:
            now = datetime.datetime.now()
            print("%s: Loading %s to table %s at %s" % (iam, fn_tsv,table,now))
       
        default_column_spec = info3[1]
        od_column_type = info3[2]
        
        error_file= "%s%s_bcp_errors.txt" % (outputsdir, basetable)
        #
        cmd_bcp = ( "bcp %s.%s in %s -c -S %s -T -e %s -a 65000"
          % (db,table, fn_tsv, server,error_file ))
        
        # Create the table - get column names
        with open(fn_header, 'rb') as fh:
            columns_line = next(fh).decode()
        column_names = columns_line.split(delimiter)
        
        # Calls to create pyodbc dataset and DictWriter create a db table.
        dsw = Dataset(open_mode='wb', dbms='pyodbc'
          , table=table, server=server, db=db
          , column_names=column_names
          , replace=1,verbosity=1)
        
        dw = dsw.DictWriter(column_names=column_names
          ,default_column_spec=default_column_spec
          ,od_column_type=od_column_type,replace=1)
        
        dsw.close()
        # Now the db table is created and handles deleted, so bcp can use 
        # it to dump data into it.
        # Create bat file name and write bcp command to a bat file for 
        # this table

        bat_file = "%s/%s_hvp2bcp.bat" % (outputsdir, basetable)
        with open( bat_file, 'w' ) as f:
            f.write(cmd_bcp)
            
        # Use subprocess(we are on Win7) to call bcp for this hvp 
        
        proc = subprocess.Popen(bat_file, stdout=subprocess.PIPE)
        out_file = "%s/%s_hvp2bcp.out" % (outputsdir, basetable)
        
        with open( out_file, 'w' ) as f:
            for line in iter(proc.stdout.readline,''):
                    f.write(line + '\n')            
        
    # end loop: for basename, info in od_basename_tableinfo.items():
    
    # Now all db tables are created and loaded. 
    if verbosity:
        now = datetime.datetime.now()
        print("%s:Done at %s" % (iam, now))

    return
# end def hvp2bcp() 
    
def tvp2bcp(server=None, db=None, inputsdir='c:/'
    , od_basename_tableinfo=None
    , outputsdir = 'c:/SAS/OGT/Joboutput/', delimiter='\t'
    , replace=1 
    , verbosity=None):
    """
    Read column_types (.cty) and paired data(.tsv) files to load tables.
    
    NB: The dbms='tvp' style of Dataset uses a separate Types file(T) 
    (basename.cty) and Value file(V) (basename.tsv)  Pair(P).
    
    Parameters:
    ===========
    
    server, db: String, String
    -------------------
    -- Server and database(db) that hold the tables to create and to 
       load from tvp file pairs.
    
    inputsdir: string
    ------------------
    
    -- Absolute path name of the directory with the pairs of tvp input files. 
    -- The tvp paired files here use the tab delimiter (required by bcp) 
       with names of:
       (1) the column types file is basename.cty 
       (2) the paired data file is basename.tsv ('tab separated values')
       Note: the column names in the header file are also tab-separated names.
    
    od_basename_tableinfo: OrderedDict()
    -----------------------------------  
    -- Dict key is the basename, described below, that is the base name for 
       the basename.cty and basename.tsv pair of files used to load a table.
    -- value tableinfo is a tuple of 2 values
       [0] = tablename to drop and recreate in the database before loading 
             data from a pair of tvp (cty,tsv) files.
       [1] = od_column_type: None (if no column type overrides are wanted),
             or a dictionary of optional entries:
             (1) key is column name. It must be a column
                 name that also appears in the .cty file.
             (2) and value is the custom field specification to use for the 
                 field name to override what appears in the .cty file.
                 Examples: int, float, nvarchar(8))
                 
             --  If a key column name as found in the cty file is  missing 
                 here, then the column type in the cty file will be used.
             such column names when creating the table.
             -- NB: Key order in this dictionary does not really matter 
                because column order is determined by the 'column names' 
                order in the basename.hdr file.
             
    outputsdir: string
    ------------------
    -- Absolute path name of a directory to hold any output files destined
       for input to 'bcp' to load a database table, including the 
       file bcp-errors.txt, one set of files per loading a table 
       via a bcp command.
    
    Extended Summary
    ================
    
    WARNING: give up trying this function now if any of your value file rows 
    has more than 8060 characters.
    
    That seems to be the limit of the bcp utility of the sql server 2012.
    
    Consider a 'basename' is a string value and from it we deduce 2 filenames 
    for a source dataset.
    
    (1) column_type_file = "%s.cty" % basename 
        and for each column in the dataset it has line, in order of the 
        columns of the data file with column_name, tab, data type. 
        
    (2) data_file = "%s.tsv" % basename
        and it must be one or more rows of tab-separated data values, in the 
        order of the column names in the .cty file.
           
    Note: 'bcp' refers to the sql server utilty, the 'batch control process'.
    
    """

    import subprocess
    import datetime
    
    iam = 'tvp2bcp()' 
    rqd_params=['server','db','inputsdir',
      'outputsdir', 'od_basename_tableinfo' ]
    for rparam in rqd_params:
        if rparam is None:
            raise ValueError("Parameters %s must be set" % repr(rqd_params))
    delimiter = '\t' if delimiter is None else delimiter
    if (verbosity):
        print ("%s: start to upload %d tables..." 
               % (iam, len(od_basename_tableinfo)))
    for basename, info2 in od_basename_tableinfo.items():
        fn_tsv = "%s%s.tsv" % (inputsdir, basename)
        fn_cty = "%s%s.cty" % (inputsdir, basename)
        if verbosity or 1==1:
            print "%s:fn_tsv=%s, fn_cty=%s" % (iam, fn_tsv, fn_cty)
        if info2[0] is None:
            #no string for tablename at [0], so reuse basename.
            basetable = basename
        else:
            basetable = info2[0]
           
        table = ("dbo.%s" % ( basetable))
        
        if verbosity:
            now = datetime.datetime.now()
            print("%s: Loading %s (cty=%s)\n\tto table %s at %s" 
                  % (iam, fn_tsv, fn_cty,table,now))
        
        # Valid entries of d_column_type, override od_column_type         
        d_column_type = info2[1]
        
        error_file= "%s%s_bcp_errors.txt" % (outputsdir, basetable)
        #
        cmd_bcp = ( "bcp %s.%s in %s -c -S %s -T -e %s -a 65000"
          % (db, table, fn_tsv, server, error_file ))
        
        # Create the table - get column names
        od_column_type = OrderedDict()
        with open(fn_cty, 'rb') as fh:
            for line in fh:
                line = line.split('\n')[0]
                fields = line.split(delimiter)
                #print "table %s cty line: %s" % (table,repr(fields))
                od_column_type[fields[0]] = fields[1]
            
        # Valid entries of d_column_type, override od_column_type 
        if d_column_type is not None:
            for ocol, otype in d_column_type.items():
                if ocol in od_column_type:
                    od_column_type[ocol] = otype
                else:
                    raise ValueError(
                      "%s: table '%s' has no column '%s' to override"
                      % (iam, table, ocol))
        
        # Call Dataset to create pyodbc dataset and DictWriter 
        # to create for it a db table.
        dsw = Dataset(open_mode='wb', dbms='pyodbc'
          , table=table, server=server, db=db
          , replace=replace, verbosity=verbosity)
        
        dsw.DictWriter(od_column_type=od_column_type)   
        dsw.close()
        
        # Now the db table is created and handles deleted, so bcp can use 
        # it to dump data into it.
        # Create bat file name and write bcp command to a bat file for 
        # this table

        bat_file = "%s/%s_tvp2bcp.bat" % (outputsdir, basetable)
        with open( bat_file, 'w' ) as f:
            f.write(cmd_bcp)
            
        # Use subprocess(we are on Win7) to call bcp for this tvp 
        
        proc = subprocess.Popen(bat_file, stdout=subprocess.PIPE)
        
        # Put 'bcp' output messages to out_file.
        out_file = "%s/%s_tvp2bcp.out" % (outputsdir, basetable)
        
        with open( out_file, 'w' ) as f:
            for line in iter(proc.stdout.readline,''):
                    f.write(line + '\n')            
        
    # end loop: for basename, info in od_basename_tableinfo.items():
    
    # Now all db tables are created and loaded. 
    if verbosity:
        now = datetime.datetime.now()
        print("%s Done at %s" % (iam, now))

    return
# end def tvp2bcp()
    