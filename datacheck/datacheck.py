######################################################################
# 
# (c) Copyright American Institutes for Research, unpublished work created 2013 
#  All use, disclosure, and/or reproduction of this material is 
#  prohibited unless authorized in writing. All rights reserved. 
# 
#  Rights in this program belong to: 
#   American Institutes for Research. 
# 
######################################################################
import xlrd
from collections import (
  OrderedDict, defaultdict)
from math import log

#import scipy, scipy.stats
#then can do: pmf = scipy.stats.binom.pmf(x,10,0.1)
# 




def datacheck(context=None):
    conn = context.conn
    pass

def longcomp(context=None, table_base=None, table_compare=None,  
  table_report=None, longcomp_sheet=None):
    """ 
    Report the differences between an old and new table of student data.
    
    Extended Summary:
    -----------------
    Read a layout file with requirements on how to calculate and report comparisons of data values between two datasets, usually taken a year or so apart.
    
    Hence the name, longcomp is derived from 'longitudinal comparison'.
    
    Such layout files are quite small, and so are read completely into memory
    for use here.
    
    The requirements and semantics of the layout and data files are specific, hence further detail is found in the below sections about these parameters.
    
    Input tables table_old and table_new
    ------------------------------------
    Input layout longcomp_sheet
    ---------------------------
    
    Column "Variable" must identify a column name that exists in both table_old and table_new.
    
    Column "By" indicates a SAS-like "BY" or SQL-Like groupby variable. 
      This indicates to form mutex groups of input rows, where each row in a group exhibits the same value on the byVar. 
    
    The value for By can have (1) special value "(all)" or (2) the name of another column that exists in both input tables to indicate how to aggregate data.
    
    - For example, if By is "(all)", this means that longcomp() must report a statistic for the given Variable over every encountered value of that variable in either input table.
    Column "TreatAs" only has value "nominal".
    Column "MatchAlgorithm" only has value "proportion"
    Column "Tolerance" means . . .
    Column "ToleranceType" values are in ("absolute", "stat").
    Column "missing counted" values are yes or no. It is used for something.
    Column "max_bad" values are 0, 10 or 20. It is used for some purpose.
    - 
    
    """
    # verify conn
    conn = context.conn
    with xlrd.open_workbook(longcomp_sheet) as wb:
        sheet = wb.sheet_by_index(0)
    # SAS line3 - if same-named output exists already, delete it. 
    pass # insert code here
    
    # lines 4-8 - create list by_vars, distinct by vars named in sheet.
    # SAS - select distinct by into :byvarlist sep by " " from longcompsheet
    # where variable ^= "" 
    # that is, get all "By" column values from longcompsheetvariables 
    #RVP - verify this is integer-indexable list, one per row in sheet order.
    
    #Create dict to reference sheet column indices by lowercase column name
    cn_dex = { str(sheet.col(i)[0].value).lower():i 
        for i in range(sheet.ncols) }
    
    # Orig 4-8: by_vars: per by_var, save unique value. 
    # Orig 19-24: by_keeps: per by_var, save list of keep_variables used in 
    # the layout. 
    #
    by_vars = OrderedDict()
    by_keeps = defaultdict(list)
    by_totals = defaultdict(list)
    nrow = 0
    vdex = sheet.col(cn_dex['variable'])
    for x in sheet.col(cn_dex['by']):
        val = str(x.value).lower()
        if val is not None and val != "":   
            by_vars[val] = True
            if val != '(total)':
                # Append a keep_var for this by_var.
                by_keeps[val].append(sheet.cell_value(nrow, vdex))
                
            else:
                # orig lines 26-30: add to totalvarlist
                by_totals[val].append(sheet.cell_value(nrow,vdex))
        #            
        nrow += 1 
    
    # lines 9-13 - set sheet InputSortOrder = _N_ - Review SAS docs here?
    # TODO
    
    
    # line 15 - set k = 1 
    k = 1 # SAS loop index: by_var=by_var_list[k].
    for by_var in by_vars.keys():
        # Lines 16-98 - Big loop processing report segment for each by_var.        
        # line 17: in SAS used k to set by_var, done at top of this loop.
        # line 18:
        if by_var == '(all)':
            by_var = 'all'
        # orig lines 19-24: construct keepvar list from sheet rows with current
        # by_var except where variable = 'total' done before this loop.
        
        keep_vars = by_keeps[by_var]
        
        pass
    
        # Orig 25-30 if any keepvars existed in prior loop, construct list
        # totalvarlst -done above.
        # 
        exists = 0
        
        # orig line 25
        if len(keep_vars) > 0:
            exists = 1;
            
        if by_var != '(all)' :
            # Orig 31-58: this loop mirrors that.
            # SAS comment: Do the expensive sort, keeping the minimum number 
            # of variables.
            if exists == 1:
                pass
                # ORIG lines 32-35 - 'expensive sort' of base_DS
                # table ordered by k, the "by_var", and ONLY
                # keeping/selecting "minimal" result columns for by_vars and
                #  keep_vars, ordered by by_var.
                # This table will be in sql express, so could insert SQL 
                # code here to do the sort?
                # First, Grok the whole algo and see if there is a cleaner way.
                #  Maybe even consider pandas..?
                # 
                # Insert code here to construct or construct and execute 
                # sql to get  results - or get a cursor for iterating..
                #
                
                # ORIG Lines 36-38: Expensive sort of compareDS 
                # orderd by the by_var-same sort just done for baseDS
                # 
                
                # ORIG lines 39 - 42 : Get sample size.
                # uses SAS proc freq with line 41:
                # table &byvar / out=&base_DS._sortby_&k._c(drop=percent);
                # review this SAS syntax....
                # this call to proc freq() assigns a to a new "._c" 
                # variable (see out=x above)
                # the counts for each distinct encountered value for the by 
                # variable. 
                # INSERT code here
                
                #ORIG Lines 43-46 - same as above, but do same here 
                # for the compare_DS.
                # INSERT code here
                
                # ORIG lines 46-51: 
                # 46: create lib_dataName for the "baseDS" counted values and 
                # rename the count column to 'baseN' for output.
                
                # ORIG lines 52-57: Do same for the compareDS
                # We are building the output report rows here for this by_var, 
                # and for the values for columns baseN and compareN.
                
                #Orig 58 ; end of loop started at xxx...
                
                # ORIG 59-62 : set base_n to nobs of the entire base_DS
                # ORIG 59 - now we set dsid to the re-opened  base_DS... 
                # maybe it is the 
                # orig data for all vars, as I think it was not saved 
                # when we re-used
                # its name for an output DS of just the 'by_var".
                #
                
                #ORIG 62-64: set comp_n to nobs of entire comp_DS
                
                #ORIG 65 - 83 : 
                # ORIG 67 - 82: nested - j loop - for each keepvar, 
                # ORIG 68 - set var to keepvarname
                # ORIG 69-73: proc sql selects from longcompsheet- 
                # distinct missing_counted 
                # into missoption-list for by=by_var and variable=var
                
                #ORIG 74-79: i-loop starts at 1: 
                # for each component in missotption_list, call macro
                #  %NominalComp(base=&base_DS._sortby_&k., compare=&compare_DS._sorted_&k.,byvar=&byvar., var=&var., missingq=&missing_cnt,inputDS=longcompsheet, rndfactor=100, Debug=&Debug);
                # ORIG 78 increment i 
                # ORIG 79 end i loop
                
                # ORIG 80: call next line because...
                # %PercentileComp(base=&base_DS._sortby_&k., compare=&compare_DS._sorted_&k., byvar=&byvar., var=&var.,inputDS=longcompsheet, rndfactor=100, Debug=&Debug);
                
                # ORIG 81- increment j for outer loop
                # ORIG 82 - end j loop from line 67 f0r this keepvar-
                # missingcount stuff, %NominalComp.. %PercentileComp...
                
                #ORIG 83 end main outer k loop for this by_var
                
                # Orig 84-90 if byvar is (all): call:
                # %TotalComp(base=&base_DS._sortby_&k., compare=&compare_DS._sorted_&k.,byvar=&byvar., var=&var., inputDS=longcompsheet, Debug=&Debug);
                # else call TotalComp as:
                #  %TotalComp(base=&base_DS, compare=&compare_DS,byvar=&byvar., var=&var., inputDS=longcompsheet, Debug=&Debug);
                
                # Orig 91-96 - Delete 4 temporary datasets base_DS.sortby_&k, base_DS._sorbyt_&k._c and 2 similar for compareDS.
                
        # orig line 97: increment loop counter
        k += 1
        
    #orig line 98: main loop ended: back to next segment of main code
    
    #Orig line 99: call sort for the output report by Comparison, descending diff... so biggest "diff" values are reported first.
                                
    
    # 
    # 
    # 
    
    
    #create list totalvarlst for ordered wb row values on column "Variable"
    # See longcomp macro lines 25-30
    
    # sas lines 31 - 58 - process special sheet column 'By" values 
    # EXCEPT (all): "(all)" means... 
    
    #NOTE: lines 31-35 - sort the base dataset existing rows by &k.
    # Keep the byvars and variables named in the worksheet,
    
    #sas lines 36-38 - sort input compare_table and keep only rows
    
    return 1

def nominal_comp(base=None, compare=None, byvar=None, var=None, missingq=None,
  inputDS=None, rndfactor=None, debug=None):
    """
    
    Extended Summary:
    =================
    
    Emulate the AIR SAS NominalComp Macro.
    
    Two dataset input tables have the same column names (once the names are converted to lowercase)  and the datasets are collected at different times, typically a year apart. 
    
    Byvar and Variable parameters and the output they produce:
    -----------------------------------------------------------
    Parameter byvar, when set to some input column name, indicates to analyze an aggregate statistic (e.g., a count) for a set of input rows in each dataset for each distinct nominal value that is encountered in the data for the byvar. 
    For each unique byvar value encountered in either of the input datasets, a group of consecutive output report rows will be produced.
    Each row in such a group shows the byvar name in report column "ByVariable" and the ByVar value that defines that group showing in report column "ByValue". The report also shows columns "Variable" and "Value" and "baseN" and "compareN".
    Among all encountered base input dataset row where the reported ByVariable value matched the reported ByValue and the input row's "Variable" value matched the reported "Value",  the aggregate count of such rows appears under column "baseN". Similarly column "compareN" shows the similar count from the compare input dataset.
    So, if the ByVariable assumed 3 distinct values among the two input datasets, and the Variable assumed 5 distinct values among the subset of input rows with all 3 ByVariable values, then we'd get 15 output rows for this group if input data would appear in the report unless another xlsx variable, "Tolerance" limits such output rows (details on "Tolerance" are pending). 
    However, it is easy to see that depending on the variance of data values,
    that a meaninglessly large number of small groups could be formed in the input data. So, other longcomp.xlsx parameters to indicate the Tolerance comes into play to not over-report with multiple output rows having very small counts.
    Also, the special ByVar value of "(all)" is recognized to mean to base no grouping of the input rows according to a ByVariable - rather simply use the levels of the "Variable" and report on them. One consequence is that when the ByVariable "(all)" is on a row on the output report, it shows nothing for ByValue.
    
    Output Report Column MatchType:
    -------------------------------
    Where input longcomp.xlsx file has MatchAlgorithm of proportion and ToleranceType of absolute, output MatchType is proportion-absolute. 
    Likewise, output MatchType value proportion-stat may also appear.
    
    Note: where a ByVariable other than All is used, the output report columns will show a value in ByValue and the BaseN and compareN columns will show the number of input rows having that value. Where ByVariable is all, baseN and 
compareN is always the total number or rows in the corresponding input dataset.

Output column "Comparison", if a value is given, shows the input row number fron longcomp.xlsx that directed output of the subject output row.
      
      NOTES:
      ======
      
      - This version of longcomp is designed specifically to serve the needs of OGT, which means that their specific longcomp.xlsx file is supported. 
      - So, (1) only proportion-stat and proportion-absolute MatchAlgorithm-ToleranceType settings are honored in the longcomp.xlsx input sheet.  
      - and (2) only the TreatAs value of  "nominal" is honored in the longcomp.xlsx input.
      - Given: in longcomp.xlsx, 4 variables appear, each with ByVars of all, dcrxid_attend, and bcrxid_attend
      - Given: 2 variables appear, Grade and migrant with only the ByVar of all.
      - Given? Output only appears where BOTH input files have at least 1 row with each ByVal/Val combo, it seems? 
      - Given: var1 of the 4 vars is ucrxgen with 2 good levels (plus potentially missing and error or both-checked)
      - Given var2 is ethnicity with 7 levels
      -Given var3 is upxxlp with x levels
      -Var 4 is upxxiep with x levels
      - var 5 is grade with 8 or so levels
      -var 6 is migrant (output test of first 100 rows showeed no output)
      
       
    
    """
    
    num_checks = 0
    
def freq(indata=None):
    """
    """
    pass
    #select input rows
    pass
    
