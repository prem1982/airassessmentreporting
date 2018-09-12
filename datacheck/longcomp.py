from airassessmentreporting.datacheck.summary import *
from airassessmentreporting.datacheck.longentry import *
from airassessmentreporting.datacheck.dataset import *


from collections import OrderedDict
from operator import itemgetter

def reverse_numeric(x,y):
    # Multiply two floats by 10K, and do integer diff/compare for sorting
    return int( int(y*10000.0) - int(x*10000.0))

def longcomp(
  dsr_time0=None, dsr_time1=None, dsr_longcomp=None, 
  dsw_full_report=None,
  dsw_brief_report=None):
    """ Given two datasets of time0 and time1 score data, report differences.

    Generate a report of significantly interesting differences in the datasets between frequency counts at time1 and time2.
    
    Parameters:
    ===========
    dsr_time0, dsr_time1: Dataset
     - Readable datasets for time1 and time0
     
    dsr_longcomp:Dataset
     - Dataset with parameters defining one or multiple reports to generate.
    dsw_full_report:Dataset
     - Optional writable dataset to write statistics for all report data
    dsw_brief_report:Dataset
     - Writable dataset to write data of a brief report, showing only a few report rows for each report that show the largest differences between time0 and time1.
    
    Extended Description:
    =====================
  
    The readable longcomp report parameter data must have these ordered column names (when normalized to lowercase, and spaces changed to underbars):

    required_longcomp_columns = [ 'variable', 'by', 'treatas', 'matchalgorithm',
      'tolerance', 'tolerancetype', 'missing_counted', 'max_bad' ]

    Each row defines characteristics of a longitudinal comparision report 
    that longcomp() will generate. For each row in longcomp.xls. longcomp()
    will prepare a Summary() object and a SummaryReport() object.

    dsr_longcomp columns:
    ----------------------
    The required columns in the longcomp input must appear. Traditional order is:
     col0: "Variable": variable name defining an axis (nominal variable name) on a frequency table. 
           If "By" value below is not "(all)", 
           then variable is used below as the second-named axis in a 2-way Summary, while
           "By" varname will be the first-named axis in that Summary.
           But if "By" is 'all', "Variable" is the first and only axis in a Summary.
     col1: "By": (variable name 2) is a name defining the 2d axis of a frequency table, except the special
         : name "(all)" means it is only a one-way frequency table based on "variable".
     col2: "TreatAs": variable type for the first variable 
          - For client OGT, it is always "nominal".
     col3: "MatchAlgorithm": analysis 
         - Indicates a type of analysis for the report.
         - For client OGT, it always "proportion".  
     col4: tolerance 
         - a float value to which the analysis statistic (computed 
           for each frequency cell encountered in the data)  will be compared
           for each cell, and if the statistic exceeds the tolerance, cell data 
            and the statistic will be displayed on the output report
      col5: tolerance type: (statistic type)
            For analysis type 'proportion': 
            - value 'absolute': means to calculate 'diff' for each cell as a percentage diff
            - value 'stat': means to calculate 'diff' for each entry as a liklihood ratio. 
            See code in LongEntry.summarize() for more details and code.
      col6: missing_counted - yes or no: whether to count missing data. 
            For OGT, this is always yes, so no special processing is yet done for this.
      col7: max_bad - The report lines that are output are considered 'bad' in the sense
            that the calculated statistic is not expected to exceed the tolerance, but when
            it does, it is considered "bad" because it is expected to be due to error in
            data collection of the original data. For any variable, across cells in
            each of the x-way frequency analyses, several bad values may be found.
            To prevent overwhelming the report reader individual, max_bad means the 
            maximum number of report rows to display per variable level value, and they
            are sorted from "most bad" (least likely as a result of random variation) to not-so-bad. 
    ---
    TODO: Change algo for creating the brief report to maintain list of top max_bad rows with highest 'diff' values (index 7) for each summary's 'mini-report' rather than sort the output and then pick off the top max_bad rows.
    It should cut the core row-processing execution time in half from 6 to 3 seconds for runs of 10k input rows at time0 and time1, and provide much bigger time savings for larger datasets.
    
    """
    if dsr_time0 is None:
        raise ValueError("dsr_time0 dataset is missing.")
    if dsr_time1 is None:
        raise ValueError("dsr_time1 dataset is missing.")
    if dsw_brief_report is None:
        raise ValueError("dsw_full_report is missing.")
    if dsr_longcomp is None:
        #raise ValueError("longcomp_wb workbook filepath is misisng.")
        raise ValueError("dsr_longcomp input dataset is missing.")
    required_longcomp_columns = [ 'variable', 'by', 'treatas', 'matchalgorithm',
      'tolerance', 'tolerancetype', 'missing_counted', 'max_bad' ]
    reader_longcomp = dsr_longcomp.DictReader()
    for rqcol in required_longcomp_columns:
        if rqcol not in reader_longcomp.fieldnames:
            raise ValueError(
              "Required column '%s' is not in dsr_longcomp='%s'"
              % (rqcol,repr(dsr_longcomp)))
    # Process each longcomp row into a summary, creating a list of summaries
    # For the longcomp.xlsx file, for each row, define a Summary/report 
    # that will be
    # created here on which to base data collection, analysis, and reporting.

    summaries=[]
    for idx, row_lc in enumerate(reader_longcomp):
        # Read non-numeric row values in lower case, especially for 'normal' 
        # column name matching purposes.
        max_bad = int(float(row_lc['max_bad']))
        if (max_bad < 1):
            # Skip processing for this summary. No report rows are required.
            continue;
        vn0 = row_lc['variable'].lower()
        vn1 = row_lc['by'].lower()
        # treat_as: always 'nominal' for OGT, but revise this if other 
        # clients use other types.
        treat_as = row_lc['treatas'].lower()
        match_algorithm = row_lc['matchalgorithm'].lower()
        tolerance = float(row_lc['tolerance'])
        tolerance_type = row_lc['tolerancetype'].lower()
        missing_counted = row_lc['missing_counted'].lower()
        summary_column_names = []
        print ( 
          "Comparison idx=%d,vn0=%s,vn1=%s,tas=%s,tolerance=%f,"
          "tolerance_type=%s,max_bad=%d"
          % (idx, vn0, vn1, treat_as, tolerance, tolerance_type, max_bad)
          )
        if vn1 == '(all)':
            summary_column_names.append(vn0)
            #populate leader columns for var1, val2
            dict_leader = OrderedDict()
            dict_leader['Var_all'] = '(all)'
            dict_leader['Val_all'] = ''
            # Emulate SAS longcomp - report "missing values" for levels.
            missing_level_report = True
        else:
            summary_column_names.append(vn1)
            summary_column_names.append(vn0)
            dict_leader=None
            missing_level_report = False
        # Make a summary report and summary. 
        print (
          "New summary for summary_column_names=%s"
          % (repr(summary_column_names)))
        sr = SummaryReport(
          comparison_id=idx+1,
          dsw_full_report=dsw_full_report, 
          tolerance_type=tolerance_type, tolerance=tolerance,
          missing_level_report=missing_level_report,
          match_algorithm=match_algorithm, 
          dict_leader=dict_leader, max_bad=max_bad
          )
        su = Summary(
          column_names=summary_column_names,
          EntryClass=LongEntry, summary_report=sr)
        #append this summary to list
        summaries.append(su)
    # Define a data collector, using the 2 input datasets and summaries list
    datasets = [dsr_time0, dsr_time1]
    # collection: accrue entry data for each summary from input datasets
    collector = Collector(datasets=datasets, summaries=summaries)
    collector.collect()
    out_rows = []
    for idx, su in enumerate(summaries):
        # Generate summary statistics
        sr = su.summary_report
        max_bad = sr.max_bad
        if max_bad < 1:
            # No report rows desired, so skip it
            continue
        print "Reporting for comparison_id %s" % sr.comparison_id
        # accrue High level summary entry stats from cell-level entry stats,
        su.summarize()
        # Now issue output report data
        report_rows = su.report()
        print (
          "Comparison_id %d yields %d total report_rows" 
          % (sr.comparison_id, len(report_rows)))
        # Sort by 'worst' statistic (highest-valued or most significant)
        # and select top "max_bad" report rows
        report_rows.sort(key=itemgetter(7),cmp=reverse_numeric)
        max_bad = ( sr.max_bad if sr.max_bad <= len(report_rows) 
                    else len(report_rows))
        if max_bad < 1 or max_bad > len(report_rows):
            # No report rows possible, so skip it
            continue
        report_rows = report_rows[:max_bad]
        # Accrue the report rows
        out_rows.extend(report_rows)
    # Set report columns - they are the same for any summary report by
    # traditional design.
    column_names=["ByVariable","ByValue","Variable","Value","MatchType",
      "base","compare","diff","Comparison","baseN","compareN"]
    brief_writer = dsw_brief_report.DictWriter(
      column_names=column_names)
    brief_writer.writeheader()
    dict_brief = {}
    for row in out_rows:
        for i in range(len(row)):
            dict_brief[column_names[i]] = str(row[i])
        brief_writer.writerow(dict_brief)
    return out_rows       
#

if __name__ == '__main__':
    # TEST LONGCOMP
    # NOTE: See also regression tests under .../test/test_datacheck.py,
    # test in test_longcomp_001(). They employ group-accessible 
    # data on the H drive.
    # 
    import os
    home = os.path.expanduser("~")+ os.sep
    tdir=home+"testdata/"
    
    fn_time0=home + "testdata/testdata1.csv"
    fn_time1=home + "testdata/testdata2.csv"
    fn_longcomp_wb= tdir + "ogt_longcomp2.xls"
    fnw_full_report = tdir + "tmp_full_report.csv"
    dsr_time0 = Dataset(name=fn_time0,open_mode='rb')
    dsr_time1 = Dataset(name=fn_time1,open_mode='rb')
    dsr_longcomp = Dataset(
      dbms='excel_srcn', workbook_file=fn_longcomp_wb,
      sheet_name=None, open_mode='rb')
    dsw_full_report = Dataset(name=fnw_full_report,open_mode='wb')                  
    ofn = home+"testdata/tmpout.csv"
    odfn = home+"testdata/tmpdictout.csv"
    brief_name = home+"testdata/tmp_brief_report.csv"
    dsw_brief_report = Dataset(name=brief_name, open_mode='wb')
    #dsw_brief = Dataset(dbms='csv', name=
    #dw0 = Dataset(name=ofn, open_mode='wb',column_names=['A','B','C'])
    #dwb = Dataset(name=odbfn, open_mode='wb')
    #can set dsw_full_report to dw0 or, for slight speed-up, to None.
    output = longcomp(
      dsr_time0=dsr_time0, dsr_time1=dsr_time1, dsr_longcomp=dsr_longcomp,
      dsw_full_report=dsw_full_report,
      dsw_brief_report=dsw_brief_report )
    print (
      "longcomp is done: See brief output='%s', full output='%s'" 
      % (repr(dsw_brief_report), repr(dsw_full_report)))
    # del some datasets, else output ones won't flush immediately.
    del dsr_time0, dsr_time1, dsr_longcomp, dsw_brief_report, dsw_full_report

    pass


