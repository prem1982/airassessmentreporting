import scipy.stats as ss
import math

def air_binom_pmf(count, total, p ):
    """ This incorporates a work around to the scipy stats.binom.pmf()  
         bug (TRAC #1881) per advice from Paul Van Wamelen.
    """
    if p == 0.0:
        if count == total:
            return 0.0
        else:
            return 1.0
    elif p == 1.0:
        if count == 0:
            return 0.0
        elif count == total:
            return 1.0
    return ss.binom.pmf(count,total,p)

class LongEntry(object):
    # report_column_names keys should match with dictionary keys of 
    # entry.report().
    report_column_names = ["MatchType","base","compare","diff","CompID",
      "baseN", "compareN"]
    
    def __init__(self):
        """
        This is an entry type that may be associated with each cell of a Summary object. 
        
        It is used in practice by passing the class name LongEntry to create a Summary() object.
        
        This entry type is designed specifically to collect, summarize, and
        report the types of buffer_ and statistics needed to support longcomp().
        
        """
        self.counts = [0] * 2
        self.percents = [0.0] * 2
        self.diff = [0]
    def __repr__(self):
        prefix = "%s" % (self.__class__.__name__)
        return ("%s:counts=%s, diff=%s." 
          % (prefix, repr(self.counts), repr(self.diff)))
    def accrue_row(self, index, value=1):
        """ This accrues counts to a LongEntry object in a Summary() cell.
        
        Probably the simplest possible accrue_row() function that is useful. 
        It does not even take a dependent variable name in the row data, though most implementations probably would need to do that.
        It simply increments an existence counter in the cell entry.
        For other types of common analyses, a parameter would include row column data to track one or more dependent variables, each with its own entry members to manage a total, a sums of squares, N, and perhaps others that the accrue_row() function would modify, and the accrue_row function in those cases would take values for a list of ordinal dependent variable names.
        """
        #print "accrue_row: using index=",index
        if self.counts[index] is None:
            self.counts[index] = 1;
        else:
            self.counts[index] += 1
    def accrue_entry(self, entry=None):
        """ To this entry, accrue counts values of the given entry parameter.
        
        Sample Usages:
        (1) Summary.node_accrue_children calls this function for a node entry to propagate entry values of its child nodes in a Summary tree.
        """
        if entry is None:
            raise ValueError("Argument entry is None")
        # print "accrue_entry:len(self.count)=",len(self.counts)
        for i in range(len(self.counts)):
            if self.counts[i] is None:
                # Its ok to have 0 counts in margins
                self.counts[i] = 0;
            if entry.counts[i] is None:
                #Add this clause for testing.
                entry.counts[i] = 0;
            if entry.counts[i] is not None:
                self.counts[i] += entry.counts[i]
    def summarize(self, parent_entry=None, summary_report=None):
        """
        For this entry, calculate summary statistics based on its accrued values and on the parent entry's accrued values.
         
        For this entry, caller has already called accrue_entry that accrued all of the child nodes.
        When summarization for the summary tree (which root node has entry summary.grand.entry) is completed, one result is, for example, summary.grand.entry.counts[0] is the number of total observations in the first dataset.
        Parameters:
        ===========
        - parent_entry may be accessed for parent totals set via entry.accumulate_entry()
        - summary_report contains variables for match_algorithm, others that may affect calculations.
        """
        if parent_entry is None:
            raise ValueError("parent_entry is None.")
        if summary_report is None:
            raise ValueError("summary_report is None.")
        c0 = self.counts[0]
        c1 = self.counts[1]
        t0 = parent_entry.counts[0]
        t1 = parent_entry.counts[1]
        c0 = c0 if c0 else 0
        t0 = t0 if t0 else 0
        c1 = c1 if c1 else 0
        t1 = t1 if t1 else 0
        
        if summary_report.tolerance_type == 'absolute':
            # may need to add None checks depending on missing_counted value.
            # but always 'yes' for OGT.
            for i in range(2):
                self.percents[i] = ( 
                  0 if parent_entry.counts[i] == 0  
                  else (float(self.counts[i]) / float(parent_entry.counts[i]))
                  * 100.0)
            self.diff = math.fabs( 
              float(self.percents[0]) - float(self.percents[1]))
            #print ( "entry.summarize: c0=%s, c1=%s, self.diff=%s\n"
            #      % (repr(c0), repr(c1), repr(self.diff)))
        elif summary_report.tolerance_type == 'stat':
            # see macro datacheck lines 1160-1162, and get baseN and 
            # compareN values.
            # this passes - diff corresponds to test SAS output 
            # for dcrxid_attend = 049019, ucrxgen= 1.
            #count1 = 23; count2 = 5; 
            #total1 = 52; total2 = 49
            den = float(t0 + t1)
            p = 0.0 if den == 0.0 else (float(c0) + float(c1))/den
            #print ("count1=%d, total1=%d, count2=%d, total2=%d, p=%f" 
            # % (count1, total1, count2, total2, p))
            num0 = air_binom_pmf(c0,t0,p)
            if math.isnan(num0):
                print("num0 is a nan: c0=%d,t0=%d,c1=%d,t1=%d,p=%f" % (c0,t0,c1,t1,p))
            num1 = air_binom_pmf(c1,t1,p)
            if math.isnan(num1):
                print("num1 is a nan: c0=%d,t0=%d,c1=%d,t1=%d,p=%f" % (c0,t0,c1,t1,p))
            #numerator = ss.binom.pmf(c0,pc0,p) * ss.binom.pmf(c1,t1,p)
            numerator = num0 * num1
            if math.isnan(numerator):
                print("numerator is a nan: c0=%d,t0=%d,c1=%d,t1=%d,p=%f" % (c0,t0,c1,t1,p))
            p0 = 0.0 if t0 == 0 else float(c0)/float(t0)
            p1 = 0.0 if t1 == 0 else float(c1)/float(t1)
            if t0 != 0 and t1 != 0:
                den0 = air_binom_pmf(c0, t0, p0)
                if math.isnan(den0):
                    print "den0 is a nan: c0=%d,t0=%d,c1=%d,t1=%d,p0=%f,p1=%f" % (c0,t0,c1,t1,p0,p1)
                den1 = air_binom_pmf(c1, t1, p1)
                if math.isnan(den1):
                    print "den1 is a nan: c0=%d,t0=%d,c1=%d,t1=%d,p0=%f,p1=%f" % (c0,t0,c1,t1,p0,p1)
                denominator = den0 * den1
                if math.isnan(denominator):
                    #print "denominator1 is a nan: c0=%d,t0=%d,c1=%d,t1=%d,p0=%f,p1=%f" % (c0,t0,c1,t1,p0,p1)                              )
                    print "denominator is a nan: c0=%d,t0=%d,c1=%d,t1=%d,p0=%f,p1=%f" % (c0,t0,c1,t1,p0,p1) 
                else:
                    pass
            else:
                denominator = 0.0
            if denominator != 0.0:
                if numerator == 0.0:
                    # Set diff to a huge value so the difference is significant.
                    # Otherwise math.log would throw a ValueError.
                    self.diff = 50.000
                else:
                    ratio = float(numerator)/float(denominator)
                    if math.isnan(ratio) or ratio <= 0.0 :
                        print ("ratio %f is non-positive or nan: c0=%d,t0=%d,c1=%d,t1=%d,p0=%f,p1=%f" 
                               % (ratio, c0,t0,c1,t1,p0,p1))
                        self.diff = 0
                    else:
                        try:
                            self.diff = -math.log(ratio)
                        except ValueError:
                            print ("ValueError for -math.log(numerator=%f/denominator=%f)"
                                   % (float(numerator),float(denominator)))
                            print ("ratio %f is a negative or nan: c0=%d,t0=%d,c1=%d,t1=%d,p0=%f,p1=%f" 
                               % (ratio, c0,t0,c1,t1,p0,p1))
            else:
                self.diff = 0
        else:
            raise ValueError(
              "Unknown tolerance_type '%s' given." 
              % summary_report.tolerance_type)
            
    def report(self, parent_entry=None, summary_report=None, dict_out=None):
        """
        Assume caller has called summarize() to accrue entry statistics. 
        Report them if they qualify.
        """
        if parent_entry is None:
            raise ValueError("parent_entry is None.")
        if summary_report is None:
            raise ValueError("summary_report is None.")
        sr = summary_report
        # Do not report non-numeric results
        if math.isnan(self.diff):
            print "report(): self.diff is a nan"
            return None
        # Do not report uninteresting statistics
        if float(self.diff) < float(sr.tolerance):
            return None
        # Marshall some values for output:
        str_match_type = ("%s-%s" 
          %( str(sr.match_algorithm),
             str(sr.tolerance_type))
          )
        if sr.tolerance_type == 'stat':
            stat0 = self.counts[0]
            stat1 = self.counts[1]
        else:
            # Asume tolerance_type is 'absolute'. Meaning to show percentages
            # instead of counts. 
            stat0 = str(self.percents[0])
            stat1 = str(self.percents[1])
        # Member diff may be int or float in some cases, so use str() 
        # for generic output.
        sdiff = str(self.diff)
        #comparison, aka compID need not be int, so use str
        scomparison = str(sr.comparison_id)
        base0 = (parent_entry.counts[0])
        base1 = (parent_entry.counts[1])
        if dict_out is not None:
            # Put output values in dict_out. The key values must match
            # those in LongEntry.report_column_names to be seen on the report.
            # Pasted reminder: report_column_names = 
            # ["MatchType","base","compare","diff","CompID","baseN",
            #  "compareN"]
            dict_out['MatchType'] = str_match_type
            dict_out['base'] = stat0
            dict_out['compare'] = stat1
            dict_out['diff'] = sdiff
            dict_out['CompID'] = scomparison
            dict_out['baseN'] = base0
            dict_out['compareN'] = base1
        # Put out an ordered list of column values to be processed separately
        # from dict_out. 
        outcols= [str_match_type, stat0, stat1, self.diff,
          sr.comparison_id, base0, base1]
        return outcols
        """
        output = ( "%s,%s,%s,%s,"
          "%s,%d,%d\n"
          % (str_match_type,
             str(stat0), str(stat1), sdiff, 
             scomparison,
             base0, base1) )
        return output
        """