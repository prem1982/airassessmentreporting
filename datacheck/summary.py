from collections import OrderedDict
from dataset import *

class SummaryColumn(object):
    def __init__(self, name=None, data_type="nominal"):
        self.name = name
        if name is None:
            raise ValueError("Name cannot be None")
        if data_type != "nominal":
            raise ValueError(
              "Only nominal buffer_ type is allowed now. You gave '%s'" 
              % (data_type))
        self.data_type = data_type
        self.d_vals = {}
    def __repr__(self):
        return ("%s:colum='%s(%s)'." 
          % (self.__class__.__name__, repr(self.name)
          ,repr(self.data_type)))
class SummaryColumns(object):
    def __init__(self, column_names):
        self.columns = []
        self.num_columns = 0
        self.d_columns = {}
        for cn in column_names:
            v = self.d_columns.get(cn)
            if v is not None:
                raise ValueError(
                  "Column name '%s' is duplicated" % cn)
            column = SummaryColumn(cn)
            # print "Created Column named '%s'" % column.name
            self.columns.append(column)
            self.d_columns[cn] = column
            self.num_columns += 1 
    def __repr__(self):
        return ("%s:columns='%s'." 
              % (self.__class__.__name__, repr(self.columns)))
class SummaryNode(object):
    def __init__(self, parent=None, EntryClass=None, coords=None):
        """ Initialize a summary node for a Summary tree structure.
        """
        if EntryClass is None:
            raise ValueError("EntryClass is None.")
        self.parent = parent
        #Create a new EntryClass object when needed
        self.entry = EntryClass()
        self.EntryClass = EntryClass()
        self.coords = coords
        # dictionary of buffer_ levels for this entry
        self.d_level = {}
    def __repr__(self):
        return ("%s:coords='%s'." 
              % (self.__class__.__name__, repr(self.coords)))
        
class Summary(object):
    def __init__(self, column_names=None, EntryClass=None, summary_report=None, base_summary=None):
        """ At least one column_name is required. See method cell() for more details.
        self.dict is the root of nested ordered dictionaries, each is a dimension 
        for a cross-tabulation.
        Caller may provide the comparison value. This will be output for every row of the 
        summary output report as column "Comparison" ala SAS longcomp() tradition.
        Param EntryClass must have methods as has LongEntry for same purposes.
        TODO: Implement the init from parameter base_summary not None:
           - fail (ie raise uncaught exception) if that base_summary is not yet summarized
           - fail if EntryClass is also a given param -- the new summary must re-use the EntryClass of
             the base_summary.
           - create a new summary that inits self.summarized == 1 - so it can only be report()'ed.
           - the param column_names must be a subset of the base_summary's column names, also
             commonly known as pivot columns. If not, fail.
           - the new summary will provide another report()'able view of base_summary, ala pivot columns
           - this will save much processing time over re-reading the raw buffer_ into a new summary
             because the new summary (pivot) may be calculated from the significantly lower quantity 
             of pre-gathered and summarized entry values in the base_summary than in the raw buffer_.
        """
        if column_names is None:
            raise ValueError ("Required parameter column_names not given.")
        self.column_names = column_names
        if EntryClass is None:
            ValueError ("Required parameter EntryClass not given.")
        self.summary_columns = SummaryColumns(column_names)
        self.columns = self.summary_columns.columns
        self.num_columns = self.summary_columns.num_columns
        self.EntryClass = EntryClass
        self.num_cells = 0
        self.num_nodes = 0
        if summary_report:
            self.summary_report = summary_report
        else:
            raise ValueError("Required parameter summary_report is empty.")
        self.summarized = 0
        # Init grand root node of the summary buffer_ tree.   
        self.grand = SummaryNode(EntryClass=self.EntryClass)
    def __repr__(self):
        prefix = "%s" % (self.__class__.__name__)
        return ("%s:columns=%s,EntryClass=%s" 
          % (prefix, repr(self.columns), repr(self.EntryClass)))
    def cell(self, coords=None):
        """ Return the specified cell entry, and create it first if not extant.
        Coords is a list of nominal-valued coordinates. 
        If the entry does not exist, create entry=EntryClass() in proper tree location. 
        """
        if coords is None:
            raise ValueError("coords is None. A total of %d coords is required."
                % (self.num_columns) )
        if len(coords) != self.num_columns:
            raise ValueError(
              "This Summary requires '%d' coords, but caller gave %d coordinates."
              % (self.num_columns, len(coords)) )
        # Start at grand root node of summary tree and go depth-first.
        node = self.grand
        n_coord = 0;
        for idx,coord in enumerate(coords):
            # print "n_coord=%d, coord='%s'" % (n_coord, coord)
            d_level = node.d_level
            next_node = d_level.get(coord)
            if next_node is None:
                #No node exists for ths coord yet, so create one.
                next_node = SummaryNode(EntryClass=self.EntryClass, parent=node, 
                  coords=coords[0:idx+1])
                #Create parent dict entry with coord for key, next_node for value.
                #print ("Creating node for coord='%s'" % coord)
                d_level[coord] = next_node
                self.num_nodes += 1
                if n_coord == (self.num_columns -1):
                    # This node is created at the last level of summary
                    # tree, so
                    # this holds a raw data cell. Increment the number of
                    # data cells made to hold accumulated raw data
                    self.num_cells += 1
                else:
                    # Create a new coordinate in the Summary space, and its
                    # own dictionary for encountered data values
                    # If this coord is new, register this as an encountered column 
                    # value so that summarize()functions can accrue values in the space.
                    column = self.columns[n_coord]
                    presence = column.d_vals.get(coord)
                    if presence is None:
                        column.d_vals[coord] = 1;
            node = next_node
            n_coord = n_coord + 1
        return node
    def node_accrue_children(self, node=None):
        """ To given node, accrue child entries from all deeper nodes in the summary and return the node's entry.
        The entry has a method accrue_entry() that is not necessarily a simple sum, as
        it could be any arbitrary processing to accrue one entry's values to another.
        """
        if node is None:
            raise ValueError("node is None.")
        entry = node.entry
        # Visit each child entry and accrue them to this entry.
        if node.d_level is not None:
            for child_node in node.d_level.values():
                entry.accrue_entry(entry=self.node_accrue_children(child_node))
        return entry
    
    def node_summarize(self, node=None):
        """ For each cell entry, calculate summary values, possibly dependent on parent entry values.
        """
        if node is None:
            raise ValueError("node is None.")
        if node.d_level:
            # Node has a d_level, so it is in Summary tree above the terminal cell level,
            # so summarize child nodes.
            for child_node in node.d_level.values():
                self.node_summarize(node=child_node)
        else:
            # This node entry is at the cell level, so summarize entry using parent-level
            # stats like totals and averages
            node.entry.summarize(parent_entry=node.parent.entry, 
                 summary_report = self.summary_report)
    def summarize(self):
        """
        Gather entry data to parent entries in the tree and call summarize for each cell entry.
        """
        if self.summarized:
            raise ValueError("This summary was already summarized.")
        # Accrue data to parent entries child entries.
        self.node_accrue_children(self.grand)
        # temporary test output
        # print "summary.summarize(): Grand count[0]=", self.grand.entry.counts[0]
        # print "summary.summarize(): Grand count[1]=", self.grand.entry.counts[1]
        # Visit the summary tree and call entry.summarize(parent_entry) to calculate 
        # each entry's summary statistics.
        self.node_summarize(self.grand)
        # Mark this summary as summarized
        self.summarized = 1
    def node_report(self, 
      node=None, dict_writer=None, dict_out=None, 
      coord_var_names=None, coord_val_names=None):
        """ Method node_report() adds final entries to dict_out for node coords and entry, and
        if an entry has output, calls dict_writer to write it.
        """
        if node is None:
            raise ValueError("node is None.")
        if coord_var_names is None:
            raise ValueError("coord_var_names is None.")
        if coord_val_names is None:
            raise ValueError("coord_val_names is None.")
        node_vals = []
        # track depth of recursion
        is_entry = 0
        # parent_entry = node.entry
        if node.d_level:
                        # Node has a dict d_level, so it is above the cell level, so it has child nodes.
            is_entry = 0;
            for key,child_node in node.d_level.iteritems():
                # Funky AIR Longcomp() condition - do not report missing 
                # value if > 1-way frequency
                if (self.summary_report.missing_level_report == False 
                    and key =='') :
                    continue;
                child_entry, child_cols = self.node_report(
                  node=child_node, dict_out=dict_out, 
                  dict_writer=dict_writer, 
                  coord_var_names=coord_var_names, 
                  coord_val_names=coord_val_names)
                # Avoid unwanted recursive nesting of lists for reports of varying rank.
                if (len(child_cols) > 0):
                    if child_entry:
                        node_vals.append(child_cols)
                    else:
                        node_vals.extend(child_cols)
        else:
            # d_level dict is empty, so this node entry is at the 
            # cell level, 
            # so if node.entry generates output values, then report it.
            # print "node_report: calling entry_output..."
            is_entry = 1
            entry_vals = node.entry.report(
              parent_entry=node.parent.entry, 
              summary_report=self.summary_report,dict_out=dict_out)
            if entry_vals:
                # Here we should emit a report row, so prepare leader 
                # and coord values for it.
                # append any lead column values
                if self.summary_report.dict_leader:
                    for key,val in self.summary_report.dict_leader.iteritems():
                        node_vals.append(val)
                    pass
                for idx, name in enumerate(self.column_names):
                    if dict_out:
                        dict_out[coord_var_names[idx]] = name
                        dict_out[coord_val_names[idx]] = node.coords[idx]
                    # append column values for ccoord name and value
                    node_vals.append(name)
                    node_vals.append(str(node.coords[idx]))
                # Output this report row to an optionally-defined dict_wirter
                if dict_writer:
                    dict_writer.writerow(dict_out)   
                #Accrue to node_vals[] the entry's output row data in
                #  entry_vals[]
                node_vals.extend(entry_vals)
        # Return is_entry and the list of report rows for all entries under 
        # this node
        return (is_entry, node_vals)
    
    def get_report_column_names(self):
        """ Derive the list of report columns and return it. 
        """
        # Compose the list of report_column names required for
        #  summary_report.dsw.DictWriter()
        sr = self.summary_report
        dict_leader = sr.dict_leader
        dict_out = sr.dict_out
        column_names = self.column_names
        report_column_names = []
        #if dict_leader is not None and dict_out is not None:
        if dict_leader is not None and dict_out is not None:
            for key,value in dict_leader.iteritems():
                #print "Adding report_column_name(from dict_leader)=",key
                report_column_names.append(key)
                dict_out[key] = value
        # We have to initialize the DictWriter with the report_column_names
        # below. 
        # Also need matched coord_val and var names for calling node_report()
        # below, so we do this duplication of storage of names.  
        coord_var_names = []
        coord_val_names = []
        for idx, column_name in enumerate(column_names):
            var_name = "Var_%s" % str(idx+1)
            report_column_names.append(var_name)
            coord_var_names.append(var_name)
            val_name = "Val_%s" % str(idx+1)
            report_column_names.append(val_name)
            coord_val_names.append(val_name)
        # Add the entry report_column_names
        report_column_names += self.EntryClass.report_column_names
        return report_column_names
    def report(self):
        """ 
        Report on the Summary data using the parameters in SummaryReport.
        Return value outut_rows is the brief report.
        If argument dict_out and dsw_full_report are provided, a full report that is not sorted
        nor limited by the number of rows in the max_bad parameter is output as well.
        Note to caller: If only bad rows should be reported, and no bad rows 
        should be reported, then no rows should be reported. 
        Caller can avoid creating, collecting, and reporting such a Summary.
        Caller may provide leader_column_names and matching values that will be the left-most 
        columns listed for the report lines.
        Param leader_names is used for caller LongComp() which, if about to report a Summary()
        with only 1 column, will prepare a leader column "ByVariable" and "ByValue" and supply 
        respective leader_values ["(all)", ""] to emulate current SAS program Longcomp output.
        Food for thought: Should ALL this logic should be moved to LongComp, above this call... to compute and output
        the initial row of header column names. Caller already knows summary, summary_report and
        entry objects. One goal of object Summary is to be agnostic about the type of Entry
        and what calculations and reports it serves.
        Only leader_values are passed as report params and onward to self.node_report.
                However, node_report must print coordNname, comma, coord1value for all coords so as to
        fit into a csv style of output file, into columns emulating current SAS longcomp report.
        """
        # Compose the list of report_column names required for 
        # summary_report.dsw.DictWriter()
        sr = self.summary_report
        dict_leader = sr.dict_leader
        dict_out = sr.dict_out
        report_column_names = []
        if dict_leader is not None and dict_out is not None:
            for key,value in dict_leader.iteritems():
                #print "Adding report_column_name(from dict_leader)=",key
                report_column_names.append(key)
                dict_out[key] = value
        # We have to initialize the DictWriter with the report_column_names
        # below. 
        # Also need matched coord_val and var names for calling node_report()
        #  below,
        # so we do this duplication of storage of names.  
        coord_var_names = []
        coord_val_names = []
        for idx, column_name in enumerate(self.column_names):
            var_name = "Var_%s" % str(idx+1)
            report_column_names.append(var_name)
            coord_var_names.append(var_name)
            val_name = "Val_%s" % str(idx+1)
            report_column_names.append(val_name)
            coord_val_names.append(val_name)
        # Add the entry report_column_names
        report_column_names += self.EntryClass.report_column_names
        # Instantiate dsw.DictWriter with report column names
        # 4 lines follow for quick test output
        columns_string = ""; sep = ""
        for i,cn in enumerate(report_column_names):
            columns_string += sep + cn
            sep = ", "
        if sr.dsw_full_report is not None:
            # Instantiate the dict writer to write only one-row at a time,
            # rather than buffer the entire report in memory before
            # outputting, to reduce memory footprint of 
            # large reports.
            # The caller assumes responsibility to sort such a large report 
            # as needed, and to produce a view of only the 'max_bad' rows, 
            # if needed; for example, by loading the full report
            # into a sql table and after it is populated by this routine, 
            # using its facilities to sort and manipulate the report rows.
            dict_writer = (self.summary_report.dsw_full_report
                           .dict_writer(report_column_names))
            if sr.write_header:  
                # write the header row
                dict_writer.writeheader()
        else:
                        dict_writer = None
        # Accrue output data values for a buffered report, separate from a 
        # report that node_report may write, row by row, using dict_writer. 
        # The output collected here may be further quickly sorted and 
        # examined without having to  reread the file that dict_writer 
        # writes to.
        # Coord data output is formatted in node_report().
        # node_report() adds final entries column data to dict_out for 
        # node coords and entry, and
        # if an entry has output, calls dict_writer to write it.
        is_entry, outrows = self.node_report(
          self.grand, 
          dict_out=self.summary_report.dict_out, 
          dict_writer=dict_writer,
          coord_var_names=coord_var_names, 
          coord_val_names=coord_val_names)
        return outrows 

class Collector(object):
    def __init__(self, datasets=[], summaries=[], ordinal_columns=[]):
        """A collector reads a dataset and stores it as summary buffer_ into one or more summary tables.
        Param ordinal_columns, if given, is a list of buffer_ column names whose encountered buffer_ values 
        from collected datasets will be passed along to the summary cell entry's accrue_row() function.
        Future (put this logic in caller):(1) detect summaries that are pivots of other summaries, 
        marking them as sub_summaries.
        (2) Do not collect into sub_summaries
        (3) after collection of non-sub summaries, call summary.summarize()
        for each non-and set summary.summarized = 1.
        (4) create the sub_summaries aptly from their base summaries.
        """
        if not datasets:
            raise ValueError("Parameter datasets is empty. A list of Datasets is required.")
        self.datasets = datasets
        if not summaries:
            raise ValueError("Parameter summaries[] is empty. A list of Summary objects is required.")
        self.summaries = summaries
        # future- add checking here that each dataset has the column names
        # required by each summary,
        # and same for ordinal_columns when they are implemented. 
        # Or do checking in Dataset().
    def __repr__(self):
        return ("%s:datasets=%s, summaries=%s." 
          % (self.__class__.__name__, repr(self.datasets),
             repr(self.summaries)))
    def collect(self):
        """ Collect some Datasets of raw data into each given Summary.
        """
        for idx_ds, ds in enumerate(self.datasets):
            # print "collect() using ds = ", repr(ds)
            reader_rows = ds.dict_reader()
            for row in reader_rows:
                # print "Collect: idx_ds=%d" % idx_ds
                for summary in self.summaries:
                    #Derive coords from the row for this summary
                    coords=[]
                    for sum_col in summary.columns:
                        level = str(row[sum_col.name])
                        if level is not None and level != "None": 
                            if level.find('.') != -1:
                                # May be a float value with .0 ending to trim
                                try:
                                    # If value is parsable as a float, and it 
                                    # is an integer, represent it as an integer.
                                    flevel = float(level)
                                    # Strip a final .0 from the string.
                                    level = (
                                      str(int(flevel)) if flevel == int(flevel)
                                      else str(level))
                                except:
                                    # Not a float, OK.
                                    pass
                        else:
                            level = ""
                        coords.append(level)
                    #print "coords:", repr(coords)
                    #Register row data into this summary.
                    cell = summary.cell(coords)
                    #Future, along with ds_index, could also pass along 
                    # row's ordinal column values.
                    # Note to self: rename accrue_row to accrue_row() 
                    # when get into eclipse env
                    cell.entry.accrue_row(idx_ds)
#

class SummaryReport(object):
    def __init__(self, comparison_id=0, dict_leader=None, 
      dsw_full_report=None,
      match_algorithm=None, 
      tolerance=-100.0, tolerance_type='stat', 
      missing_counted=True, max_bad=10,  
      missing_level_report=True, 
      write_header=False
      ):
        """ 
        This is passed along unexamined by a Summary to its node's entry.* methods
        to use for its own purposes, mainly do calculations and report outputs.
        Parameters:
        missing_level_report: bool
        - flag to support a longcomp report generation rule to skip missing buffer_ only for N way summaries, where n > 1.
        dsw_full_report - optional output dataset:
             Instantiate the dict writer to write only one-row at a time, rather than
            # buffer the entire report in memory before outputting, to reduce memory footprint of 
            # large reports.
            # The caller assumes responsibility to sort such a large report as needed, and to produce
            # a view of only the 'max_bad' rows, if needed; for example, by loading the full report
            # into a sql database and using its facilities to sort and manipulate the report rows.
        Notes:
        This class might be better named LongEntryReport(), or LongEntrySummary, as it has 
        parameters that are passed down to entry.summarize() and entry.report() methods
        to control their processing, and now only EntryClass LongEntry is needed.
        Future: this object takes kw dictionary of extra report column names and values.
        This is an attempt to keep the parameters that entry methods use separate from the 
        EntryClass itself simply to save space. Each entry will use the same static 
        parameters.
        Another implementation is to make such parameters class members of LongEntry,
        but their values do change for 13 separate LongEntry reports for the LongComp() 
        implementation, so jamming these settings into LongEntry class members seems
        ugly and not explicit. And beautiful is better than ugly and explicit 
        is better than implicit.
        """
        self.comparison_id = comparison_id if comparison_id else 0
        self.write_header = write_header if not write_header else True
        # May add check later for comparison value in some range...
        if match_algorithm is None:
            raise ValueError("Missing match_algorithm.")
        if match_algorithm not in ('proportion'):
            raise ValueError("match_algorithm %s is unknown." % repr(match_algorithm))
        self.match_algorithm = match_algorithm
        self.dsw_full_report = dsw_full_report
        if tolerance_type not in ('absolute','stat'):
            raise ValueError("Unknown tolerance_type=%s." % repr(tolerance_type))
        self.tolerance_type = tolerance_type
        self.tolerance = tolerance if tolerance else 0
        if type(missing_counted) != bool :
            raise ValueError("Param missing_counted type must be bool.")
        self.missing_counted = True if missing_counted == 'yes' else False
        if type(missing_level_report) != bool:
            raise ValueError("Param missing_level_report type must be bool.")
        self.missing_level_report = missing_level_report
        self.max_bad = int(max_bad)
        # Caller must provide OrderedDictionary of named leader name/value pairs for 
        # start of every output report line
        self.dict_leader = dict_leader 
        # Dictionary of outut column names and values used of dsw_full_report is defined
        self.dict_out = OrderedDict() if dsw_full_report is not None else None

