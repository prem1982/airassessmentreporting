
import os
import logging
import xlrd

__all__ = [ 'FormatOut' ]

class FormatOut( object ):
    """ 
    Interpret a row output format specification and write data rows with it.
    """
    def __init__(
      self, fout, spec=None, lskip=0, layout=None, sheetname=None):
        """
        Construct a FormatOut object.
        
        Extended Summary: 
        ----------------- 
        Given an input spec name, read other input parameters to derive the output specifications to use to output rows of one or more ordered buffer_ values.
        
        Parameters:
        ----------------
        fout : file object
          A file handle already opened for writing on which output may be written by method writerow().
                  
        spec : string
          Name for a type of format specification.  
                    
        Other parameters when spec value == 'spec1':
        ............................................ 
        lskip : int
          The number of initial rows in a layout file to skip before processing. Default is 0.
          
        layout: string
          A file path to an excel workbook file with output layout parameters. The first sheet in the workbook is used for the output layout information. See method _read_layout() for details on the sheet contents.
                      
        Returns:
        --------
        The FormatOut object that is initialized with an output format in member fmtstring.
   
        """

        self.fout = fout
        # lskip is num
        self.lskip = lskip
        # Consider setting up fout with a large file buffer here for 
        # speed-up if warranted.
       
        self.fmtstring = ""
        self.format_string = ""
        self.layout = None
        self.columns = ""
        self.wb = None
        if spec is None:
            spec = 'spec1'
            
        if spec == "spec1":
            if layout is None:
                raise ValueError(
                  "spec is '%s', but no layout is given" % spec)
            with xlrd.open_workbook(layout) as self.wb:
                if sheetname is not None:
                    self.sheet = self.wb.sheet_by_name(sheetname)
                else:
                    self.sheet = self.wb.sheet_by_index(0)
                
                logging.info(
                  "FormatOut(): spec='%s', layout file = '%s',"
                  " sheet name='%s'" 
                  % (spec, layout, self.sheet.name))
    
                self._read_layout()
            
        elif spec == "assignments":
            # Just output colnames and values, not really fixed width.
            for c in self.colnames:
                self.fmtstring += (", colname=%s:value="  % c ) 
                self.fmtstring += "%s"
            
        else:
            raise ValueError("spec='%s' not implemented." % spec)
        
        self.fmtstring += os.linesep        
    
    def __del__(self):
        if (self.wb):
            self.wb.release_resources()
                      
    def _read_layout(self):
        """
        Input and validate layout information for the lspec values of 'spec1' and 'assignments'.
        
        Extended Summary:
        -----------------
        Read rows of the Excel sheet in self.sheet and verify: 
        
        1. column 1 has a numeric start value that is a column number for output, 
        2. column 2 is a numeric value for the ending column in which to place output, 
        3. column 3 is a numeric length value that  must be correct, 
        4. column 4 is an input column name that must either:
        
         - appear in self.columns (maybe allow upper-casing of table column names to match upper case version of layout colum names) 
         - or be 'filler', in which case blanks are output in the field.
        
        5. column 5 is the 'type', and if a value starts with 'alpha', method writerow()  will left-justify output for the column,  otherwise output for the column will be right justified within the output field. 
        
        
        Parameters: 
        -------------
        No formal ones. This is a private function that processes Context member data and modifies format_string.
        
        Returns:
        --------
        Context member format_string is built and returned.
        
        Member format_string will be used by method writerow() to format output to the fout file object.
        
        Notes:
        ------
        Layout Sheet Details: 
        .....................
        If the layout sheet has any row where the first column is empty, we assume we should simply skip that physical line in the layout sheet.
        
        This is because this type of row is frequently used in layout files to allow special legends for data values to be managed in cells/columns beyond the fifth column that we need to use here.
        
        Development Notes:  
        ..................
        - Sift through these Ideas for future checks and issues:
        - Raise an exception on any error in the layout, further constraining that the start columns be ascending, that numeric columns contain and any other logical checks that need be done. 
        - Maybe no duplicate column output names in the layout should be allowed, or maybe allow it with an option?
        
        - Future: 
         - Add an option that requires that every table or input column name be referenced by a layout column name? May be helpful to users.
         - Add an option to warn in output log when and if output value truncation must/does occur.
               
        - Potential issue: when/if layout column names do not include all table column names and in that order -- may need to add mechanism to writerow() to pick out the correct layout-named column values from the table-selected row of values, at the expense of speed to manage this mechanism.
        
        - Another alternative: Allow the export function to, as normal, use __init__() parameter  "column" names for the input data selection and simply make the user responsible for using a layout that matches the order and count of those columns. This is the simplest approach, meaning there is no need to read the layout column names at all. 
               
        - Consider: Maybe also zero-fill to the left? - or make an option, or always DO zero fill. If this is an issue, let users raise this issue.     
        
        """
        self.fmtstring = ""
        #For every layout row:
        # Append to fmtstring as the layout is read in.
        self.format_string = ""
        s = self.sheet
        logging.info("20130516 _read_layout(): self.sheet.name='%s'" 
            % (self.sheet.name))

        started = 0
        # So that writerow can issue warnings if values exceed 
        # column lengths
        self.collengths = []
        multcol = None;
        nrow = 0
        for row in range(s.nrows):
            nrow += 1
            if nrow <= self.lskip:
                continue
            start = s.cell_value(row,0)
            istart = int(float(start))
            if not started and start == "Start":
                started = 1
                continue;
            endcol = s.cell_value(row,1)
            iendcol = int(float(endcol))
            length = s.cell_value(row,2)
            ilength = int(float(length))
            desc = s.cell_value(row,3)
            justify = s.cell_value(row,4)
            logging.info(
              "_read_layout():start=%d, endcol=%d, length=%d, "
              "desc='%s', justify='%s'"
              % (istart, iendcol, ilength, desc, justify))

            if ( ilength < 1 or ilength != ( iendcol - istart + 1) ):
                raise ValueError(
                  "Layout row %d bad values start=%s, end=%s, length=%s" 
                  % (nrow, start, endcol, length))
            self.collengths.append(length)

            if desc == 'filler':
                filler = ' ' * int(length)
                self.fmtstring += filler
                self.format_string += filler
            else:
                # got a non-filler column name
                if multcol:
                    self.columns += ','
                multcol = 1
                self.columns += desc.strip()
                if (justify.startswith("alpha")):
                    leftjust = 1
                else:
                    leftjust = 0
                if leftjust:    
                    self.fmtstring += "%-" + ("%d" % length) + "s"
                    # left aligned string output, truncated to length
                    self.format_string += ("{!s:<%d.%d}" % (length,length))
                else:
                    self.fmtstring += "%" + ("%d" % length) + "s" 
                    # right aligned string output, truncated to length
                    self.format_string += ("{!s:>%d.%d}" % (length,length))

        #RESUME self.format_string += "{" + ("%d.%d") 
        self.format_string += os.linesep                     
        logging.info(
            "_read_layout():format_string='%s'"
            % (self.format_string))
        
        print ("_read_layout():format_string='%s'"
            % (self.format_string))

        return
        
    def writerow(self,row):
        """
        Given ordered list of string values, write them in formatted output fields.
        
        Extended Summary:
        -----------------
        Receive a list of  data values positionally matched with the table's selected output columns and write a line of the data to the output file in the given layout format.
        
        Method writerow() is the main worker method of this FormatOut object, and it accepts a row of data as a list of string values from its caller and writes an ordered list of data values to file object 'fout' in accord with some of the __init__ parameters that are processed by _read_layout(). 
                      
        Returns:        
        ---------
        Expected Written File Output via method writerow() :
        ....................................................
        A physical file line is output to the output file with column values.
        
        1.  left-align the column values that have a type value starting "alpha" in the layout
        2.  right-align all other column values within the output field. 
          
        Notes:
        ------
         
        1. The method name writerow is not strictly PEP-8, but here it is set to match the venerated cvs.writerow() method name as a convenience for some callers.   
                     
        Overflow check?
        ...............
        Add a FormatOut column layout spec field "overflow" and if set to 1, then cycle through each row's column values and issue a warning if the value length exceeds self.collengths[x] for that column. 
        
        If set to 2, issue an error, and if 3, say, raise an exception - depending on user needs. 
        
        In current code, such checking and reporting is not done, and fixed column output support is expected to disappear by 2015, so maybe not a priority.
        """
         
        #self.fout.write(self.fmtstring % tuple(row))
        self.fout.write(self.format_string.format(*row))
        
        return
