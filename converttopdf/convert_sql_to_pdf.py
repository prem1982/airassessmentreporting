'''
Created on Mar 19, 2013

@author: zschroeder
'''
import pyodbc
from string import Template

from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, cm, inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet
import xlrd
from airassessmentreporting.airutility import *

##########################################################################
## Note: The functionality for table wrapping (if > 9 columns it splits
##        into multiple tables and appends them) is only implemented
##        in ConvertXLStoPDF. It can be inserted into ConvertSQLtoPDF
##        also but it has not been done yet. It is as simple as copying
##        code though and adding the parameter.
##########################################################################

__all__ = [ 'ConvertSQLtoPDF', 'ConvertXLStoPDF' ]


def ConvertSQLtoPDF(columns=['*'],    #column names to select from table - if * is specified it will get the names from the table itself
                    db_context=None,# DBContext in which this will be run
                    tablename='',   #name of table that contains the data we need to query
                    outputname=''   #name of output file
                    ):
    """This function converts a SQL table into a PDF file

        Extended summary
        -------------------------
        This will convert a SQL table to a PDF file. We create a select statement from the list of
        columns provided. If ["*"] is provided then we get all columns.
        
        Parameters
        --------------
        columns : List of Strings
            This should be the list of column names to get. If ["*"] is specified we query the table to get
            all column names and then create our select statement getting all columns. The default value is ['*']
            
        db_context : DBContext object
            The DBContext in which this will be run
            
        tablename : String
            This should be the name of the table to query.
        
        outputname : String
            This should be the String name of the output PDF file
        
        Returns
        ----------
        Nothing. It only creates the PDF file.        
        
        Notes
        --------
        This code requires reportlab to be installed.
    """
    if len(columns) == 0:
        raise Exception("Please enter the column names to be selected")
    if db_context is None:
        raise Exception("Please enter the DBContext")
    if tablename.strip() == '':
        raise Exception("Please enter the table name")
    if outputname.strip() == "":
        raise Exception("You must enter an output file name")
    if ".pdf" not in outputname:
        outputname += ".pdf"
    run_context = db_context.runContext
    ##check table exists
    if not table_exists( tablename, db_context ):
        raise Exception("Table \"" + tablename + "\" does not exist")
    #define the pdf document
    doc = SimpleDocTemplate(outputname, pagesize=letter)
    #get styles
    styles = getSampleStyleSheet()
    styleH = styles["Normal"]
    styleH.alignment = TA_CENTER
    #define our header
    header = Paragraph("""
        <i><b>AIR Tests</b></i><br/>
    <i><b>Erasure Analysis</b></i><br/>
       <i><b>Test Admin </b></i><br/>
    """,styleH)
    #define list of flowable objects to add to document and add our header to it
    elements = []
    elements.append(header)
    #create the list that will hold our table of data
    table_list = []
    #create list to hold our column header paragraphs 
    col_headers = []
    #if they specify * (all columns in SQL) then we go to the table and get the column names
    if columns[0].strip() == "*":
        columns = []
        tablespec = db_context.getTableSpec(tablename)
        for col in tablespec:
            columns.append(db_identifier_unquote(col.field_name))
    #create table headers
    for colname in columns:
        col_headers.append(Paragraph(Template("""<b><font size="5">$colname</font></b>""").substitute(locals()),styles["Normal"]))
    #add headers to table_list
    table_list.append(list(col_headers))
    #setup and run the select query, and add each row in the table to table_list to store the table in memory
    for row in db_context.executeBuffered("select " + ",".join(columns) + " from " + tablename):
        table_list.append(list(row))
    t = Table(table_list)
    #define grid for the table
    t.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                           ('FONTSIZE', (0, 0), (-1, -1), 5), 
                           ('LEFTPADDING',   (0,0), (-1,-1), 0.1*cm),
                           ('RIGHTPADDING',  (0,0), (-1,-1), 0.1*cm),
                           ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
                           ('BOX', (0,0), (-1,-1), 0.25, colors.black),
                           ]))
    elements.append(t)
    # write the document to disk
    doc.build(elements)
    
def ConvertXLStoPDF(filename='',       #xls file name to grab
                    outputname='',     #output pdf file name
                    stretchPage='N' # if 'Y' will stretch the page to fit the data, if 'N' it will break the tables with too many
                                    #    columns into multiple tables (any more than 9 columns get split)
                                    # When stretching the page, it sets the page width and height to:
                                    #    (number of columns + 2) inches. This gives about an inch for each column
                                    #    with margins of an inch around the outside. It should fit mostly any table.
                    ):     
    """This function converts an XLS file to a PDF file

        Extended summary
        -------------------------
        This will convert an excel file to a PDF file. It has an optional parameter to stretch
        the page to fit the number of columns in the table. The default is to not stretch the page. 
        If you do not stretch the page then the table will go up to the default 9 columns, and any
        further columns will be moved into a new table.
        
        Parameters
        --------------
        filename : String 
            This should be the full path and name of the excel file
            
        outputname : String
            This should be the String name of the output PDF file
            
        stretchPage : String
            This should be either 'Y' or 'N' or not specified. It is 'N' by default. If 'N' then we split the
            worksheet into tables with 9 rows each. If 'Y' then we put each sheet in its own table and the page
            stretches to fit the size of the table. The page width and height get set to
            (number of columns + 2) inches. This gives about an inch for each column with margins of an inch
            around the outside. It should fit mostly any table.
        
        Returns
        ----------
        Nothing. It only creates the PDF file.        
        
        Notes
        --------
        This code requires reportlab to be installed.
    """
    if filename.strip() == "":
        raise Exception("You must enter an input xls file name")
    if outputname.strip() == "":
        raise Exception("You must enter an output file name")
    if ".pdf" not in outputname:
        outputname += ".pdf"
    #define the pdf document
    doc = SimpleDocTemplate(outputname, pagesize=letter)
    #if stretchPage = 'Y' this will hold the maximum number of columns in all sheets
    max_num_cols = 9
    #get styles
    styles = getSampleStyleSheet()
    styleBH = styles["Normal"]
    styleBH.alignment = TA_CENTER
    #define list of flowable objects to add to document
    elements = []
    #define our header
    header = Paragraph("""
        <i><b>AIR Tests</b></i><br/>
    <i><b>Erasure Analysis</b></i><br/>
       <i><b>Test Admin </b></i><br/>
    """,styleBH)
    elements.append(header)
    wb = xlrd.open_workbook(filename)
    sheets = wb.sheet_names()
    #loop through the sheets one by one
    for sheet in sheets:
        #create the table from the query
        table_list = []
        table_list.append([]) # create a table in our list of tables
        #open the sheet
#        sh = wb.sheet_by_name(sheet)
        reader = SafeExcelReader(None,filename=filename,sheet_name=sheet)
        #list to hold tuples to add to table style (i.e. a tuple for making a row bold)
        table_styles = [('FONTSIZE', (0, 0), (-1, -1), 5), 
                       ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
                       ('LEFTPADDING',   (0,0), (-1,-1), 0.1*cm),
                       ('RIGHTPADDING',  (0,0), (-1,-1), 0.1*cm),
                       ('BOX', (0,0), (-1,-1), 0.25, colors.black)]
        #now loop through the rows of the current sheet one by one
        rownum = 0
        for row in reader.getRows():
            cols = row
            #if it is the first row of a sheet we assume there are column names in it so we make them bold
            if rownum == 0:
                for colname in cols.keys():
                    cols[colname] = Paragraph(Template("""<b><font size="5">$item</font></b>""").substitute(item=cols[colname]),styles["Normal"])
                    #table_styles[0].append(('TEXTFONT', (0, len(table_list)), (-1, len(table_list)), 'Times-Bold'))
            #this commented out loop below is for if you want word wrap in the tables. It slows down the 
            #    run a lot though so it isn't used unless you uncomment it.
#            for i in xrange(len(cols)):
#                if isinstance(cols[i],(float,int)):
#                    cols[i] = str(cols[i])
#                if not isinstance(cols[i], Paragraph):
#                    cols[i] = Paragraph(r'<font size="5">' + cols[i] + r"</font>",styles["Normal"])
            table_list[0].append(cols.values())
            rownum += 1
        if len(table_list) > 0:
            len_list = map(lambda x:len(x),table_list[0])
            max_len = max(len_list)
            split_cols = False
            if stretchPage.strip().upper() == 'Y':
                if max_len > max_num_cols:
                    max_num_cols = max_len
            #9 is the max number of columns excel allows on a pdf so we copy that
            if max_len > 9 and stretchPage.strip().upper() == 'N':
                max_len = 9
                split_cols = True
            #if there's more than 9 columns then we copy the extra columns to a different table 
            #    (create their own rows and append them to table_list)
            while split_cols:
                #create a new table in our list of tables
                table_list.append([])
                table_number = len(table_list)-1
                for cnt in xrange(len(table_list[table_number-1])): # want rows from the current table, not the new table we just created
                    row = table_list[table_number-1][cnt]
                    if len(row) > max_len:
                        #append any columns after 9 to a new table
                        table_list[table_number].append(row[max_len:])
                        #set row to be only first 9 columns
                        table_list[table_number-1][cnt] = row[:max_len]
                #check if our new table has too many columns, if so do it again 
                len_list = map(lambda x:len(x),table_list[table_number]) 
                max_len = max(len_list)
                split_cols = False
                if max_len > 9:
                    max_len = 9
                    split_cols = True
            for table in table_list:
                t = Table(table)
                #define attributes for the table
                t.setStyle(TableStyle(table_styles))
                #append table to elements list
                elements.append(t)
                #insert space between tables
                elements.append(Paragraph("""
                <br/>
                <br/>
                """,styles["Normal"]))
    #if we stretch the document to fit the page then we must resize it first
    if stretchPage.strip().upper() == 'Y':
        doc = SimpleDocTemplate(outputname, pagesize=((max_num_cols+2)*inch,(max_num_cols+2)*inch))
    # write the document to disk
    doc.build(elements)
    
    
#if __name__ == "__main__":
#    colnames = ["Grade","bcrxid_attend","dcrxid_attend","bcrxnm_attend","dcrxnm_attend","Lithocode","SSID","StudentID","pass_fail","subject","raw","erased","w_r","w_w","r_w"]
#    colnames = ["*"]
#    ConvertSQLtoPDF(colnames, RunContext('erasuredb'),tablename="erasure_out",outputname='sqlpdfoutput.pdf')
    #ConvertXLStoPDF("""C:\SpecSheet.xls""", "xlsTOpdf.pdf")
    #ConvertXLStoPDF(r"C:\CheatAnalysis.xls", "CheatAnalysis.pdf", stretchPage='y')


