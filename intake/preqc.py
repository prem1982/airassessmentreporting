'''
Created on May 14, 2013

@author: temp_plakshmanan
'''
from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote
from collections import OrderedDict
import itemizedcolumns
import layoutcheck
import datetime
import pprint
import copy
import re
import subprocess
import os

_INSERT_INTO_TABLE_QUERY = "INSERT INTO {0} ({1}) VALUES ({1:',',itemfmt='?'})"
ERROR_FMT = "%25s%25s%45s%25s%25s"

class PreQC(object):

    def __init__(self, runcontext, dbcontext, layoutfile = '', inputfile = '', patterns = [], OEpatterns = [], flat_tablename='',debug=False, bulk_insert = True, 
                 errorfile='' , outputsdir = 'c:/SAS/OGT/Joboutput/', table_names = 'TABLE_NAMES', output_table='Layout_Temp', lock=None, 
                 batchfile='bcpfile.bat', row_count=1 ):
        
        """This is the intialization step and it needs the following inputs
        to run
        runcontext = creates the logger methods
        dbcontext  = creates the db connection objects
        layoutfile = layoutfile that will be this process
        inputfile  = inputfile that will be this process
        pattern = pattern to identify the MC item fields
        OEpattern = pattern to identify the OE item fields
        flat_tablename = The flat tablename that will be created
        debuf = FALSE(default), this will print all the debug messages in the console
        bulk_insert = Indicator to specify to perform bulk inserts
        errorfile = This file will contain all the errors while processing
        outputsdir = All the outputs will be listed in this directory, If the directory does not exists it will be created.
        
        """
        self.lock=lock
        self.output_table=output_table
        self.RC = runcontext
        self.db = dbcontext
        self.logger = self.RC.get_logger( "preqc" )
        self.layoutfile = layoutfile
        self.inputfile = inputfile
        self.errorfile = errorfile
        self.outputrec = {}
        self.layoutdict = []
        self.maxmindict = []
        self.recodingsdict = {}
        self.reset_counter = 1
        self.patterns = patterns
        self.OEpatterns = OEpatterns
        self.OE_items_tables_collist = []
        self.OE_items_pos_tables_collist = []
        self.mc_items_tables_collist = []
        self.mc_items_table_names = []
        self.mc_items_table_field_names = {}
        self.item_tables_columns = []
        self.all_columns = []
        self.tablename = ''
        self.item_table = False
        self.debug = debug
        #Missing_column should be a Ordereddict as we need to add the MISSING Columns
        self.missing_columns = OrderedDict()
        self.tablenames = []
        self.column_names = []
        self.flat_tablename = flat_tablename
        self.bulkinsert = bulk_insert
        self.bulk_insert_list = []
        self.row_count = row_count
        self.t1 = datetime.datetime.now()
        self.t2 = datetime.datetime.now()
        if not os.path.exists(outputsdir):
            os.makedirs(outputsdir)
        self.f = open('c:/OGTValidation.txt', "w")
        self.error_file = open(self.errorfile,'w')
        self.insert_rows = []
        self.file_lists = []
        self.field_ind = 0
        self.outputs_dir = outputsdir
        self.pre_flat_qc_table_file = open(self.outputs_dir + self.flat_tablename + '.txt','w')
        self.batchfilename = batchfile
        self.batchfile = open(self.outputs_dir + batchfile,'w')
        self.statsfile = open(self.outputs_dir + 'loadstats.txt','w')
        open(self.outputs_dir + 'error.txt', 'w')
        batch_file_string = 'bcp ' + self.db.db + '.dbo.' +  self.flat_tablename +  ' in ' + self.outputs_dir +  self.flat_tablename + '.txt' \
                + ' -t "&!!" -c -S ' + self.db.server + ' -T  -e ' + self.outputs_dir + 'error.txt'
        self.batchfile.write(batch_file_string + '\n')
        qry = "CREATE TABLE " + table_names + " (subject_id char(5),tablename nvarchar(50) )"
        drop_table_if_exists( table_names, self.db)
        self.db.executeNoResults(qry)
        i=0
        for each in self.patterns:
            qry = "Insert into " + table_names + " values ('{0}','{1}')".format(each[4],each[3])
            self.db.executeNoResults(qry)
            i += 1
        qry = "Insert into " + table_names + " values ('{0}','{1}')".format('F',self.flat_tablename)
        self.db.executeNoResults(qry)


    def process(self):
        """ This is the main process that controls the flow of the main module.
        This module normalizes the data for each itemized pattern and creates one table for each of them.
         
        This module creates the following lists
        1. Lists of list - [self.mc_items_tables_collist] - for all itemized columns for each pattern.
        2. Lists of list - [self.OE_items_pos_tables_collist] - for all OE position columns for each pattern.
        3. Lists of list - [self.OE_items_tables_collist] for all OE count columns for each pattern.
        4. Unique list - [self.item_tables_columns] - to identify all columns that will be used to create FLAT table.
        """

        LC = layoutcheck.LayoutCheck(runcontext=self.RC, dbcontext=self.db, output_table=self.output_table,
                                                layoutfile=self.layoutfile)

        self.layoutdict, self.maxmindict, self.recodingsdict = LC.process()
        
        self.logger.debug( "Layoutdict = {0}".format(self.layoutdict))
        
        if self.layoutdict:
            print str(datetime.datetime.now()) + "  0"
            for k,v in self.layoutdict.items():
                self.all_columns.append(k)
            print '_get_mc_table_names()' 
            self._get_mc_table_names()
            print str(datetime.datetime.now()) + "  1"
            print '_get_mc_itemized_columns() start'
            self._get_mc_itemized_columns()
            print str(datetime.datetime.now()) + "  2"
            print'_get_OE_itemized_columns() start'
            self._get_OE_itemized_columns()
            print str(datetime.datetime.now()) + "  3"
            print '_get_flat_columns_in_table() start'
            self._get_flat_columns_in_table()
            print str(datetime.datetime.now()) + "  4"
            print '_prepare_flat_table_create_stmt() start'
            self._prepare_flat_table_create_stmt()
            print str(datetime.datetime.now()) + "  5"
            print'_prepare_mc_item_table_create_stmt() start'
            self._prepare_mc_item_table_create_stmt()
            print str(datetime.datetime.now()) + "  6"
            print 'Process start'
            del self.tablenames[0]
            self._create_input_files()
            print str(datetime.datetime.now()) + "  7"
            if len(self.all_columns) == len(self.flat_tables_columns) + len(self.item_tables_columns):
                self._create_input_dict()
            
            print str(datetime.datetime.now()) + "  8"
            self.pre_flat_qc_table_file.close()
            self.batchfile.close()
            print str(datetime.datetime.now()) + "  9"
            for each in self.file_lists:
                each.close()
            self.t2 = datetime.datetime.now()
            
            if self.lock is not None:
                self.lock.acquire()
            proc = subprocess.Popen(self.outputs_dir + self.batchfilename,stdout=subprocess.PIPE)
            if self.lock is not None:
                self.lock.release()
 
            print "MY MARKER 0 " + str(datetime.datetime.now())
            for each in iter(proc.stdout.readline,''):
                self.statsfile.write(each + '\n')

            print "MY MARKER 1 " + str(datetime.datetime.now())
            filename = self.outputs_dir + 'error.txt'
            if os.stat(self.outputs_dir + 'error.txt').st_size <> 0:
                print "LOADING ERRORS, CHECK THE ERROR REPORT"
            print "MY MARKER 2 " + str(datetime.datetime.now())
            self.t3 = datetime.datetime.now()
            self._create_indexes()
            print 'self.t1={0}'.format(self.t1)
            print 'self.t2={0}'.format(self.t2)
            print 'self.t3={0}'.format(self.t3)
            print 'Time_taken to prepare files to be loaded = {0}'.format(self.t2 - self.t1)
            print 'Time_taken to load files={0}'.format(self.t3 - self.t2)
            print 'Overall Time_taken = {0}'.format(self.t3 - self.t1)
            print 'Total Number of columns={0}'.format(len(self.all_columns))
            print 'Total Number of columns in Flat Table = {0}'.format(len(self.flat_tables_columns))
            print 'Total Number of columns in MC Table = {0}'.format(len(self.item_tables_columns))
            print 'Total number of Input records processed = {0}'.format(self.row_count - 1)
            
#     
    def _get_mc_table_names(self):
        """ This method gets all the itemized columns for a given matching pattern"""
        
        for each in self.patterns:
            if each in self.mc_items_table_names: 
                raise Exception("MC Item table names should be different")
            self.mc_items_table_names.append(each[3])
            
    def _get_mc_itemized_columns(self):
        """ This method gets all the itemized columns for a given matching pattern"""
        
        for each in self.patterns:
            IC = itemizedcolumns.ItemizedColumns(pattern=each[0],columns=self.all_columns)
            variables, variable_names = IC.process()
            self.mc_items_tables_collist.append(variables)
            self.mc_items_table_field_names.update(variable_names)
    
    def _get_OE_itemized_columns(self):
        """ This method gets all the OE columns for a given matching pattern. Creates 2 lists 1 - OE_Positions and 2- OE_Counts"""
        
        for each in self.patterns:
            IC = itemizedcolumns.ItemizedColumns(pattern=each[1],columns=self.all_columns)
            variables, variable_names = IC.process()
            self.OE_items_pos_tables_collist.append(variables)
            self.mc_items_table_field_names.update(variable_names)
        
        for each in self.patterns:
            IC = itemizedcolumns.ItemizedColumns(pattern=each[2],columns=self.all_columns)
            variables, variable_names = IC.process()
            self.OE_items_tables_collist.append(variables)
            self.mc_items_table_field_names.update(variable_names)
        
    def _get_flat_columns_in_table(self):
        """ This method merges all the columns together and identifies the unique column that needs to be used"""
        #All the columns from the items_tables are merged together to determine the lists of columns in the flat tables
        
        for eachitem in self.mc_items_tables_collist:
            for eachtable in eachitem:
                for eachcolumn in eachtable:
                    self.item_tables_columns.append(eachcolumn.upper())
        
        for eachitem in self.OE_items_pos_tables_collist:
            for eachtable in eachitem:
                for eachcolumn in eachtable:
                    self.item_tables_columns.append(eachcolumn.upper())
        
        for eachitem in self.OE_items_tables_collist:
            for eachtable in eachitem:
                for eachcolumn in eachtable:
                    self.item_tables_columns.append(eachcolumn.upper())
        self.flat_tables_columns = [ x.upper() for x in self.all_columns if x not in self.item_tables_columns]
    
    def _create_input_files(self):
        for each in self.patterns:
            filename = each[3] + '.txt'
            self.file_lists.append(open( self.outputs_dir + filename, "w"))
            errorfilename = each[3] + 'error.txt'
            open(self.outputs_dir + errorfilename,"w")
            batch_file_string = 'bcp ' + self.db.db + '.dbo.' +  each[3] +  ' in ' + self.outputs_dir +  each[3] + '.txt' \
                + ' -t "&!!" -c -S ' + self.db.server + ' -T  -e ' + self.outputs_dir + errorfilename 
            self.batchfile.write(batch_file_string + '\n')
            
    def _prepare_flat_table_create_stmt(self):
        """This method calls the build_table_columns method to create new columns for the flat table"""
        self.tablename = self.flat_tablename
        self._initialize_table()
        
        for eachfield in self.flat_tables_columns:
            self._build_table_columns(eachfield.upper(), table_ind=1)
            self.field_ind += 1
        self._create_table()
    
    def _prepare_mc_item_table_create_stmt(self):
        """This method calls the build_table_columns method to create new columns for the itemized table"""
        table_ind = 0
        OE_list_indicator = 0
        for eachitem in self.mc_items_tables_collist:
            self.tablename = self.mc_items_table_names[table_ind]
            self.item_table = True
            self.field_ind = 0
            self._initialize_table()
            
            for eachtable in eachitem:
                self._build_table_columns(eachtable[0],table_ind, 'MC')
                self.field_ind += 1
            
#             field_indicator = 0
            if self.OE_items_tables_collist:
                for each in self.OE_items_tables_collist[OE_list_indicator]:
                    self._build_table_columns(each[0],OE_list_indicator, 'OE')
                    self.field_ind += 1
             
            self._create_table()
            table_ind += 1
            OE_list_indicator += 1

    def _build_table_columns(self, eachfield = '', table_ind = 0,  itemlist = ''):
        """This function determines the column type for each field to create the FieldSpec object"""
        create_missing_column = False
        field_type = self._define_type(eachfield)
        
        if field_type == 'NVARCHAR':
            if eachfield in self.mc_items_table_field_names:
                ### For Varchar fields, +5 has been added as a buffer to store NOMINAL_ID fields where they are converted to integer,recoded(180 becomes 18.0(4 bytes) and stored back as strings.
                self.table.add(FieldSpec(field_name=self.mc_items_table_field_names[eachfield], basic_type=field_type, data_length=self.layoutdict[eachfield][3] + 5, nullable = False))
            else:
                self.table.add(FieldSpec(field_name=eachfield, basic_type=field_type, data_length=self.layoutdict[eachfield][3] + 5, nullable = False))
        elif field_type == 'FLOAT': 
            if eachfield in self.mc_items_table_field_names:
                self.table.add(FieldSpec(field_name=self.mc_items_table_field_names[eachfield], basic_type=field_type, data_length=self.layoutdict[eachfield][3]))
            else:
                self.table.add(FieldSpec(field_name=eachfield, basic_type=field_type))

        # This check will add a new missing field if '.' recoding values are encountered, if its those fields are tracked and a additional column called pre-fixed with MISSING_ is created
#         for each in self.recodingsdict[eachfield][1]:
#             if each == '.':
#                 create_missing_column = True
#         for variable in self.recodingsdict:
#             for item in self.recodingsdict[variable]:
#                 if self.recodingsdict[variable][item] == '.':
#         value = self.recodingsdict[eachfield].get('.')
#         if value is not None:
#                 create_missing_column = True
        try:
            for v in self.recodingsdict[eachfield].values():
                if v == '.':
                    create_missing_column = True
        except:
            create_missing_column = False
                
        #The self.missing_columns is a dictionary that will contain all the fields and their position in the item_list, this dictionary is later processed and inserted into the item_list
        if create_missing_column:
            if eachfield in self.mc_items_table_field_names:
                fieldname = 'MISSING_' + self.mc_items_table_field_names[eachfield]
                missing_collist = []
                for each in range(len(self.mc_items_tables_collist[table_ind][0])):
                    missing_collist.append(fieldname)
                self.field_ind += 1
                self.missing_columns[fieldname] = [itemlist,table_ind,self.field_ind,missing_collist] 
                self.table.add(FieldSpec(fieldname, basic_type='NVARCHAR', data_length=5, nullable = False))
            else:
                field_name='MISSING_' + eachfield
                self.field_ind += 1
                self.table.add(FieldSpec(field_name, basic_type='NVARCHAR', data_length=5, nullable = False))
        
                
    def _create_table(self):
        """This step will create the flat and itemized tables"""
        self.tablenames.append(self.tablename)
        drop_table_if_exists( self.tablename, self.db)
        self.logger.info('self.tablename={0}'.format(self.tablenames))
        self.logger.info('qry = {0}'.format(self.table.definition))
        self.db.executeNoResults(self.table.definition)

    def _initialize_table(self):
        """This function will do the housekeeping for new table creation
        ie; It will create a tablename, tableschema and preqc_id that will be used as a unique identifier
        """
        self.table = TableSpec(self.db,self.tablename)
        self.table.table_name = self.tablename
        self.table.tableSchema = 'dbo'
        self.table.add(FieldSpec(field_name='ID', basic_type='int'))
        
        if self.item_table:
            self.table.add(FieldSpec(field_name='FLAT_TABLE_ID', basic_type='int'))
        
        self.item_table = False
        
                
    def _define_type(self, eachfield):
        """This function determines the type of the field based on the field definition in the layout file"""
      
        if self.layoutdict[eachfield][2] in  ['NOMINAL_ID1','NOMINAL_ID2','NOMINAL_ID3','STRING','NOMINAL','ID1','ID2','ID3']:
            field_type = 'NVARCHAR'
        else:
            field_type = 'FLOAT'
        return field_type
    
    def _create_input_dict(self):

        """This function reads the input file and creates a dictionary with
                    variable names and values
            Layoutlist structure    - ['variable_name','start','end','type','length'] 
            maxmindict structure    - {'variable_name':[max,min]}
            recodingsdict structure - {'variable_name':[[oringialvalue],[recodingsvalue]]}"""
        
        for each in self.missing_columns:
            if self.missing_columns[each][0] == 'MC':
                tableind = self.missing_columns[each][1]
                fieldind = self.missing_columns[each][2]
                value = self.missing_columns[each][3]
                self.mc_items_tables_collist[tableind].insert(fieldind,value)
            elif self.missing_columns[each][0] == 'OE':
                tableind = self.missing_columns[each][1]
                fieldind = self.missing_columns[each][2]
                value = self.missing_columns[each][3]
                self.OE_items_tables_collist[tableind].insert(fieldind,value)
                
        self.flat_tables_columns = []
        qry="Select name from sys.columns where object_id = OBJECT_ID('{0}') order by column_id".format(self.flat_tablename)
        result = self.db.execute(qry)
        
        for item in result:
            self.flat_tables_columns.append(item[0].encode('ascii').upper())
            
        for each in self.tablenames:
            qry="Select name from sys.columns where object_id = OBJECT_ID('{0}') order by column_id".format(each)
            result = self.db.execute(qry)
            column = []
            for item in result:
                column.append(item[0])
            self.column_names.append(column)
            
        inpfile = open(self.inputfile, 'r')
        error_str = ERROR_FMT%('RECORD-NUMBER','FIELDNAME','START','END','FIELD-VALUE')
        num_of_records_processed = 0
        self.t1 = datetime.datetime.now()
        displaycounter = 1
        self.pattern = re.compile('NOMINAL_ID[11234567890]|[10]')
        self.pattern_id = re.compile('ID[11234567890]|[10]')
        
#         pprint.pprint(self.flat_tables_columns)
        
        for eachline in inpfile:
            
            if num_of_records_processed == 1000:
                print datetime.datetime.now()
                print 'Number of records processed from Input file=', str(num_of_records_processed) +  ' & Increment= ' + str(displaycounter)
                num_of_records_processed = 1
                displaycounter += 1
                self.db.commit()
            num_of_records_processed += 1
            
            self._process_flat_table(eachline)
            
            self._process_OE_items(eachline)

            self.row_count += 1
            
        self.db.commit()

    def _process_flat_table(self,eachline):
        """ This method loops through each and every column of the flat tables and populated the data in the table"""
        
        
        for col in self.flat_tables_columns:
            
            if col in self.layoutdict:
                start, end  = self.layoutdict[col][0], self.layoutdict[col][1]
                value = eachline[start - 1:end]
                
#                 self.logger.info('flat table variable = {0}'.format(col))
#                 self.logger.info('flat table start = {0}'.format(start))
#                 self.logger.info('flat table END = {0}'.format(end))
#                 self.logger.info('flat table value = {0}'.format(value))
#                 self.logger.info('flat table type = {0}'.format(self.layoutdict[col][2]))
                
                if self.debug:
                    print 'flat table variable = {0}'.format(col)
                    print 'flat table start = {0}'.format(start)
                    print 'flat table END = {0}'.format(end)
                    print 'flat table value = {0}'.format(value)
                    print 'flat table type = {0}'.format(self.layoutdict[col][2])
                
                value = self._check_recodings(col,start,end,value)    
                self.insert_rows.append(value)
            else:
                if col == 'ID':
                    self.insert_rows.append(str(self.row_count))
                else:
                    self.insert_rows.append(' ')
        
        self.pre_flat_qc_table_file.write('&!!'.join(self.insert_rows)+'\n')
        
        self.insert_rows = []
    
    def _process_OE_items(self,eachline):
            processing_item_table_count = 0
            cnt = 0
            OE_default_population = True

            for eachpattern in self.mc_items_tables_collist:
# #                 mc_item_table_pk_key = 1
#                 print 'range(len(eachpattern[0]) =', len(eachpattern[0])
                mc_item_table_pk_key = len(eachpattern[0])
                incr = 0
                for eachfield in range(len(eachpattern[0])):
                    for eachelem in range(len(eachpattern)):
                        col = eachpattern[eachelem][eachfield]
                        if col in self.layoutdict:
                            start, end = self.layoutdict[col][0], self.layoutdict[col][1]
                            value = eachline[start - 1:end]
                            if self.debug:
                                print 'Item table variable = {0}'.format(col)
                                print 'Item table start = {0}'.format(start)
                                print 'Item table END = {0}'.format(end)
                                print 'Item table value = {0}'.format(value)
                                print 'Item table type = {0}'.format(self.layoutdict[col][2])
                            value = self._check_recodings(col,start,end,value)
                            self.insert_rows.append(value)
                        else:
                            self.insert_rows.append(' ')
                      
                    #OE position items to be processed
                    
                    for eachOEpattern in range(len(self.OE_items_pos_tables_collist[cnt])):
                        for eachfield in range(len(self.OE_items_pos_tables_collist[cnt][eachOEpattern])):
                            col = self.OE_items_pos_tables_collist[cnt][eachOEpattern][eachfield]
                            start, end = self.layoutdict[col][0], self.layoutdict[col][1] 
                            value = eachline[start - 1:end]
                            
                            try:
                                int(value)
                            except ValueError as e:
                                value = 0
                                
                            if mc_item_table_pk_key == int(value):
                                OE_default_population = False
                                for eachelem in range(len(self.OE_items_tables_collist[cnt])):
                                    col = self.OE_items_tables_collist[cnt][eachelem][eachfield]
                                    if col in self.layoutdict:
                                        start, end = self.layoutdict[col][0],self.layoutdict[col][1]
                                        value = eachline[start - 1:end]
                                        value = self._check_recodings(col,start,end,value)
                                        self.insert_rows.append(value)
                                    else:
                                        self.insert_rows.append(' ')
                       
                    if OE_default_population:
                        for eachelem in range(len(self.OE_items_tables_collist[cnt])):
                            self.insert_rows.append(' ')
                           
                    OE_default_population = True
                    
                    self.insert_rows.insert(0,str(mc_item_table_pk_key))
                    self.insert_rows.insert(1,str(self.row_count))
                    self.file_lists[cnt].write('&!!'.join(self.insert_rows)+'\n')
                    mc_item_table_pk_key -= 1
                    incr += 1
                    self.insert_rows = []  
                cnt += 1
                processing_item_table_count += 1
                     
    def _check_recodings(self, fieldname, start, end, value):
        
        result = None
        if fieldname in self.recodingsdict:
#             if self.layoutdict[fieldname][2] not in  ['STRING','NOMINAL','NOMINAL_ID1','NOMINAL_ID2']:
#                 #This check is added to convert the input value to float before searching the recodings dict..For example: UFXX_TEST_TYPE recodings are stored as floats
#                 if value.isdigit():
#                     value = str(float(value))
            
            result  = self.recodingsdict[fieldname].get(value)

        #This check is added for DOB_YEAR-OGT and similar field conditions, where the correct values were recoded to Null values
        
        if value is not None and result is None:
            pass
        else:
            value = result
            
        if self.layoutdict[fieldname][2] not in  ['STRING','NOMINAL','NOMINAL_ID1','NOMINAL_ID2']:
            #The dict.get(value) return a None value if the key is not present, this check will turn into default value
            if value is None:
                value = None
            
            if value == '.':
                value = ' '
            else:
                try:
                    float(value)
                except ValueError, e:
#                     error_str = ERROR_FMT%(self.row_count,fieldname,start,end,value)
#                      self.error_file.write(error_str + '\n')
                    value = None
        #This check is for Nominal_id[1-10] fields, where the field needs to be converted to float and re-convert to float  Example field:UPSXRAW in OGT
        #The field needs to be recoded for numeric value but stored as string.
                    
        if self.layoutdict[fieldname][2] in  ['NOMINAL_ID1','NOMINAL_ID2','NOMINAL_ID3','ID1','ID2','ID3']:
            if value is None:
                value = ' '
            match = self.pattern.match(self.layoutdict[fieldname][2])
            if match:
                if value.isdigit():
                    len_of_str = len(match.group()[int(match.end()) - 1:int(match.end())])
                    divide_num = '1' + str((len_of_str * '0'))
                    value = float(value) / int(divide_num)
            match_id = self.pattern_id.match(self.layoutdict[fieldname][2])
            if match_id:
                if value.isdigit():
                    len_of_str = len(match_id.group()[int(match_id.end()) - 1:int(match_id.end())])
                    divide_num = '1' + str((len_of_str * '0'))
                    value = float(value) / int(divide_num)
#                     value = '{0:3g}'.format(value)
#                 else:
#                     #There are cases were nominal_id as '-' value, this check counters it
#                     value = '0'
#             else:
#                     #There are cases were nominal_id as '-' value, this check counters it
#                     value = '0'
#                     

#         self.logger.info('recoded value = {0}'.format(value))
        
            
        # We need to convert the None's to empty string, as we can't join lists with None values,, for ex:- a = None, str(a) will make None as string 'None'
        if value is None:
            value = str(' ')
        else:
            value  = str(value)
            
        if self.debug:
            print 'recoded value=', value 
        
        return value
                    
    def _insert_rows(self, joiner):
        """This function will insert one row at a time"""
        
        print 'self.insert_rows', self.insert_rows
        print _INSERT_INTO_TABLE_QUERY.format(self.outputTable,Joiner(joiner))
        
        if self.bulkinsert:
            self.db.executemany(_INSERT_INTO_TABLE_QUERY.format(self.outputTable,Joiner(joiner)),self.bulk_insert_list)
            self.bulk_insert_list = []
        else:
            self.db.executeNoResults(_INSERT_INTO_TABLE_QUERY.format(self.outputTable,Joiner(joiner)),self.insert_rows)
        
        self.insert_rows = []
        
    def _create_indexes(self):
        create_index = "Create Clustered Index {indexname} on  {tablename} ({fieldname})".format(indexname='preqc_ix',tablename=self.flat_tablename,fieldname='id')
        self.db.executeNoResults(create_index)
        
        fields = 'flat_table_id, id'
        for each in self.mc_items_table_names:
            ixname = each + '_ix'
            create_index = "CREATE CLUSTERED INDEX {indexname} on  {tablename} ({fieldname})".format(indexname=ixname.lower(),tablename=each,fieldname=fields)
            self.db.executeNoResults(create_index)

if __name__ == '__main__':
    from airassessmentreporting.testutility import SuiteContext
    RC = SuiteContext('unittest')
#     RC = RunContext('unittest')
    dbcontext = RC.getDBContext()
    print 'dbcontext=', dbcontext
    x = PreQC(runcontext=RC, dbcontext=dbcontext, 
#               layoutfile='C:\CVS Projects\CSSC Score Reporting\OGT Fall 2012\Intake Layout\OGT_FA12_Op_DataLayout_IntakeLayout.xls',
            layoutfile='C:\CVS Projects\CSSC Score Reporting\OGT Fall 2012\Intake Layout\OGT_FA12_Op_DataLayout_IntakeLayout.xls',
#             inputfile='C:\SAS\OGT\Input\original-record1.txt',
            inputfile='H:\\share\\Ohio Graduation Tests\\Technical\\2012 October\\ScoreReports\\TextFileFromDRC\\536215_2012OhioOGTFall_Regular.txt',
#               inputfile='H:/share/Ohio Graduation Tests/Technical/2012 October/ScoreReports/TextFileFromDRC/536215_2012OhioOGTFall_Regular.txt',
#                 inputfile='H:\\share\\Ohio Graduation Tests\\Technical\\2012 July\\ScoreReports\\TextFileFromDRC\\536214_2012OhioOGTSummer_Regular.txt',
#                 inputfile='C:\SAS\OGT\Input\input-1.txt',    
#             inputfile='C:\SAS\OGT\Input\original-part1.txt',
#             inputfile='C:\SAS\OGT\Input\original.txt',
#                 inputfile='C:\SAS\OGT\Input\original-record1.txt',
    
              patterns=[ ('[u][pcf][w]x_.*{icnt}.*', '[u][pcf][w]x__OE.*Pos.*{icnt}','[u][pcf][w]x_OE_.*{icnt}','MC_TABLE_W','W')
                        ,('[u][pcf][c]x_.*{icnt}.*','[u][pcf][c]x__OE.*Pos.*{icnt}','[u][pcf][c]x_OE_.*{icnt}','MC_TABLE_C','C')
                        ,('[u][pcf][s]x_.*{icnt}.*','[u][pcf][s]x__OE.*Pos.*{icnt}','[u][pcf][s]x_OE_.*{icnt}','MC_TABLE_S','S')
                        ,('[u][pcf][r]x_.*{icnt}.*','[u][pcf][r]x__OE.*Pos.*{icnt}','[u][pcf][r]x_OE_.*{icnt}','MC_TABLE_R','R')
                        ,('[u][pcf][m]x_.*{icnt}.*','[u][pcf][m]x__OE.*Pos.*{icnt}','[u][pcf][m]x_OE_.*{icnt}','MC_TABLE_M','M')], 
              debug=False,flat_tablename='PRE_QC_FLAT_TABLE_2',bulk_insert=False, errorfile='c:\SAS\OGT\Error.txt', outputsdir = 'c:/SAS/OGT/Joboutput/')
              
    x.process()

#MC_TABLE_W_1- contains all the rows
#     import cProfile, pstats
#     pr = cProfile.Profile()
#     pr.enable()
#     pr.runcall( x.process )
#     pr.disable()
#     
#     filename = os.path.join( RC.logs_dir, 'preqc_profile.txt' )
#     with open( filename, 'w' ) as f:
#         stats = pstats.Stats( pr, stream=f )
#         stats.print_stats()


#     import cProfile, pstats
#     pr = cProfile.Profile()
#     pr.enable()
#     pr.runcall( x.process )
#     pr.disable()
#     
#     filename = os.path.join( RC.logs_dir, 'preqc_profile.txt' )
#     with open( filename, 'w' ) as f:
#         stats = pstats.Stats( pr, stream=f )
#         stats.print_stats()
