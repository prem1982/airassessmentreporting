'''
Created on May 14, 2013

@author: temp_plakshmanan
'''
from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote
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
                        errorfile='' , outputsdir = 'c:/SAS/OGT/Joboutput/'):
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
        self.RC = runcontext
        self.db = dbcontext
        self.layoutfile = layoutfile
        self.inputfile = inputfile
        self.errorfile = errorfile
        self.outputrec = {}
        self.layoutdict = []
        self.maxmindict = []
        self.recodingsdict = []
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
        self.missing_columns = {}
        self.tablenames = []
        self.column_names = []
        self.flat_tablename = flat_tablename
        self.bulkinsert = bulk_insert
        self.bulk_insert_list = []
        self.row_count = 1
        self.t1 = datetime.datetime.now()
        self.t2 = datetime.datetime.now()
        if not os.path.exists(outputsdir):
            os.makedirs(outputsdir)
        self.f = open('c:/OGTValidation.txt', "w")
        self.error_file = open(self.errorfile,'w')
        self.insert_rows = []
        self.file_lists = []
        self.outputs_dir = outputsdir
        self.pre_flat_qc_table_file = open(self.outputs_dir + self.flat_tablename + '.txt','w')
        self.batchfile = open(self.outputs_dir + 'bcpfile.bat','w')
        self.statsfile = open(self.outputs_dir + 'loadstats.txt','w')
        open(self.outputs_dir + 'error.txt')
        batch_file_string = 'bcp ' + dbcontext.db + '.dbo.' +  self.flat_tablename +  ' in ' + self.outputs_dir +  self.flat_tablename + '.txt' \
                + ' -t "&&&" -c -S ' + dbcontext.server + ' -T  -e ' + self.outputs_dir + 'error.txt'
        self.batchfile.write(batch_file_string + '\n')
        
        

    def process(self):
        """ This is the main process that controls the flow of the main module.
        This module normalizes the data for each itemized pattern and creates one table for each of them.
         
        This module creates the following lists
        1. Lists of list - [self.mc_items_tables_collist] - for all itemized columns for each pattern.
        2. Lists of list - [self.OE_items_pos_tables_collist] - for all OE position columns for each pattern.
        3. Lists of list - [self.OE_items_tables_collist] for all OE count columns for each pattern.
        4. Unique list - [self.item_tables_columns] - to identify all columns that will be used to create FLAT table.
        """
        self.RC.debug('Main process of pre_qc started')
        LC = layoutcheck.LayoutCheck(runcontext=self.RC, dbcontext=self.db,
                                                layoutfile=self.layoutfile)

        self.layoutdict, self.maxmindict, self.recodingsdict = LC.process()
        
        if self.layoutdict:
            for k,v in self.layoutdict.items():
                self.all_columns.append(k)
            print '_get_mc_table_names()'
            self._get_mc_table_names()
            print '_get_mc_itemized_columns() start'
            self._get_mc_itemized_columns()
            print '_get_OE_itemized_columns() start'
            self._get_OE_itemized_columns()
            print '_get_flat_columns_in_table() start'
            self._get_flat_columns_in_table()
            print '_prepare_flat_table_create_stmt() start'
            self._prepare_flat_table_create_stmt()
            print '_prepare_mc_item_table_create_stmt() start'
            self._prepare_mc_item_table_create_stmt()
            print 'Process start'
            del self.tablenames[0]
            self._create_input_files()
            if len(self.all_columns) == len(self.flat_tables_columns) + len(self.item_tables_columns):
                self._create_input_dict()
            
            self.pre_flat_qc_table_file.close()
            self.batchfile.close()
            for each in self.file_lists:
                each.close()
                
            proc = subprocess.Popen(self.outputs_dir + 'bcpfile.bat',stdout=subprocess.PIPE)
 
            for each in iter(proc.stdout.readline,''):
                self.statsfile.write(each + '\n')

            filename = self.outputs_dir + 'error.txt'
            if os.stat(self.outputs_dir + 'error.txt').st_size <> 0:
                print "LOADING ERRORS, CHECK THE ERROR REPORT"
                
            self._create_indexes()             
            print 'Total Number of columns=',len(self.all_columns)
            print 'Total Number of columns in Flat Table=',len(self.flat_tables_columns)
            print 'Total Number of columns in MC Table=',len(self.item_tables_columns)
            print 'Total number of Input records processed=', self.row_count - 1
            
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
            batch_file_string = 'bcp ' + dbcontext.db + '.dbo.' +  each[3] +  ' in ' + self.outputs_dir +  each[3] + '.txt' \
                + ' -t "&&&" -c -S ' + dbcontext.server + ' -T '
            self.batchfile.write(batch_file_string + '\n')
            
    def _prepare_flat_table_create_stmt(self):
        """This method calls the build_table_columns method to create new columns for the flat table"""
        self.tablename = self.flat_tablename
        self._initialize_table()
        
        for eachfield in self.flat_tables_columns:
            self._build_table_columns(eachfield.upper(), tableind=1, fieldind=1)
        self._create_table()
    
    def _prepare_mc_item_table_create_stmt(self):
        """This method calls the build_table_columns method to create new columns for the itemized table"""
        table_indicator = 0
        OE_list_indicator = 0
        for eachitem in self.mc_items_tables_collist:
            self.tablename = self.mc_items_table_names[table_indicator]
            self.item_table = True
            self._initialize_table()
            field_indicator = 0
            for eachtable in eachitem:
                self._build_table_columns(eachtable[0],table_indicator, field_indicator, 'MC')
                field_indicator += 1
            
            field_indicator = 0
            if self.OE_items_tables_collist:
                for each in self.OE_items_tables_collist[OE_list_indicator]:
                    self._build_table_columns(each[0],OE_list_indicator, field_indicator, 'OE')
                    field_indicator += 1
             
            self._create_table()
            table_indicator += 1
            OE_list_indicator += 1

    def _build_table_columns(self, eachfield = '', tableind = 0, fieldind = 0, itemlist = ''):
        """This function determines the column type for each field to create the FieldSpec object"""
        
        create_missing_column = False
        field_type = self._define_type(eachfield)
        
        if field_type == 'VARCHAR':
            if eachfield in self.mc_items_table_field_names:
                ### For Varchar fields, +5 has been added as a buffer to store NOMINAL_ID fields where they are converted to integer,recoded(180 becomes 18.0(4 bytes) and stored back as strings.
                self.table.add(FieldSpec(field_name=self.mc_items_table_field_names[eachfield], basic_type=field_type, data_length=self.layoutdict[eachfield][3] + 5))
            else:
                self.table.add(FieldSpec(field_name=eachfield, basic_type=field_type, data_length=self.layoutdict[eachfield][3] + 5))
        elif field_type == 'FLOAT': 
            if eachfield in self.mc_items_table_field_names:
                self.table.add(FieldSpec(field_name=self.mc_items_table_field_names[eachfield], basic_type=field_type, data_length=self.layoutdict[eachfield][3]))
            else:
                self.table.add(FieldSpec(field_name=eachfield, basic_type=field_type))

        # This check will add a new missing field if '.' recoding values are encountered, if its those fields are tracked and a additional column called pre-fixed with MISSING_ is created
        for each in self.recodingsdict[eachfield][1]:
            if each == '.':
                create_missing_column = True
                
        #The self.missing_columns is a dictionary that will contain all the fields and their position in the item_list, this dictionary is later processed and inserted into the item_list
        if create_missing_column:
            if eachfield in self.mc_items_table_field_names:
                fieldname = 'MISSING_' + self.mc_items_table_field_names[eachfield]
                missing_collist = []
                for each in range(len(self.mc_items_tables_collist[tableind][0])):
                    missing_collist.append(fieldname)
                self.missing_columns[fieldname] = [itemlist,tableind,fieldind,missing_collist] 
                self.table.add(FieldSpec(fieldname, basic_type='VARCHAR', data_length=5))
            else:
                field_name='MISSING_' + eachfield
                self.table.add(FieldSpec(field_name, basic_type='VARCHAR', data_length=5))
        
                
    def _create_table(self):
        """This step will create the flat and itemized tables"""
        self.tablenames.append(self.tablename)
        drop_table_if_exists( self.tablename, self.db)
        if self.debug:
            print 'self.tablename=', self.tablenames
            print 'qry=',self.table.definition
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
      
        if self.layoutdict[eachfield][2] in  ['NOMINAL_ID1','NOMINAL_ID2','NOMINAL_ID3','STRING','NOMINAL']:
            field_type = 'VARCHAR'
        else:
            field_type = 'FLOAT'
        return field_type
    
    def _create_input_dict(self):

        """This function reads the input file and creates a dictionary with
                    variable names and values
            Layoutlist structure    - ['variable_name','start','end','type','length'] 
            maxmindict structure    - {'variable_name':[max,min]}
            recodingsdict structure - {'variable_name':[[oringialvalue],[recodingsvalue]]}"""

        self.table_columns = []
        self.tables = []
        self.table_row = []                        
        for eachpattern in self.mc_items_tables_collist:
            for j in range(len(eachpattern[0])):
                for k in (range(len(eachpattern))):
                    self.table_row.append(eachpattern[k][j])
#                     print eachpattern[k][j]
                self.table_columns.append(self.table_row)
                self.table_row = []
            self.tables.append(self.table_columns)
            self.table_columns = []
        
#         pprint.pprint(self.tables[0])
#         print 'Table-2'
#         pprint.pprint(self.tables[1])
        
        for each in self.missing_columns:
            if self.missing_columns[each][0] == 'MC':
                tableind = self.missing_columns[each][1]
                fieldind = self.missing_columns[each][2]
                value = self.missing_columns[each][3]
                self.mc_items_tables_collist[tableind].insert(fieldind+1,value)
            elif self.missing_columns[each][0] == 'OE':
                tableind = self.missing_columns[each][1]
                fieldind = self.missing_columns[each][2]
                value = self.missing_columns[each][3]
                self.OE_items_tables_collist[tableind].insert(fieldind+1,value)
        
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
        pprint.pprint(self.tables[0])
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
        for eachfield in self.flat_tables_columns:
            
            if eachfield in self.layoutdict:
                start, end  = self.layoutdict[eachfield][0], self.layoutdict[eachfield][1]
                value = eachline[start - 1:end]
                if self.debug:
                    print 'flat table variable=',eachfield
                    print 'flat table start=', start
                    print 'flat table END=', end
                    print 'flat table value=', value
                    print 'flat table type=', self.layoutdict[eachfield][2]
                
                value = self._check_recodings(eachfield,start,end,value)    

                if self.debug:
                    print 'flat table recoded value=', value
                    
                self.insert_rows.append(str(value))
            else:
                if eachfield == 'ID':
                    self.insert_rows.append(str(self.row_count))
                else:
                    self.insert_rows.append('')
        
        self.pre_flat_qc_table_file.write('&&&'.join(self.insert_rows)+'\n')
        
        self.insert_rows = []
    
    def _process_OE_items(self,eachline):
        processing_item_table_count = 0
        cnt = 0
        
        for table in self.tables:
            sub_table = table
            for row in sub_table:
                mc_item_table_pk_key = 1
                for eachcolumn in row:
                    if eachcolumn in self.layoutdict:
                        start, end = self.layoutdict[eachcolumn][0], self.layoutdict[eachcolumn][1]
                        value = eachline[start - 1:end]
                        value = self._check_recodings(eachcolumn,start,end,value)
                        self.insert_rows.append(str(value))
                    else:
                        self.insert_rows.append('')
#                 for eachOEpattern in range(len(self.OE_items_pos_tables_collist[cnt])):
#                     for eachfield in range(len(self.OE_items_pos_tables_collist[cnt][eachOEpattern])):
#                         fieldname = self.OE_items_pos_tables_collist[cnt][eachOEpattern][eachfield]
#                         start, end = self.layoutdict[fieldname][0], self.layoutdict[fieldname][1] 
#                         value = eachline[start - 1:end]
#                         if self.debug:
#                             print 'start=', start
#                             print 'end=', end
#                             print 'value=', value
#                         try:
#                             int(value)
#                         except ValueError as e:
#                             value = 0
#                                 
#                         if mc_item_table_pk_key == int(value):
#                             OE_default_population = False
#                             for eachelem in range(len(self.OE_items_tables_collist[cnt])):
#                                 fieldname = self.OE_items_tables_collist[cnt][eachelem][eachfield]
#                                 if fieldname in self.layoutdict:
#                                     start, end = self.layoutdict[fieldname][0],self.layoutdict[fieldname][1]
#                                     value = str(eachline[start - 1:end])
#                                     value = self._check_recodings(fieldname,start,end,value)
#                                     self.insert_rows.append(str(value))
#                                 else:
#                                     self.insert_rows.append('')
#                        
#                 if OE_default_population:
#                     for eachelem in range(len(self.OE_items_tables_collist[cnt])):
#                         self.insert_rows.append('')
#                            
#                 OE_default_population = True

                self.insert_rows.insert(0,str(mc_item_table_pk_key))
                self.insert_rows.insert(1,str(self.row_count))
                     
                mc_item_table_pk_key += 1
                self.file_lists[cnt].write('&&&'.join(self.insert_rows)+'\n')
                self.insert_rows = []
            cnt += 1
            processing_item_table_count += 1     

    def _check_recodings(self, fieldname, start, end, value):
        if fieldname in self.recodingsdict:                    
            for count, each in zip(range(len(self.recodingsdict[fieldname][0])),self.recodingsdict[fieldname][0]):
                if each == value:
                    value = self.recodingsdict[fieldname][1][count]
                    break
        
        if self.layoutdict[fieldname][2] not in  ['STRING','NOMINAL','NOMINAL_ID1','NOMINAL_ID2']:
            if value == '.':
                value = ''
            else:
                try:
                    float(value)
                except ValueError, e:
                    error_str = ERROR_FMT%(self.row_count,fieldname,start,end,value)
                    self.error_file.write(error_str + '\n')
                    value = 0
        #This check is for Nominal_id[1-10] fields, where the field needs to be converted to float and re-convert to float  Example field:UPSXRAW in OGT
        #The field needs to be recoded for numeric value but stored as string.
                
        if self.layoutdict[fieldname][2] in  ['NOMINAL_ID1','NOMINAL_ID2']:
            match = self.pattern.match(self.layoutdict[fieldname][2])
            if match:
                if value.isdigit():
                    len_of_str = len(match.group()[int(match.end()) - 1:int(match.end())])
                    divide_num = '1' + str((len_of_str * '0'))
                    value = float(value) / int(divide_num)
                    value = str(value)
                    
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
        self.t2 = datetime.datetime.now()
        print 'Time_taken=',self.t2 - self.t1
        create_index = "Create Clustered Index {indexname} on  {tablename} ({fieldname})".format(indexname='preqc_ix',tablename=self.flat_tablename,fieldname='id')
        self.db.executeNoResults(create_index)
        
        fields = 'id,flat_table_id'
        for each in self.mc_items_table_names:
            ixname = each + '_ix'
            create_index = "CREATE CLUSTERED INDEX {indexname} on  {tablename} ({fieldname})".format(indexname=ixname.lower(),tablename=each,fieldname=fields)
            self.db.executeNoResults(create_index)

if __name__ == '__main__':
    RC = RunContext('unittest')
    dbcontext = RC.getDBContext()
    print 'dbcontext=', dbcontext
    x = PreQC(runcontext=RC, dbcontext=dbcontext, layoutfile='C:\CVS Projects\CSSC Score Reporting\OGT Spring 2012\Input Layout\OGT_SP12_Op_DataLayout_IntakeLayout.xls',
                inputfile='C:\SAS\OGT\Input\input-1.txt',
#             inputfile='C:\SAS\OGT\Input\original-part1.txt',
              patterns=[ ('[u][pcf][w]x_.*{icnt}.*', '[u][pcf][w]x__OE.*Pos.*{icnt}','[u][pcf][w]x_OE_.*{icnt}','MC_TABLE_W')
                        ,('[u][pcf][c]x_.*{icnt}.*','[u][pcf][c]x__OE.*Pos.*{icnt}','[u][pcf][c]x_OE_.*{icnt}','MC_TABLE_C')
                        ,('[u][pcf][s]x_.*{icnt}.*','[u][pcf][s]x__OE.*Pos.*{icnt}','[u][pcf][s]x_OE_.*{icnt}','MC_TABLE_S')
                        ,('[u][pcf][r]x_.*{icnt}.*','[u][pcf][r]x__OE.*Pos.*{icnt}','[u][pcf][r]x_OE_.*{icnt}','MC_TABLE_R')
                        ,('[u][pcf][m]x_.*{icnt}.*','[u][pcf][m]x__OE.*Pos.*{icnt}','[u][pcf][m]x_OE_.*{icnt}','MC_TABLE_M')], 
              debug=False,flat_tablename='PRE_QC_FLAT_TABLE1',bulk_insert=False, errorfile='c:\SAS\OGT\Error.txt')
              
    x.process()

