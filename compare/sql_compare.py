'''
Created on Apr 4, 2013

@author: temp_plakshmanan
'''
import datetime
from pprint import pprint

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

from airassessmentreporting.airutility import RunContext
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec

class SqlCompare(object):
    def __init__(self,run_context,dbcontext,TableNm1='',TableNm2='',Keyfields=[],excludefields=[],db_tag=None,num_of_errors=50,tolerance=0,debug=False,output_file='Compare Report.pdf'):
        """This is the initialization para and this job needs the following inputs -
        1. Runcontext
        2. TableName_1 that will be compared
        3. TableName_2 that will be compared
        4. The Keyfields based on which the comparison will be performed
        5. The list of fields that will be excluded in the compare
        6. Database schema
        7. Number of errors that will be written to the output
        8. TOLERANCE Value
        9. debug indicator""" 
        
        self.runContext = run_context
        self.logger = self.runContext.get_logger( "sqlcompare" )
        self.TableNm1 = TableNm1
        self.TableNm2 = TableNm2
        self.Keyfields = Keyfields
        self.excludefields = excludefields
        self.dbTag = db_tag
        self.conn = dbcontext
        self.keys = ''
        self.qry = ''
        self.where_list = ''
        self.where_list_1 = ''
        self.tolerance = tolerance
        self.error_dict = {}
        self.error_count = {}
        self.error_lists = []
        self.error_count_lists = []
        self.key_values_list = Keyfields
        self.key_values_list = [ each.upper() for each in self.key_values_list]
        self.num_of_errors = num_of_errors
        self.num_of_rows_in_table1 = 0
        self.num_of_rows_in_table2 = 0
        self.num_of_non_matching_rows = 0 
#         self.num_of_unmatched_rows_in_table2 = 0
        self.num_of_new_record_table1 = 0
        self.num_of_new_record_table2 = 0
        self.num_of_mismatch_matched_keys = 0
        self.t1 = datetime.datetime.now()
        self.c = canvas.Canvas(output_file,pagesize=letter)
        self.startval = 750
        self.linediff = 12
        self.debug = debug
        if not self.Keyfields:
            raise "error"
    
    def process(self):
            """This is the main process that will be called to perform the compare between 2 tables"""
        #query="Select name from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        #This query will get all the matching columns
#         try:
            table_exist = table_exists( self.TableNm1, db_context = self.conn)
            
            if not table_exist:
                raise Exception("Table-1 not available, check if the database is correct")
            
            table_exist = table_exists( self.TableNm2, db_context = self.conn)
            
            if not table_exist:
                raise ("Table-2 not available, check if the database is correct")
            
            query = """SELECT A.NAME FROM SYS.COLUMNS A,SYS.COLUMNS B WHERE A.NAME = B.NAME AND a.object_id = OBJECT_ID(?) AND b.object_id = OBJECT_ID(?)"""
            self.logger.info( "Query = {0}".format(query))
            self.table_Schema = self.conn.execute(query, (self.TableNm1,self.TableNm2))
            self.table_Schema = [str(each[0]).upper() for each in self.table_Schema]
            self.table_matchfields = [each for each in self.table_Schema if each not in self.Keyfields]
            self.table_matchfields = [each for each in self.table_matchfields if each not in self.excludefields]
            self.table_qry_schema = [x.upper() for x in self.table_Schema]
            
            #This query will get all the columns of table-1 and their types
            query="Select name,TYPE_NAME(system_type_id),max_length from sys.columns where object_id = OBJECT_ID(?) order by column_id"
            self.table1_Schema      = self.conn.execute(query,self.TableNm1 )
            self.table1_Schema_type = dict( ('TB1'+str(each[0]).upper() ,[str(each[1]).upper(), each[2] ] ) for each in self.table1_Schema)
            self.table1_Schema      = [str(each[0]).upper() for each in self.table1_Schema]
            
            #This query will get all the columns of table-2 and their types
            query="Select name,TYPE_NAME(system_type_id),max_length from sys.columns where object_id = OBJECT_ID(?) order by column_id"
            self.table2_Schema      = self.conn.execute(query,self.TableNm2 )
            self.table2_Schema_type = dict( ('TB2'+str(each[0]).upper() ,[str(each[1]).upper(), each[2] ] ) for each in self.table2_Schema)
            self.table2_Schema      = [str(each[0]).upper() for each in self.table2_Schema]
            
            self.logger.info( "Table_schema = {0}".format(self.table_Schema))
            self.logger.info( "Table1_schema = {0}".format(self.table1_Schema))
            self.logger.info( "Table1_schema_type = {0}".format(self.table1_Schema_type))
            self.logger.info( "Table2_schema_type = {0}".format(self.table2_Schema_type))
#  
            self.table1_not_in_table2_fields = ['TB1'+each for each in self.table1_Schema if each not in self.table_Schema]
            self.table2_not_in_table1_fields = ['TB2'+each for each in self.table2_Schema if each not in self.table_Schema]
            
            self.logger.info('self.table1_not_in_table2_fields = {0}'.format(self.table1_not_in_table2_fields))
            self.logger.info('self.table2_not_in_table1_fields = {0}'.format(self.table2_not_in_table1_fields))
            self.logger.info('self.table_matchfields = {0}'.format(self.table_matchfields))
            self.logger.info('self.table_Schema = {0}'.format(self.table_Schema))
#     
            self._prepareqry()
            cursor = self.conn.createcur()
            print self.qry
#             self.logger.error('self.qry = {0}'.format(self.qry))
            cursor.execute(self.qry)

            for row in self.conn.execQuery(cursor):
#                 self.table_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_qry_schema,list(row)))
                self.table_row_dict = row
#                 self.log.error('self.table_row_dict = {0}'.format(self.table_row_dict))
                #Check to see if the rows in table-1 are null
                
#                 table1_null = any([item for item in self.key_values_list if getattr(self.table_row_dict,'TB1'+item) is None])
                table1_null = all( getattr(self.table_row_dict, 'TB1'+item) is None for item in self.key_values_list)
                #Check to see if the rows in table-1 are null
#                 table2_null = any([item for item in self.key_values_list if getattr(self.table_row_dict,'TB2'+item) is None])
                table2_null = all( getattr(self.table_row_dict, 'TB2'+item) is None for item in self.key_values_list)
                #Gets the actual Key values
                
                self.key_values = [str(getattr(self.table_row_dict,'TB1'+item)) for item in self.key_values_list]
                
#                 self.log.error('self.key_values'.format(self.key_values))
                if table1_null:
                        self.num_of_new_record_table2 += 1
                        self.logger.info( 'table1_null')
                        self.logger.info( 'self.num_of_new_record_table2='.format(self.num_of_new_record_table2))
                        
                if table2_null:
                        self.num_of_new_record_table1 += 1
                        self.logger.info('table2_null')
                        self.logger.info('self.num_of_new_record_table1 = {0}'.format(self.num_of_new_record_table1))
                
                
                self.logger.info('table1_null = {0}'.format(table1_null))
                self.logger.info('table2_null = {0}'.format(table2_null))
                
                if table1_null is False and table2_null is False:
                    
                    self.num_of_non_matching_rows += 1
#                     self.num_of_unmatched_rows_in_table1    += 1
#                     self.num_of_unmatched_rows_in_table2    += 1
                    self.num_of_mismatch_matched_keys += 1
                    if self.debug:
                        print 'self.num_of_rows_in_table1 = {0}',self.num_of_rows_in_table1
                        print 'self.num_of_rows_in_table2 = {0}',self.num_of_rows_in_table2
                        print 'self.num_of_mismatch_matched_keys = {0}', self.num_of_mismatch_matched_keys
                
                    for eachcol in self.table_qry_schema:
                            tab2col = 'TB2' + eachcol
                            eachcol = 'TB1' + eachcol
                             
                            self.logger.info('eachcol = {0}'.format(eachcol))
                            self.logger.info('tab2col = {0}'.format(tab2col))
                            self.logger.info('getattr(self.table_row_dict,eachcol) = {0}'.format(getattr(self.table_row_dict,eachcol)))
                            self.logger.info('getattr(self.table_row_dict,tab2col) = {0}'.format(getattr(self.table_row_dict,tab2col)))
                            self.logger.info('type_table_row_dict[eachcol] = {0}'.format(type(getattr(self.table_row_dict,eachcol))))
                            self.logger.info('type_table_row_dict[tab2col] = {0}'.format(type(getattr(self.table_row_dict,tab2col))))
                            self.logger.info('self.table1_Schema_type[eachcol][0] = {0}'.format(self.table1_Schema_type[eachcol][0]))
                            self.logger.info('self.table2_Schema_type[eachcol][0] = {0}'.format(self.table2_Schema_type[tab2col][0]))
                            
                            if self.table1_Schema_type[eachcol][0] == self.table2_Schema_type[tab2col][0]: # Checks for the type of the field
                                #There are cases where the blank cases needs to be stripped before comparison,
                                value_1 = getattr(self.table_row_dict,eachcol)
                                value_2 = getattr(self.table_row_dict,tab2col)
                                
                                if isinstance(value_1,str) and isinstance(value_2,str):
                                    value_1 = value_1.strip()
                                    value_2 = value_2.strip()
                                if value_1 == value_2:
                                        pass
                                else:
                                    compare, numeric = self._check_types(eachcol,tab2col)
                                         
                                    if numeric:
                                        if getattr(self.table_row_dict,eachcol) == None or getattr(self.table_row_dict,tab2col) == None:
                                            self._write_error_lists(eachcol,tab2col)
                                        else:
                                            result = abs(getattr(self.table_row_dict,eachcol) - getattr(self.table_row_dict,tab2col))  / max([abs(getattr(self.table_row_dict,eachcol)),abs(getattr(self.table_row_dict,tab2col))])    
                                            if result >= self.tolerance:
                                                self._write_error_lists(eachcol,tab2col)
                                    else:
                                        self._write_error_lists(eachcol,tab2col)
                            else:
                                compare, numeric = self._check_types(eachcol,tab2col)
                                if compare:
                                    if getattr(self.table_row_dict,eachcol) == getattr(self.table_row_dict,tab2col):
                                        pass
                                    else:
                                        if numeric:
                                            if getattr(self.table_row_dict,eachcol) == None or getattr(self.table_row_dict,tab2col) == None:
                                                self._write_error_lists(eachcol,tab2col)
                                            else:
                                                result = abs(getattr(self.table_row_dict,eachcol) - getattr(self.table_row_dict,tab2col))  / max([abs(getattr(self.table_row_dict,eachcol)),abs(getattr(self.table_row_dict,tab2col))])    
                                                if result >= self.tolerance:
                                                    self._write_error_lists(eachcol,tab2col)
                                        else:
                                            self._write_error_lists(eachcol,tab2col)
                
            cursor.close()
            print 80 * '-'
            print "Compare Report"
            print 80 * '-'
            print 'self.error_count', self.error_count
            print 'self.error_count_lists',self.error_count_lists
            print 'self.error_dict',self.error_dict
            print 'self.error_lists',self.error_lists
            self._printreport()
#         except Exception as e:
#             print 80*"-"
#             print "Error Report"
#             print 80*"-"
#             print "Error=",e
#             print 'self.table_row_dict=',self.table_row_dict
#             print 'eachcol=', eachcol
#             print 'tab2col=', tab2col
#             print 'Table1_Value:-self.table_row_dict=', getattr(self.table_row_dict,eachcol)
#             print 'Table2_Value:-self.table_row_dict=', getattr(self.table_row_dict,tab2col)
#             print 'self.table1_Schema_type[eachcol]=',self.table1_Schema_type[eachcol]
#             print 'self.table2_Schema_type[tab2col]=',self.table2_Schema_type[tab2col]
        
    def _check_types(self,eachcol,tab2col):
        """This function will check the types of the field from both the tables and perform the compare accordingly"""
        compare = False
        numeric = False

        if ((self.table1_Schema_type[eachcol][0] == 'VARCHAR' and self.table2_Schema_type[tab2col][0] == 'CHAR') or         \
            (self.table1_Schema_type[eachcol][0] == 'CHAR' and self.table2_Schema_type[tab2col][0] == 'VARCHAR')): 
            numeric = False
            compare = True        
        elif ((self.table1_Schema_type[eachcol][0] == 'DECIMAL' and self.table2_Schema_type[tab2col][0] == 'FLOAT') or       \
                (self.table1_Schema_type[eachcol][0] == 'FLOAT' and self.table2_Schema_type[tab2col][0] == 'DECIMAL') or
                (self.table1_Schema_type[eachcol][0] == 'DECIMAL' and self.table2_Schema_type[tab2col][0] == 'DECIMAL') or
                (self.table1_Schema_type[eachcol][0] == 'FLOAT' and self.table2_Schema_type[tab2col][0] == 'FLOAT')):
                numeric = True
                compare = True
        return compare, numeric
    
    def _write_error_lists(self,eachcol,tab2col):
        """This function will create a error_lists that will maintain a counter of all the errors"""
           
        key_present = False
        key_not_present = False
        if len(self.error_count_lists) == 0:
            self.error_count[eachcol] = 1
            self.error_count_lists.append(self.error_count)
        else:
            for each in self.error_count_lists:
                if eachcol in each:
                    key_present = True
                    break
                else:
                    key_not_present = True
        if self.debug:
            print 'key_present=',     key_present
            print 'key_not_present=', key_not_present 
        
        if key_present == True:self.error_count[eachcol] = self.error_count[eachcol] + 1
        
        if key_not_present == True:
            self.error_count[eachcol] = 1

#         if self.error_count[eachcol] < self.num_of_errors:
        self._write_error_dict(eachcol,tab2col)
        
    def _write_error_dict(self,eachcol,tab2col):
        """This function will create a dictionary that will write the errors to the dictionary"""
        self.logger.info( '_write_error_dict')
        self.logger.info('eachcol='.format(eachcol))
        self.logger.info('tab2col='.format(tab2col))
        self.logger.info( 'self.key_values='.format(self.key_values))
        
        self.error_dict['KEY']  = self.key_values
        self.error_dict['FIELD']   = eachcol
        self.error_dict['VALUE-1'] = getattr(self.table_row_dict,eachcol)
        self.error_dict['VALUE-2'] = getattr(self.table_row_dict,tab2col)
        self.error_lists.append(self.error_dict)
        self.error_dict = {}
    
    def _printreport(self):
        """This function will create a pdf compare report"""
        query= """Select count(*) as c1 from {table_name}""".format(table_name=self.TableNm1)
        self.tot_num_rows_in_table1 = self.conn.execute(query)
        print 'self.tot_num_rows_in_table1=', self.tot_num_rows_in_table1 [0][0]
        query= """Select count(*) as c1 from {table_name}""".format(table_name=self.TableNm2)
        self.tot_num_rows_in_table2 = self.conn.execute(query)
        print 'self.tot_num_rows_in_table2=', self.tot_num_rows_in_table2 [0][0]
                                        
        print '-'*80 + 'Error Report' + '-'*80 
        print "error_count=",self.error_count_lists
        self.TableNm1 = self.TableNm1.upper()
        self.TableNm2 = self.TableNm2.upper()
        for x in self.error_count_lists:
            pprint(x)
        
        num_of_error_keys =[]
        from operator import itemgetter
        error_lists = sorted(self.error_lists,key=itemgetter('FIELD'))
        
        for x in error_lists:
            num_of_error_keys.append(x['KEY'][0])
        # This print the total number of unmatched rows--self.num_of_unmatched_rows_in_table1 & self.num_of_unmatched_rows_in_table2 are same
        print 'Number of rows not matched  =', self.num_of_non_matching_rows
        # This prints the total number of rows in Table1
        print 'Number of Observations in TABLE1=',self.tot_num_rows_in_table1[0][0]
        # This print the total number of unmatched rows  
        print 'Number of Observations in TABLE2=',self.tot_num_rows_in_table2[0][0]
        #         print 'self.num_of_unmatched_rows_in_table1=', self.num_of_unmatched_rows_in_table1
        # Fetch will give the number of unmatched rows, so observation matched is num_of_rows_table1 - num_of_non_matching_rows (we check only for table-1 and THAT IS ENOUGH)
        print "Number of Observations MATCHED=", self.tot_num_rows_in_table1[0][0] - self.num_of_non_matching_rows
        print 'length num_of_error_keys=',len(num_of_error_keys)
       
        print "Number of Observations with compared variables unequal=", len(set(num_of_error_keys)) #Number of unique keys in the error lists
#         print "Number of Observations with compared variables EQUAL=",  self.num_of_non_matching_rows - len(set(num_of_error_keys)) #Total number of NON matched records - Total number of unique keys in error table
        print "Number of new Observations in TABLE-1=",self.num_of_new_record_table1
        print "Number of new Observations in TABLE-2=",self.num_of_new_record_table2
        print "Number of variables compared=",len(self.table_matchfields)
        if len(self.error_count_lists):
            print "Number of Variables Compared with All Observations Equal=",len(self.table_matchfields) - len(self.error_count_lists[0].keys())
            print "Number of Variables Compared with Some Observations Unequal=",len(self.error_count_lists[0].keys())
        print "Total number of values which compare unequal=",len(self.error_lists)
        print "Number of variables in Table-1 and not in Table-2=",self.table1_not_in_table2_fields
        print "Number of variables in Table-2 and not in Table-1=",self.table2_not_in_table1_fields
        schema_types = []
        for x in self.table1_Schema_type:
            schema_types.append(self.table1_Schema_type[x][0])
        
        for x in self.table2_Schema_type:
            schema_types.append(self.table2_Schema_type[x][0])
        
        print "Types of variables compared=", set(schema_types)    
        
        self.t2 = datetime.datetime.now()
        print 'Time taken=',self.t2-self.t1
        print '-'*80
        
        line7  = "Number of Observations NOT MATCHED = {0}".format(str(self.num_of_non_matching_rows))
        line8  = "Number of Observations with compared variables unequal = {0}".format(str(len(set(num_of_error_keys))))
#         line9  = "Number of Observations with compared variables EQUAL = {0}".format(str(self.num_of_non_matching_rows - len(set(num_of_error_keys))))
        line9 = ""
        line10  = "Number of new Observations in TABLE-1 = {0}".format(str(self.num_of_new_record_table1))
        line11  = "Number of new Observations in TABLE-2 = {0}".format(str(self.num_of_new_record_table2))
        line12  = "Number of variables compared = {0}".format(str(len(self.table_matchfields)))
        if len(self.error_count_lists):
            line13  = "Number of Variables Compared with All Observations Equal = {0}".format(str(len(self.table_matchfields) - len(self.error_count_lists[0].keys())))
            line14 = "Number of Variables Compared with Some Observations Unequal = {0} ".format(str(len(self.error_count_lists[0].keys())))
        else:
            line13  = "Number of Variables Compared with All Observations Equal = 0"
            line14 = "Number of Variables Compared with Some Observations Unequal = 0"
        line15 = "Total number of values which compare unequal = {0}".format(str(len(self.error_lists)))
        
#         print self.error_lists
        
        width, height = letter
        print 'width=',width
        print 'height=',height
        self.c.setFont("Helvetica", 12)
        
        self.incr = 2
        
        self._write_page1_summary()
        
        self.c.setFont("Helvetica", 8)
        
        for x in range(len(self.table1_not_in_table2_fields)):
            if self.incr * self.linediff > 700:
                self._write_headers
            self.c.drawString(80,self.startval - (self._incr_val() * self.linediff), self.table1_not_in_table2_fields[x][3:] )
            key=self.table1_not_in_table2_fields[x]
            self.c.drawString(300,self.startval - (self.incr * self.linediff), str(self.table1_Schema_type[key][0]))
            self.c.drawString(400,self.startval - (self.incr * self.linediff), str(self.table1_Schema_type[key][1]))
            
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
        self.c.setFont("Helvetica", 10)
        self.c.drawString(160, self.startval - (self._incr_val() * self.linediff), "Number of variables in {0} and not in {1}".format(self.TableNm2,self.TableNm1))
        
        self.c.setFont("Helvetica", 10)
        self.c.drawString(80,self.startval  - (self._incr_val() * self.linediff), "FIELD")
        self.c.drawString(300,self.startval - (self.incr        * self.linediff), "TYPE")
        self.c.drawString(400,self.startval - (self.incr        * self.linediff), "LENGTH")
        self.c.setFont("Helvetica", 8)
       
        for x in range(len(self.table2_not_in_table1_fields)):
            if self.incr * self.linediff > 700:
                self._write_headers()
            self.c.drawString(80,self.startval - (self._incr_val() * self.linediff), self.table2_not_in_table1_fields[x][3:] )
            key=self.table2_not_in_table1_fields[x]
#             print 'self.table2_not_in_table1_fields=',self.table2_not_in_table1_fields
#             print 'self.table2_not_in_table1_fields[x]=',self.table2_not_in_table1_fields[x]
#             print 'self.table2_Schema_type=', self.table2_Schema_type
            self.c.drawString(300,self.startval - (self.incr * self.linediff), str(self.table2_Schema_type[key][0]))
            self.c.drawString(400,self.startval - (self.incr * self.linediff), str(self.table2_Schema_type[key][1]))
        self._write_headers()
        self.c.setFont("Helvetica", 10)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), "Observation Summary")
        self.c.setFont("Helvetica", 8)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line7)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line8)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line9)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line10)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line11)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line12)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line13)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line14)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), line15)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
        self.c.setFont("Helvetica", 10)
        self.c.drawString(220, self.startval - (self._incr_val() * self.linediff),  "Variables with unequal values")
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
        self.c.drawString(80,self.startval -  (self._incr_val() * self.linediff), "FIELD NAME")
        self.c.drawString(400,self.startval - (self.incr        * self.linediff), "COUNT")
        
        self.c.setFont("Helvetica", 8)
        for x in self.error_count_lists:
            for eachitem in x:
                if self.incr * self.linediff > 700:
                    self._write_headers()
                self.c.drawString(80,self.startval -  (self._incr_val() * self.linediff), eachitem[3:])
                self.c.drawString(400,self.startval - (self.incr        * self.linediff), str(x[eachitem]))
                
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
        self.c.setFont("Helvetica", 10)
        self.c.drawString(220, self.startval - (self._incr_val() * self.linediff),  "Value comparison Results for variables")
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
                
        self.c.setFont("Helvetica", 10)
        
        self.c.drawString(80,self.startval  - (self._incr_val() * self.linediff), "Key")
        self.c.drawString(400,self.startval - (self.incr        * self.linediff), "Compare")
        self.c.drawString(240,self.startval - (self.incr        * self.linediff), "Base")
        self.c.setFont("Helvetica", 8)
        count = 0
        for x in range(len(error_lists)):
                
                if self.incr * self.linediff > 700:
                    self._write_headers()
                if x == 0:
                    field_name = error_lists[x]['FIELD']
                    self.c.drawString(240, self.startval - (self._incr_val() * self.linediff),  error_lists[x]['FIELD'])
                    self.c.drawString(400, self.startval - (self.incr * self.linediff),  error_lists[x]['FIELD'])
                else:
                    if field_name <> error_lists[x]['FIELD']:
                        count = 0
                        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
                        self.c.setFont("Helvetica", 10)
                        self.c.drawString(80,self.startval  - (self._incr_val() * self.linediff), "Key")
                        self.c.drawString(240,self.startval - (self.incr        * self.linediff), "Base")
                        self.c.drawString(400,self.startval - (self.incr        * self.linediff), "Compare")
                        self.c.setFont("Helvetica", 8)
                        self.c.drawString(240, self.startval - (self._incr_val() * self.linediff),  error_lists[x]['FIELD'][3:])
                        self.c.drawString(400, self.startval - (self.incr * self.linediff),  error_lists[x]['FIELD'][3:])
                        
                if count < self.num_of_errors:
                    key = ','.join(error_lists[x]['KEY'])
                    self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), key)        
                    self.c.drawString(240, self.startval - (self.incr       * self.linediff), str(error_lists[x]['VALUE-1']))
                    self.c.drawString(400, self.startval - (self.incr       * self.linediff), str(error_lists[x]['VALUE-2']))
                
                field_name = error_lists[x]['FIELD']
                count += 1
                
        self.c.save()
         
        print "Compare ran"
    
            
    def _incr_val(self):
            """This function will increment the value by 1 that will be used to write to next line in the pdf"""
            self.incr += 1
            return self.incr 
    
    def _write_headers(self):
        """This function will write the headers to the pdf report"""
        self.incr = 2
        self.c.showPage()
        self.c.setFont("Helvetica", 10)
        self.c.drawString(400, self.startval - self.linediff,  "Run time = {0}".format(self.t2))
        self.c.drawString(400, self.startval - (self._incr_val() *self.linediff),  "Num of output errors = {0}".format(self.num_of_errors))
        self.c.drawString(400, self.startval - (self._incr_val() *self.linediff),  "Tolerance = {0}".format(self.tolerance))
        self.c.drawString(80, self.startval  - (self._incr_val() * self.linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
        self.c.drawString(320, self.startval - (self.incr * self.linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
        self.c.setFont("Helvetica", 8)
    
    def _write_page1_summary(self):
        """This function will write the summary report in page-1"""
        line1  = 'Number of Observations in {0} = {1}'.format(self.TableNm1,str(self.tot_num_rows_in_table1[0][0]))   
        line2  = 'Number of Observations in {0} = {1}'.format(self.TableNm2,str(self.tot_num_rows_in_table2[0][0] ))
        line3  = "Number of variables in common = {0}".format(str(len(self.table_matchfields)))
        line4  =  "Number of variables in {0} and not in {1} = {2} ".format(self.TableNm1,self.TableNm2,str(len(self.table1_not_in_table2_fields)))
        line5  = "Number of variables in {0} and not in {1} = {2}".format(self.TableNm2,self.TableNm1, str(len(self.table2_not_in_table1_fields)))
        line6  = "Number of ID variables=" + str(len(self.Keyfields))
        
        self.c.drawString(200, self.startval,               "The COMPARE Procedure Report-Table compare")
        self.t2=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.c.drawString(400, self.startval - self.linediff,  "Run time = {0}".format(self.t2))
        self.c.setFont("Helvetica", 8)
        self.c.drawString(400, self.startval - (self._incr_val() *self.linediff),  "Num of output errors = {0}".format(self.num_of_errors))
        self.tolerance = "{:10.15f}".format(self.tolerance)
        self.tolerance = str(float(self.tolerance))
        self.c.drawString(400, self.startval - (self._incr_val() *self.linediff),  "Tolerance = {0}".format(self.tolerance))
        self.c.setFont("Helvetica", 10)
        self.c.drawString(220, self.startval - (self._incr_val() * self.linediff),  "Data set summary")
        self.c.setFont("Helvetica", 8)
        self.c.drawString(80, self.startval  - (self._incr_val() * self.linediff),  "Name of TABLE-1 = "+self.TableNm1.upper())
        self.c.drawString(320, self.startval - (self.incr * self.linediff),         "Name of TABLE-2 = "+self.TableNm2.upper())
        self.c.drawString(80, self.startval  - (self._incr_val() * self.linediff),  line1)
        self.c.drawString(80, self.startval  - (self._incr_val() * self.linediff),  line2)
        self.c.drawString(80, self.startval  - (self._incr_val() * self.linediff),  "")
        self.c.setFont("Helvetica", 10)
        self.c.drawString(220, self.startval - (self._incr_val() * self.linediff),  "Variables summary")
        self.c.setFont("Helvetica", 8)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  line3)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  line4)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  line5)
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  line6)
        
        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
        self.c.setFont("Helvetica", 10)
        self.c.drawString(160, self.startval - (self._incr_val() * self.linediff), "Number of variables in {0} and not in {1}".format(self.TableNm1,self.TableNm2))
        
        self.c.setFont("Helvetica", 10)
        self.c.drawString(80,self.startval  - (self._incr_val() * self.linediff), "FIELD")
        self.c.drawString(300,self.startval - (self.incr        * self.linediff), "TYPE")
        self.c.drawString(400,self.startval - (self.incr        * self.linediff), "LENGTH")
        
    
    def _prepareqry(self):
        """This function will prepare the full outer join query that will be used as the basis for this compare report"""
        for x in range(len(self.Keyfields)):
            if x==0:
                self.keys = self.keys + 'TB1.' + self.Keyfields[x] + ' = TB2.' + self.Keyfields[x]
            else:
                self.keys = self.keys + ' AND TB1.' + self.Keyfields[x] + ' = TB2.' + self.Keyfields[x]
        
        for x in range(len(self.table_matchfields)):
            if x == 0:
                self.where_list = """( {where_list} TB1.{table_matchfields} <> TB2.{table_matchfields}  
                OR (TB1.{table_matchfields} is null AND TB2.{table_matchfields} is not null)
                OR (TB1.{table_matchfields} is not null AND TB2.{table_matchfields} is null) )""".format(where_list=self.where_list,table_matchfields=self.table_matchfields[x] )
                print 'self.where_list=',self.where_list
            else:
                self.where_list = """{where_list} OR ( TB1.{table_matchfields} <> TB2.{table_matchfields}  
                OR (TB1.{table_matchfields} is null AND TB2.{table_matchfields} is not null)
                OR (TB1.{table_matchfields} is not null AND TB2.{table_matchfields} is null) ) """.format(where_list=self.where_list,table_matchfields=self.table_matchfields[x] )
        
        for x in range(len(self.Keyfields)):
            if x == 0:
                self.where_list_1 = self.where_list_1 + '  A.' + self.Keyfields[x] + ' is null'
            else:
                self.where_list_1 = self.where_list_1 + ' OR A.' + self.Keyfields[x] + ' = B.' + self.Keyfields[x]
        
        collist1 = ','.join(['TB1.' + x + ' AS TB1' + x for x in self.table_Schema])
        collist2 = ','.join(['TB2.' + x + ' AS TB2' + x for x in self.table_Schema])
    
        self.qry = """Select   {cols1},{cols2} from {table_nm1} TB1 
                full outer join  
                {table_nm2} TB2
                on {keys}  where {where_list}  """.format(table_nm1=self.TableNm1,table_nm2=self.TableNm2,keys=self.keys,where_list=self.where_list,cols1=collist1,cols2=collist2)
        
        self.runContext.debug( 'self.qry=' + self.qry )

if __name__ == '__main__':
    RC = RunContext('sharedtest')
    dbcontext = RC.getDBContext()
#     x = SqlCompare( run_context=RC, dbcontext= dbcontext,TableNm1='dbo.pre_qctable_table_SAS',TableNm2='dbo.pre_qc_flat_table',Keyfields=['STUDENTID'],db_tag='CSSC',num_of_errors=50,tolerance=0.000001,
#                     excludefields=['id'],output_file='c:/compare.pdf')
#     x = SqlCompare(TableNm1='Employee1',TableNm2='Employee2',Keyfields=['EMP_NO','EMP_FNAME'],db='CSSC',num_of_errors=50,tolerance=0)
    x = SqlCompare( run_context=RC, 

                    dbcontext= dbcontext,

                    TableNm1='school_repnum_Ascii',

                    TableNm2='school_repnum_Ascii',

                    Keyfields=['GRADE'],

                    db_tag='ScoreReportingTestData',

                    num_of_errors=50,

                    tolerance=0.000001,

                    excludefields=[],

                    output_file='c:/compare.pdf')

    x.process()
    
    
    