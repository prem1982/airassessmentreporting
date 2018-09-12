'''
Created on Mar 29, 2013

@author: temp_plakshmanan
'''
import datetime
from pprint import pprint

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

from airassessmentreporting.airutility import RunContext
from query import Query

__all__ = [ 'Compare' ]


def mynext(it):
    try:
        return next(it)
    except StopIteration:
        raise StopIteration(it)
class Compare(object):
    
    def __init__(self,run_context,TableNm1='',TableNm2='',Keyfields=[],excludefields=[],db_tag=None,num_of_errors=0,tolerance=0,debug=False):
        '''
        Constructor: This Object compares 2 tables and list the differences using key fields 
        '''
        self.runContext = run_context
        self.dbTag    = db_tag
        self.incr  = 0 
        self.num_of_errors = num_of_errors
        self.tolerance = tolerance
        self.conn       = self.runContext.getDBContext(tag=self.dbTag)
        self.TableNm1   = TableNm1
        self.TableNm2   = TableNm2
        self.Keyfields  = Keyfields
        self.excludefields   = excludefields
        self.num_of_rows_in_table1 = 0  
        self.num_of_rows_in_table2 = 0
        self.num_of_rows_in_table1 = 0
        self.table1_key = '' 
        self.table2_key = ''
        self.table1_row = '' 
        self.table2_row = ''
        self.exhaust_table_1 = 'N'
        self.exhaust_table_2 = 'N'
        self.compare_ind = ''
        self.debug = debug
        self.table1_row_dict = ''
        self.table2_row_dict = ''
        self.num_of_matched_keys = 0
        self.num_of_new_record_table1 = 0 
        self.num_of_new_record_table2 = 0
        self.error_dict = {}
        self.error_count = {}
        self.error_lists = []
        self.error_count_lists = []
        self.key_values_list = []
        self.table1_Schema_type = {}
        self.table2_Schema_type = {}
        self.t1 = datetime.datetime.now()
        self.c = canvas.Canvas("Compare report",pagesize=letter)
        self.startval = 750
        self.linediff = 12
        if not self.Keyfields:
            raise "error"
        print '-'*80
        print '                         Compare Utility Match Report                '
        print '-'*80
    
    def process(self):
        """This step compares """
        self.Table1_Sel_Qry, self.Table2_Sel_Qry,self.table_matchfields_with_key = Query(self.TableNm1,self.TableNm2,self.Keyfields,self.excludefields,self.dbTag).process()
        if self.debug:
            print "SYSTEM TABLE SELECT QUERY"
            print 'self.Table1_Sel_Qry=',self.Table1_Sel_Qry
            print 'self.Table2_Sel_Qry=',self.Table2_Sel_Qry
            print "SYSTEM TABLE SELECT QUERY----COMPLETE"
        
        
        query="Select name,TYPE_NAME(system_type_id),max_length from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        self.table1_Schema      = self.conn.execute(query,self.TableNm1 )
        self.table1_Schema_type = dict( (str(each[0]).upper() ,[str(each[1]).upper(), each[2] ] ) for each in self.table1_Schema)
        self.table1_Schema      = [str(each[0]).upper() for each in self.table1_Schema]
        
        query="Select name,TYPE_NAME(system_type_id),max_length from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        self.table2_Schema      = self.conn.execute(query,self.TableNm2 )
        self.table2_Schema_type = dict( (str(each[0]).upper() ,[str(each[1]).upper(), each[2] ] ) for each in self.table2_Schema)
        self.table2_Schema      = [str(each[0]).upper() for each in self.table2_Schema]

        self.table1_not_in_table2_fields = [each for each in self.table1_Schema if each not in self.table_matchfields_with_key]
        self.table2_not_in_table1_fields = [each for each in self.table2_Schema if each not in self.table_matchfields_with_key]
        self.table_matchfields = [each for each in self.table_matchfields_with_key if each not in self.Keyfields]
        
        if self.debug:
            print "TABLE_SCHEMA"
            print 'self.table1_Schema=',self.table1_Schema
            print 'self.table2_Schema=',self.table2_Schema
            print 'self.keyfields=',self.Keyfields
            print 'self.table_matchfields=',self.table_matchfields
            print 'self.table1_not_in_table2_fields=',self.table1_not_in_table2_fields
            print 'self.table2_not_in_table1_fields=',self.table2_not_in_table1_fields
            print "TABLE_SCHEMA-----COMPLETE"
            
        conn1 = self.runContext.getDBContext(tag=self.dbTag)
        table1cur = conn1.createcur()
        #The below execute is a general execute and not related to DBContext
        table1cur.execute(self.Table1_Sel_Qry)
        self.table1_generator = conn1.execQuery1(table1cur)
        
        try:
            self.table1_row = mynext(self.table1_generator)
            if self.debug:
                print 'self.table1_generator=',self.table1_generator
                print 'self.table1_row=',self.table1_row
            self.num_of_rows_in_table1 += 1
        except StopIteration:
            self.num_of_rows_in_table1 = 0
        
        conn2 = self.runContext.getDBContext(tag=self.dbTag)
        table2cur = conn2.createcur()
        table2cur.execute(self.Table2_Sel_Qry)
        self.table2_generator =  conn2.execQuery1(table2cur)
        
        
        try:
            self.table2_row = mynext(self.table2_generator)
            if self.debug:
                print 'self.table2_generator=',self.table2_generator
                print 'self.table2_row=',self.table2_row
            self.num_of_rows_in_table2 += 1
        except StopIteration:
            self.num_of_rows_in_table2 = 0
        
        if self.debug:
            print 'self.num_of_rows_in_table1=',self.num_of_rows_in_table1
            print 'self.num_of_rows_in_table2=',self.num_of_rows_in_table2
        
        if  (self.num_of_rows_in_table1 == 0 and
            self.num_of_rows_in_table2 == 0):
            self._printreport()
        elif (self.num_of_rows_in_table1 == 0 and 
             self.num_of_rows_in_table2 <> 0):
            self.num_of_new_record_table2 += 1
            self.exhaust_table_1 = 'N'
            self.exhaust_table_2 = 'Y'
            self._process_addn_rows()
            self._printreport()
        elif (self.num_of_rows_in_table1 <> 0 and 
             self.num_of_rows_in_table2 == 0):
            self.num_of_new_record_table1 += 1
            self.exhaust_table_1 = 'Y'
            self.exhaust_table_2 = 'N'
            self._process_addn_rows()
            self._printreport()
        else:
            self._check_key_field()
            self._comparekeys()
            self._process_addn_rows()
            self._printreport()
    
    def _comparekeys(self):
        end_of_table_1 = 'N'
        end_of_table_2 = 'N'
        while end_of_table_1 =='N' and end_of_table_2 == 'N':
                if self.debug:
                    print 'Compare start'
                    print 'self.table1_key=',self.table1_key
                    print 'self.table2_key=',self.table2_key
                try:
                    if self.table1_key == self.table2_key:
                        if self.debug:
                            print 'same'
                        self.compare_ind = 'A'
                        self._comparefields()
                        self.table1_row = mynext(self.table1_generator)
                        self.num_of_rows_in_table1 += 1
                        self.table2_row = mynext(self.table2_generator)
                        self._check_key_field()
                        self.num_of_rows_in_table2 += 1
                    elif self.table1_key > self.table2_key:
                        if self.debug:
                            print 'key greater'
                        self.compare_ind = 'B'
                        print'1-New row in TABLE2=', self.table2_row_dict
                        self.num_of_new_record_table2 += 1
                        self.table2_row = mynext(self.table2_generator)
                        self._check_key_field()
                        self.num_of_rows_in_table2 += 1
                    elif self.table1_key < self.table2_key:
                        if self.debug:
                            print 'key lesser'
                        print'2-New row in TABLE1=', self.table1_row_dict
                        self.compare_ind = 'C'
                        self.num_of_new_record_table1 += 1
                        self.table1_row = mynext(self.table1_generator)
                        self._check_key_field()
                        self.num_of_rows_in_table1 += 1
                except StopIteration as e:
                    if self.debug:
                        print 'self.table1_generator=',self.table1_generator
                        print 'self.table2_generator=',self.table2_generator
                        print 'ERROR                =',e.args[0]
                        print 'self.compare_ind=',self.compare_ind
                    if self.table1_generator == e.args[0]:
                        if self.debug:
                            print 'equal'
                        if self.compare_ind == 'A':
                            self.exhaust_table_1 = 'N'
                            self.exhaust_table_2 = 'Y'
                        elif self.compare_ind == 'B':
                            self.exhaust_table_1 = 'N'
                            self.exhaust_table_2 = 'Y'
                        elif self.compare_ind == 'C':
                            self.exhaust_table_1 = 'Y'
                            self.exhaust_table_2 = 'N'
                            if self.debug:
                                print'3-New row in TABLE2=', self.table2_row_dict
                            print'3-New row in TABLE2=', self.table2_row_dict
                            self.num_of_new_record_table2 += 1
                    elif self.table2_generator == e.args[0]:
                        if self.debug:
                            print 'not equal'
                        print 'generator-2'
                        print 'self.compare_ind=', self.compare_ind
                        if self.compare_ind == 'A':
                            self.exhaust_table_1 = 'Y'
                            self.exhaust_table_2 = 'N'
                            self.num_of_new_record_table1 += 1 #when generator-2 reaches, generator-1 is already read, which is a new record
                            print'5-New row in TABLE1=', self.table1_row_dict 
                        elif self.compare_ind == 'B':
                            self.exhaust_table_1 = 'N'
                            self.exhaust_table_2 = 'Y'
                            if self.debug:
                                print'4-New row in TABLE1=', self.table1_row_dict
                            self.num_of_new_record_table1 += 1
                        elif self.compare_ind == 'C':
                            self.exhaust_table_1 = 'Y'
                            self.exhaust_table_2 = 'N'
                    if self.debug:                            
                        print 'IterError'
                    end_of_table_1 = end_of_table_2 = 'Y' 
                    
    def _comparefields(self):
        self.num_of_matched_keys +=1 
        if self.debug:
            print 'Compare fields'
            print 'table1_row_dict=',self.table1_row_dict
            print 'table2_row_dict=',self.table2_row_dict
            
            
#         for key in self.table1_row_dict:
#             if key in self.table_matchfields:
#                 if self.table1_row_dict[key] == self.table2_row_dict[key]:
#                     if self.table1_Schema_type[key] <> self.table2_Schema_type[key]:
#                         self._write_error_lists(key)
#                 else:
#                     self._write_error_lists(key)
        for key in self.table1_row_dict:
            if key in self.table_matchfields:
                if self.debug:
                    print 'self.table1_Schema_type[key][0]=',self.table1_Schema_type[key][0]
                    print 'self.table2_Schema_type[key][0]=',self.table2_Schema_type[key][0]
                    print 'self.table1_Schema_type[key][1]=',self.table1_Schema_type[key][1]
                    print 'self.table2_Schema_type[key][1]=',self.table2_Schema_type[key][1]
                    print 'self.table1_row_dict[key]=', self.table1_row_dict[key]
                    print 'self.table2_row_dict[key]=', self.table2_row_dict[key]
                if self.table1_Schema_type[key][0] == self.table2_Schema_type[key][0]: # Checks for the type of the field
                    if self.table1_row_dict[key] == self.table2_row_dict[key]:
                        pass
                    else:
                        compare, numeric = self._check_types(key)
                        if numeric:
#                             if self.table1_row_dict[key] >= self.table2_row_dict[key]:
#                                 result = self.table1_row_dict[key] - self.table2_row_dict[key]
#                             else:
#                                 result = self.table2_row_dict[key] - self.table1_row_dict[key]
                                
                            result = abs(self.table1_row_dict[key] - self.table2_row_dict[key])  / max([abs(self.table1_row_dict[key]),abs(self.table2_row_dict[key])])    
                            if result >= self.tolerance:
                                self._write_error_lists(key)
                        else:
                            self._write_error_lists(key)
#                         if self.table1_Schema_type[key][1] == self.table2_Schema_type[key][1]: #Checks for the length of the field
#                             pass
#                         else:
#                             self._write_error_lists(key)
                else:
                    compare, numeric = self._check_types(key)
                    if compare:
                        if self.table1_row_dict[key] == self.table2_row_dict[key]:
                            pass
                        else:
                            if numeric:
#                                 if self.table1_row_dict[key] >= self.table2_row_dict[key]:
#                                     result = self.table1_row_dict[key] - self.table2_row_dict[key]
#                                 else:
#                                     result = self.table2_row_dict[key] - self.table1_row_dict[key]
                                
                                result = abs(self.table2_row_dict[key] - self.table1_row_dict[key]) / max([self.table2_row_dict[key],self.table1_row_dict[key]])
                                if result >= self.tolerance:
                                    self._write_error_lists(key)
                            else:
                                self._write_error_lists(key)
        self.key_values_list = []
   
    def _tolerance_check(self,key):
        pass
    
    def _check_types(self,key):
            compare = False
            numeric = False
            if ((self.table1_Schema_type[key][0] == 'VARCHAR' and self.table2_Schema_type[key][0] == 'CHAR') or         \
                (self.table1_Schema_type[key][0] == 'CHAR' and self.table2_Schema_type[key][1] == 'VARCHAR')): 
                numeric = False
                compare = True        
            elif ((self.table1_Schema_type[key][0] == 'DECIMAL' and self.table2_Schema_type[key][0] == 'FLOAT') or       \
                  (self.table1_Schema_type[key][0] == 'FLOAT' and self.table2_Schema_type[key][1] == 'DECIMAL') or
                  (self.table1_Schema_type[key][0] == 'DECIMAL' and self.table2_Schema_type[key][0] == 'DECIMAL') or
                  (self.table1_Schema_type[key][0] == 'FLOAT' and self.table2_Schema_type[key][0] == 'FLOAT')):
                numeric = True
                compare = True
            return compare, numeric
        
        
    def _write_error_lists(self,key):
        if self.debug:
            print '_write_error_lists='
            
        key_present = False
        key_not_present = False
        if len(self.error_count_lists) == 0:
            self.error_count[key] = 1
            self.error_count_lists.append(self.error_count)
        else:
            for each in self.error_count_lists:
                if key in each:
                    key_present = True
                    break
                else:
                    key_not_present = True
        if self.debug:
            print 'key_present=',     key_present
            print 'key_not_present=', key_not_present 
        
        if key_present == True:self.error_count[key] = self.error_count[key] + 1
        
        if key_not_present == True:
            self.error_count[key] = 1

        if self.error_count[key] < self.num_of_errors:
            self._write_error_dict(key)
            

        
    def _write_error_dict(self,key):
        self.error_dict['KEY']  = self.key_values_list
        self.error_dict['FIELD']   = key
        self.error_dict['VALUE-1'] = self.table1_row_dict[key]
        self.error_dict['VALUE-2'] = self.table2_row_dict[key]
        self.error_lists.append(self.error_dict)
        self.error_dict = {}
         
        
    def _process_addn_rows(self):
        
        end_of_table_1 = end_of_table_2 = 'N'
        if self.exhaust_table_2 == 'Y':
            while end_of_table_2 == 'N':
                try:
                    self.table2_row = mynext(self.table2_generator)
                    self.table2_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields_with_key,list(self.table2_row)))
                    print'New row in TABLE2=', self.table2_row_dict
                    self.num_of_new_record_table2 += 1
                    self.num_of_rows_in_table2 += 1
                except StopIteration as e:
                    if self.debug:
                        print 'ERROR=',e.args
                        print 'self.table1_generator=',self.table1_generator
                    if self.table1_generator == e.args:
                        if self.debug:
                            print 'equal'
                    else:
                        if self.debug:
                            print 'not equal'
                    end_of_table_2 = 'Y'
        
        if self.exhaust_table_1 == 'Y':
            while end_of_table_1 == 'N':
                try:
                    self.table1_row = mynext(self.table1_generator)
                    self.table1_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields_with_key,list(self.table1_row)))
                    print'New row in TABLE1=', self.table1_row_dict
                    self.num_of_new_record_table1 += 1
                    self.num_of_rows_in_table1 += 1
                except StopIteration as e:
#                     if self.debug:
                    print 'ERROR=',e.args
                    print 'self.table1_generator=',self.table1_generator
                    end_of_table_1 = 'Y'
        
        self.exhaust_table_1 = 'N'
        self.exhaust_table_2 = 'N'
                
    def _check_key_field(self):
        self.table1_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields_with_key,list(self.table1_row)))
        self.table2_row_dict = dict((el1,el2) for el1, el2 in zip(self.table_matchfields_with_key,list(self.table2_row)))
        if self.debug:
            print 'self.table1_Schema=', self.table1_Schema
            print 'self.table2_Schema=', self.table2_Schema
            print 'self.table1_row_dict=',self.table1_row_dict
            print 'self.table2_row_dict=',self.table2_row_dict
        for x in self.Keyfields:
            if self.debug:
                print 'self.table1_row_dict[x]=',self.table1_row_dict[x]
                print 'self.table1_row_dict[x]=',self.table2_row_dict[x]
            if self.table1_row_dict[x] == self.table2_row_dict[x]:
                self.key_values_list.append(self.table1_row_dict[x])
                self.table1_key = 1
                self.table2_key = 1
            elif self.table1_row_dict[x] > self.table2_row_dict[x]:
                self.table1_key = 2
                self.table2_key = 1
                
                break
            elif self.table1_row_dict[x] < self.table2_row_dict[x]:
                self.table1_key = 1
                self.table2_key = 2
                
                break
                
    def _printreport(self):
        print '-'*80 + 'Error Report' + '-'*80 
        print "error_count=",self.error_count_lists
        self.TableNm1 = self.TableNm1.upper()
        self.TableNm2 = self.TableNm2.upper()
        for x in self.error_count_lists:
            pprint(x)
        
        print "error_lists=",self.error_lists
        num_of_error_keys =[]
        from operator import itemgetter
        error_lists = sorted(self.error_lists,key=itemgetter('FIELD'))
        for x in error_lists:
            print x
            num_of_error_keys.append(x['KEY'][0])
        print 'Number of Observations in TABLE1=',self.num_of_rows_in_table1   
        print 'Number of Observations in TABLE2=',self.num_of_rows_in_table2
        print "Number of Observations with MATCHED KEYS=",self.num_of_matched_keys
        print "Number of Observations with compared variables unequal=", len(set(num_of_error_keys)) #Number of unique keys in the error lists
        print "Number of Observations with compared variables EQUAL=", self.num_of_matched_keys - len(set(num_of_error_keys)) #Total number of matched records - Total number of unique keys in error table
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
        
        line7  = "Number of Observations with MATCHED KEYS = {0}".format(str(self.num_of_matched_keys))
        line8  = "Number of Observations with compared variables unequal = {0}".format(str(len(set(num_of_error_keys))))
        line9  = "Number of Observations with compared variables EQUAL = {0}".format(str(self.num_of_matched_keys - len(set(num_of_error_keys))))
        line10  = "Number of new Observations in TABLE-1 = {0}".format(str(self.num_of_new_record_table1))
        line11  = "Number of new Observations in TABLE-2 = ".format(str(self.num_of_new_record_table2))
        line12  = "Number of variables compared = ".format(str(len(self.table_matchfields)))
        if len(self.error_count_lists):
            line13  = "Number of Variables Compared with All Observations Equal = {0}".format(str(len(self.table_matchfields) - len(self.error_count_lists[0].keys())))
            line14 = "Number of Variables Compared with Some Observations Unequal {0}= ".format(str(len(self.error_count_lists[0].keys())))
        else:
            line13  = "Number of Variables Compared with All Observations Equal = 0"
            line14 = "Number of Variables Compared with Some Observations Unequal = 0"
        line15 = "Total number of values which compare unequal = ".format(str(len(self.error_lists)))
        
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
            self.c.drawString(80,self.startval - (self._incr_val() * self.linediff), self.table1_not_in_table2_fields[x] )
            print 'self.table1_not_in_table2_fields[x]=',self.table1_not_in_table2_fields[x]
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
            self.c.drawString(80,self.startval - (self._incr_val() * self.linediff), self.table2_not_in_table1_fields[x] )
            key=self.table2_not_in_table1_fields[x]
            self.c.drawString(300,self.startval - (self.incr * self.linediff), str(self.table2_Schema_type[key][0]))
            self.c.drawString(400,self.startval - (self.incr * self.linediff), str(self.table2_Schema_type[key][1]))
        print 'write-1'     
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
                self.c.drawString(80,self.startval -  (self._incr_val() * self.linediff), eachitem)
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
        for x in range(len(error_lists)):
                if self.incr * self.linediff > 700:
                    self._write_headers()
                if x == 0:
                    field_name = error_lists[x]['FIELD']
                    self.c.drawString(240, self.startval - (self._incr_val() * self.linediff),  error_lists[x]['FIELD'])
                    self.c.drawString(400, self.startval - (self.incr * self.linediff),  error_lists[x]['FIELD'])
                else:
                    if field_name <> error_lists[x]['FIELD']:
                        self.c.drawString(80, self.startval - (self._incr_val() * self.linediff),  "")
                        self.c.setFont("Helvetica", 10)
                        self.c.drawString(80,self.startval  - (self._incr_val() * self.linediff), "Key")
                        self.c.drawString(400,self.startval - (self.incr        * self.linediff), "Compare")
                        self.c.drawString(240,self.startval - (self.incr        * self.linediff), "Base")
                        self.c.setFont("Helvetica", 8)
                        self.c.drawString(240, self.startval - (self._incr_val() * self.linediff),  error_lists[x]['FIELD'])
                        self.c.drawString(400, self.startval - (self.incr * self.linediff),  error_lists[x]['FIELD'])
                        
                        
                key = ','.join(error_lists[x]['KEY'])                    
                self.c.drawString(80, self.startval - (self._incr_val() * self.linediff), key)        
                self.c.drawString(240,self.startval - (self.incr        * self.linediff), str(error_lists[x]['VALUE-1']))
                self.c.drawString(400,self.startval - (self.incr        * self.linediff), str(error_lists[x]['VALUE-2']))
                
                field_name = error_lists[x]['FIELD']
                
        self.c.save()
         
        print "Compare ran"
    
            
    def _incr_val(self):
            self.incr += 1
            return self.incr 
    
    def _write_headers(self):
        print 'write headers'
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
        line1  = 'Number of Observations in {0} = {1}'.format(self.TableNm1,str(self.num_of_rows_in_table1))   
        line2  = 'Number of Observations in {0} = {1}'.format(self.TableNm2,str(self.num_of_rows_in_table2))
        line3  = "Number of variables in common = {0}".format(str(len(self.table_matchfields)))
        line4  =  "Number of variables in {0} and not in {1} = {2} ".format(self.TableNm1,self.TableNm2,str(len(self.table1_not_in_table2_fields)))
        line5  = "Number of variables in {0} and not in {1} = {2}".format(self.TableNm2,self.TableNm1, str(len(self.table2_not_in_table1_fields)))
        line6  = "Number of ID variables=" + str(len(self.Keyfields))
        
        self.c.drawString(200, self.startval,               "The COMPARE Procedure Report")
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
        

if __name__ == '__main__':
    context = RunContext('unittest')
    x = Compare( context, TableNm1='Employee1',TableNm2='Employee2',Keyfields=['EMP_NO','EMP_FNAME'],tag='CSSC',num_of_errors=50,tolerance=0)
#     x = Compare(TableNm1='dbo.compare1',TableNm2='dbo.compare2',Keyfields=['SSID'],db='Share',num_of_errors=50,tolerance=0)
#     x = Compare(TableNm1='dbo.read_gr04_1',TableNm2='dbo.read_gr04_2',Keyfields=['SSID'])
    x.process()






