 
 
'''
Created on Apr 30, 2013
 
@author: temp_plakshmanan
'''
import re
import datetime
 
from airassessmentreporting.airutility import RunContext
from airassessmentreporting.airutility import SafeExcelReader
 
COLUMN_LIST = ['START','END','TYPE','MIN','MAX','NOMINAL_VALUES','AIR_STD_CODE','VARIABLE_NAME','DESCRIPTION','COMMENTS','COUNTS_NEEDED']
IGNORE_LIST = ['IMPORT_ORDER', 'LENGTH', 'ITEM_ORDER', 'F14', 'F15']
 
class LayoutCheck(object):
    '''
    classdocs
    '''
    def __init__(self,filename,sheet_name='sheet1',output_table='temp1',get_names=True, delimiter=','):
        '''
        Constructor:This init step will take the following inputs - 
        Layout filename- The Input file name 
        sheet name (defaults to sheet1)- The Name of the sheet that will be loaded 
        Output_table name- The Output table name that will be created in the sqlserver 
        delimiter- Delimiter
        Get_names_indicator - Indicate to load the first column or not
        '''
        self.RC         =   RunContext('unittest')
        self.dbcontext  =   self.RC.getDBContext()
        self.filename   =   filename
        self.sheet_name =   sheet_name
        self.output_table = output_table
        self.get_names  =   get_names
        self.delimiter  =   delimiter
        self.layoutdict ={}
        self.maxmindict ={}
        self.recodingsdict ={}
     
    def process(self):
         
        """This is the main process that controls the flow of this module"""
         
        SE = SafeExcelReader(self.RC, self.filename, self.sheet_name, self.output_table, db_context=self.dbcontext,get_names=True, delimiter=',', import_order='import_order')
        SE.createTable()
        try:
            self._checktypes()
            self._checkemptyrows()
            self._checkinvalidvariables()
            self._checkvalidranges()
            self._recodings_and_create_dicts()
        except Exception as error:
                print 'Module:LAYOUT_CHECK Error=',error
         
        return self.layoutdict, self.maxmindict, self.recodingsdict
         
    def _checktypes(self):
        
         
        """This function checks for the type of the following fields-START,END,NOMINAL_VALUES & AIR_STD_CODE"""
         
        typechecksql="""SELECT TB1.NAME as COLUMN_NAME ,TB2.NAME as COLUMN_TYPE 
                        from sys.columns tb1 
                        inner join sys.types tb2 
                        on tb1.system_type_id = tb2.system_type_id 
                        where tb1.object_id = object_id(?) and 
                        tb2.name <>'sysname'"""
        
        result=self.dbcontext.execute(query=typechecksql,parameters=[self.output_table])
         
         
         
        columns = [str(getattr(each,'COLUMN_NAME').upper()) for each in result if getattr(each,'COLUMN_NAME').upper() not in COLUMN_LIST and getattr(each,'COLUMN_NAME').upper() not in IGNORE_LIST ]
         
        if columns:
            error= "The following columns {0} are missing from the Input file".format(columns)
            raise Exception(error)
         
        for each in result:
            if getattr(each,'COLUMN_NAME').upper() == 'START': 
                if getattr(each,'COLUMN_TYPE').upper() <> 'FLOAT':
                    raise Exception('The START Column must be of type FLOAT')
             
            if getattr(each,'COLUMN_NAME').upper() == 'END': 
                if getattr(each,'COLUMN_TYPE').upper() <> 'FLOAT':
                    raise Exception('The END Column must be of type FLOAT')
             
            if getattr(each,'COLUMN_NAME').upper() == 'NOMINAL_VALUES': 
                if getattr(each,'COLUMN_TYPE').upper() <> 'NVARCHAR':
                    raise Exception('The NOMINAL_VALUES Column must be of type NVARCHAR')
             
            if getattr(each,'COLUMN_NAME').upper() == 'AIR_STD_CODE': 
                if getattr(each,'COLUMN_TYPE').upper() <> 'NVARCHAR':
                    raise Exception('The AIR_STD_CODE Column must be of type NVARCHAR')
         
    def _checkemptyrows(self):
        """This functions checks for any blank row in the table and flags it"""
         
        qrytemplate="""select import_order from {tablename} where start='' and nominal_values='' and comments=''  """.format(tablename=self.output_table)
         
        result = self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following rows= {0} are blank in the table, Check and correct ".format(result)
            raise Exception(error)
         
     
    def _checkinvalidvariables(self):
        """This function validates the variable names in following ways
        1.Checks for any space in variable name
        2.Checks for variable length more than 28 characters
        3.Flags variable name that starts with bad_
        4.Flags variable name that ends with _tmp
        5.Checks for duplicate variable names
        """
        #Checks for any space in the variable names
        qrytemplate="""select variable_name 
                            FROM {tablename} 
                            where Variable_name <> '' 
                            and charindex(' ',Variable_name) > 0 """.format(tablename=self.output_table)
         
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following variables contain spaces in between them - {tablename}".format(tablename=self.output_table)
            raise Exception(error)
        #Checks for variable names greater than 28
        qrytemplate="""select variable_name 
                            from {tablename} 
                            where Variable_name <> '' 
                            and len(variable_name) > 28 group by variable_name""".format(tablename=self.output_table)
         
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following variables length is too long, it should be less than 28 characters - {0}".format(result)
            raise Exception(error)
        #Checks for variable name that starts with bad_
        qrytemplate="""select variable_name 
                            from {tablename} 
                            where Variable_name <> '' 
                            and substring(variable_name,1,4) = 'bad_'  """.format(tablename=self.output_table)
          
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following variables cannot start with bad_ {0}".format(result)
            raise Exception(error)
        #Checks for variable name that ends with _tmp
        qrytemplate="""select variable_name 
                            from {tablename} 
                            where Variable_name <> '' 
                            and substring(variable_name,len(variable_name) -3,4) = '_tmp'""".format(tablename=self.output_table)
         
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following variables cannot end with _tmp {0}".format(result)
            raise Exception(error)
        #Checks for duplicate variable names
        qrytemplate="""select variable_name, count(*) 
                        from {tablename} 
                        where variable_name <> ' ' 
                        group by variable_name having count(*) > 1""".format(tablename=self.output_table)
          
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following variables are duplicate -[VARIABLE_NAME,COUNT]{0}".format(result)
            raise Exception(error)
         
    def _checkvalidranges(self):
        """This function does the following checks
        1.START value but no END value.
        2.START value greater than END. 
        3.START value is negative.
        4.No TYPE value defined.
        5.TYPE values are not defined correctly- ONLY valid types are allowed.
        6.Check for any overlapping for START & END.
        """
        #Checks for START but no END value
        qrytemplate="""select import_order,start,[end] 
                        from {tablename} 
                        where variable_name <> ' ' 
                        and start is not  null 
                        and ([end] is null or [end] =0)""".format(tablename=self.output_table)
                         
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following rows-[rowmnum,start,end] {0} has a START VALUE but no END".format(result)
            raise Exception(error)
        #Checks for START greater than END
        qrytemplate="""select import_order,start,[end] 
                            from {tablename} 
                            where variable_name <> ' ' 
                            and start  is not null 
                            and [end] is not null  
                            and start - [end] > 0""".format(tablename=self.output_table)
                             
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following rows-[rowmnum,start,end] {0} has START greater than END".format(result)
            raise Exception(error)
        #Checks for negative values in START
        qrytemplate=""" select import_order,start,[end] 
                            from {tablename} 
                            where variable_name <> ' ' 
                            and start  is not null 
                            and start < 0""".format(tablename=self.output_table)
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following rows-[rowmnum,start,end] {0} has START that has negative values".format(result)
            raise Exception(error)
        #Checks for rows with no TYPE values
        qrytemplate=""" select import_order,start,[end] 
                        from  {tablename} 
                        where start is not null 
                        and type is null""".format(tablename=self.output_table)
                         
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following rows-[rowmnum,start,end] {0} has NO TYPE values".format(result)
            raise Exception(error)
        #Checks for rows with invalid TYPE values
        qrytemplate=""" select import_order,start,[end] 
                                from  {tablename} 
                                where start is not null 
                                and (UPPER(type) not in ('INTEGER','STRING','REAL','NOMINAL','NOMINAL_N') 
                                and UPPER(type) not like 'ID[0-9]' and UPPER(type) not like 'NOMINAL_ID[0-9]')""".format(tablename=self.output_table)
                                 
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if result:
            error= "The following rows-[rowmnum,start,end] {0} has wrong TYPES defined ONLY- integer, string, real, nominal, nominal_n or ID1, ID2, ID3, ... allowed".format(result)
            raise Exception(error)
        #Checks for rows with overlapping START and END
        qrytemplate="""SELECT TB1.START,TB1.[END] FROM 
                            (
                                select START,[END],ROW_NUMBER() over(order by import_order) as rownum from 
                                {tablename} 
                                where variable_name <> ' '
                                ) TB1
                                INNER JOIN
                                (
                                select START,[END],ROW_NUMBER() over(order by import_order) as rownum from 
                                {tablename} 
                                where variable_name <> ' '
                                ) TB2
                                ON TB1.rownum + 1 = TB2.rownum 
                                WHERE TB1.[END] > TB2.START""".format(tablename=self.output_table)
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
        if len(result):
            error= "The following rows-[start,end] {0} has overlappings".format(result)
            raise Exception(error)
     
    def _recodings_and_create_dicts(self):
        """This method iterates over all the rows in the Layout table and peforms the following
        1.Checks if any variable name contains bad characters- The rules are in the regular expression.
        2.Checks if the recoded values are in the proper format.
        3.Prepares 3 dicts-Layoutdicts-['variablename':[start,end,type], Maxmindicts-[max,min] and recordingsdict-[nominal_Values,air_std_value]
        """
        qrytemplate="""SELECT type,air_std_code,import_order, variable_name, start, [end],max,min,nominal_values, air_std_code 
                        from {tablename} order by import_order""".format(tablename=self.output_table)
                         
        result=self.dbcontext.execute(query=qrytemplate,parameters=[])
         
        for each in result:
            #Check to make sure the variable names has valid names
             
            if getattr(each,'variable_name') is not None and getattr(each,'variable_name') <> ' ':
                if not re.search(r"^[a-zA-Z][a-zA-Z0-9_]*$",getattr(each,'variable_name')):
                    error= "The variable {0} in line number - {1} - contains bad characters ".format(getattr(each,'variable_name'),getattr(each,'import_order'))
                    raise Exception(error)
             
            #Check to make sure the recorded values are in the proper format
            if getattr(each,'type') is not None:
                vartype = getattr(each,'type')
                cnt = 1
            else:   
                cnt+=1
             
            if ((vartype == 'integer' or vartype =='real' or vartype[0:2] == 'id') and cnt > 1) or vartype =='nominal_n':
                if not re.search(r"^(-?\d+(\.\d*)?|\.[A-Z]?)$",str(getattr(each,'air_std_code'))):
                    error= "The recoded value '{0}' must be numeric (or a period followed by a single uppercase letter) for the following [import_order=] {1}".format(getattr(each,'air_std_code'),getattr(each,'import_order'))
                    raise Exception(error)
 
            t1 = datetime.datetime.now()
            if (getattr(each,'start') not in ['',None] and getattr(each,'end') not in ['',None]):
                self.layoutdict[str(getattr(each,'variable_name'))] = [int(getattr(each,'start')),int(getattr(each,'end')),str(getattr(each,'type'))]
                if (getattr(each,'max') not in ['',None] and getattr(each,'min') not in ['',None]):    
                    self.maxmindict[str(getattr(each,'variable_name'))] = [int(str(getattr(each,'min'))),int(str(getattr(each,'max')))]
                #The check for missed values on variable_name & nominal_values is not done as missing values comes with dot(.)
                self.recodingsdict[str(getattr(each,'variable_name'))] = [[str(getattr(each,'nominal_values'))],[str(getattr(each,'air_std_code'))]]
#                 print 'Last Added variable name=',getattr(each,'variable_name')
                variable_name_cache = str(getattr(each,'variable_name'))
            else:
#                 print 'recodingsdict=',recodingsdict
#                 print 'str(getattr(each,nominal_values))=',str(getattr(each,'nominal_values'))
#                 print 'str(getattr(each,air_std_code))=',str(getattr(each,'air_std_code'))
#                 print 'variable_name_cache=',variable_name_cache
                self.recodingsdict[variable_name_cache][0].append(str(getattr(each,'nominal_values')))
                self.recodingsdict[variable_name_cache][1].append(str(getattr(each,'air_std_code')))
             
            t2 = datetime.datetime.now()
             
         
        print 'len-layoutdict=',len(self.layoutdict)
        print 'len-maxmindict=',len(self.maxmindict)
        print 'len-recodngsdict=',len(self.recodingsdict)
        self._writereports()
 
    def _writereports(self):
        import os
        import operator
         
        result = sorted(self.layoutdict.iteritems(),key=operator.itemgetter(1))
         
        f = open('c:/layoutdict', "w") 
        for (variablename,startend) in result:
            str1 = "Variable Name=" + variablename + '                                ' + "StartEnd=" + str(startend)
            f.write(str1 + '\n')
 
        f.close()
         
        result = sorted(self.maxmindict.iteritems(),key=operator.itemgetter(1))
         
        f = open('c:/maxmindict', "w") 
        for (variablename,startend) in result:
            str1 = "Variable Name=" + variablename + '                                ' + "StartEnd=" + str(startend)
            f.write(str1 + '\n')
 
        f.close()
         
        result = sorted(self.recodingsdict.iteritems(),key=operator.itemgetter(1))
        f = open('c:/recodingsdict', "w") 
        for (variablename,startend) in result:
            str1 = "Variable Name=" + variablename + '                                ' + "StartEnd=" + str(startend)
            f.write(str1 + '\n')
 
        f.close()
 
             
 
if __name__ == '__main__':
    x = LayoutCheck(filename='C:\CVS Projects\CSSC Score Reporting\OGT Spring 2012\Input Layout\OGT_SP12_Op_DataLayout_IntakeLayout.xls',output_table='temp1')
    x.process()
    
     
     
     