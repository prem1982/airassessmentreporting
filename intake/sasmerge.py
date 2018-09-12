'''
Created on Aug 26, 2013

@author: temp_plakshmanan
'''
from threading import Thread
from multiprocessing import Pool
from airassessmentreporting.erasure import BookMapReader
from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec

class SasMerge( object ):
    """ This module merges 2 tables and works similar to SAS Merge function.
    SAS Merge merges 2 dataset and considers the 2nd dataset field as the output fields. """
    def __init__(self, runcontext, dbcontext, merge_table_1 = " ", merge_table_2 = "", output_table = " ", mergeids = [], drop_columns = [], keep_columns = [] ):
        self.rc = runcontext
        self.db = dbcontext
        self.mergeids = [each.upper() for each in mergeids]
        self.merge_table_1 = merge_table_1.upper()
        self.merge_table_2 = merge_table_2.upper()
        self.drop_columns = [each.upper() for each in drop_columns]
        self.output_table = output_table.upper()
        self.common_columns = []
        self.keep_columns = [each.upper() for each in keep_columns]
        self.keep_columns_string = ','.join(self.keep_columns)
        self.table1_columns = []
        self.table2_columns = []
        self.table1_columns_nulls = []
        self.table2_columns_nulls = []
        self.mergeid_ON = []
        self.mergeid_ON_fields = ''
        self.mergeid_WHERE_TB1 = []
        self.mergeid_WHERE_TB1_fields = ''
        self.mergeid_WHERE_TB2 = []
        self.mergeid_WHERE_TB2_fields = ''
        self.mergeid_fields_TB1 = ''
        self.mergeid_fields_TB2 = ''
        self.common_columns_fields_TB1 = ''
        self.common_columns_fields_TB2 = ''
        self.table1_columns_fields = ''
        self.table2_columns_fields = ''
        self.table_columns_fields = ''
        self.table1_columns_fields_nulls = ''
        self.table2_columns_fields_nulls = ''
        self.output_table = output_table
        drop_table_if_exists( 'SAS_MERGE_1', self.db)
        drop_table_if_exists( 'SAS_MERGE_2', self.db)
        drop_table_if_exists( 'SAS_MERGE_3', self.db)
        drop_table_if_exists(self.output_table, self.db)
        
    def process(self):
        print self.drop_columns
        print self.keep_columns

        
        if self.keep_columns and self.drop_columns:
            print "You can't drop AND keep COLUMNS \n Please correct it!!"
            
        duplicate_elem = set(self.drop_columns).intersection(set(self.keep_columns))
        print 'duplicate_elem = ', duplicate_elem
        
        
        if duplicate_elem:
            print 'SAME ELEMENTS PROVIDED IN KEEP_COLUMNS & DUPLICATE_COLUMNS, \n     Please correct it!!'
        else:
            self._get_column_names()
            self._prepare_columns()
            self._thread_processing()
            self._insert_merge_data_into_table()
#             self._process_drop_columns()
#             self._process_keep_columns()
    
    def _get_column_names(self):
        """ This method generates 3 lists
        1. All matched columns between table_1 & table_2
        2. Columns only in table_1
        3. Columns only in table_2"""
        
        common_columns_query = """SELECT A.NAME FROM SYS.COLUMNS A,SYS.COLUMNS B WHERE A.NAME = B.NAME AND a.object_id = OBJECT_ID('{0}') 
                                    AND b.object_id = OBJECT_ID('{1}')""".format(self.merge_table_1, self.merge_table_2)
        self.common_columns = self.db.execute(common_columns_query)
        self.common_columns = [each[0].encode('ascii').upper() for each in self.common_columns]
        self.common_columns = [ each for each in self.common_columns if each not in self.mergeids]
        print 'self.common_columns =', self.keep_columns
        self.common_columns = [ each for each in self.common_columns if each not in self.drop_columns]
        
#         print list(set(self.common_columns).intersection(set(self.keep_columns)))
#         keep_these_columns = list(set(self.keep_columns).intersection(set(self.keep_columns)))
#         self.common_columns = [ each for each in self.common_columns if each in keep_these_columns]
        
        print 'common_columns =', self.common_columns
            
        table1_columns_query= """SELECT TB1.*, TB2.* FROM (SELECT A.NAME FROM SYS.COLUMNS A WHERE a.object_id = OBJECT_ID('{0}') ) TB1
                                LEFT OUTER JOIN (SELECT B.NAME FROM SYS.COLUMNS B WHERE B.object_id = OBJECT_ID('{1}') ) TB2 ON TB1.NAME = TB2.NAME
                                WHERE TB2.NAME is null """.format(self.merge_table_1, self.merge_table_2)
        self.table1_columns_results = self.db.execute(table1_columns_query)
        self.table1_columns = ['TB1.' + each[0].encode('ascii').upper() for each in self.table1_columns_results if each[0].encode('ascii').upper() not in self.drop_columns]
        self.table1_columns_nulls = [ 'null as ' + each[0].encode('ascii').upper() for each in self.table1_columns_results if each[0].encode('ascii').upper() not in self.drop_columns]
        print 'table1_column_results = ', self.table1_columns
        print 'self.table1_columns_nulls = ', self.table1_columns_nulls
        
        table2_columns_query = """SELECT TB1.*, TB2.* FROM (SELECT A.NAME FROM SYS.COLUMNS A WHERE a.object_id = OBJECT_ID('{0}') ) TB1
                                LEFT OUTER JOIN (SELECT B.NAME FROM SYS.COLUMNS B WHERE B.object_id = OBJECT_ID('{1}') ) TB2 ON TB1.NAME = TB2.NAME
                                WHERE TB2.NAME is null """.format(self.merge_table_2, self.merge_table_1)

        self.table2_columns_results = self.db.execute(table2_columns_query)
        self.table2_columns = ['TB2.' + each[0].encode('ascii').upper() for each in self.table2_columns_results if each[0].encode('ascii').upper() not in self.drop_columns]
        self.table2_columns_nulls = ['null as ' + each[0].encode('ascii').upper() for each in self.table2_columns_results if each[0].encode('ascii').upper() not in self.drop_columns]
        print 'table2_column_results = ', self.table2_columns
        print 'self.table2_columns_nulls = ', self.table2_columns_nulls
            
    def _prepare_columns(self):
        print 'populate_tables'
        self.mergeid_ON = [ "TB1.{0} = TB2.{1} ".format(elem,elem) for elem in self.mergeids]
        self.mergeid_ON_fields = ' AND '.join(elem for elem in self.mergeid_ON)
        
        print 'mergeid_ON = ', self.mergeid_ON
        print 'mergeid_ON_fields = ', self.mergeid_ON_fields
        
        self.mergeid_WHERE_TB1 =[ "TB1.{0} is null".format(elem) for elem in self.mergeids]
        self.mergeid_WHERE_TB1_fields = ' AND '.join(elem for elem in self.mergeid_WHERE_TB1)
        
        print 'mergeid_WHERE_fields = ', self.mergeid_WHERE_TB1_fields
        
        self.mergeid_WHERE_TB2 =[ "TB2.{0} is null".format(elem) for elem in self.mergeids]
        self.mergeid_WHERE_TB2_fields = ' AND '.join(elem for elem in self.mergeid_WHERE_TB2)
         
        self.mergeid_fields_TB1 = ','.join('TB1.'+ elem for elem in self.mergeids)
        self.mergeid_fields_TB2 = ','.join('TB2.'+ elem for elem in self.mergeids)
        
        self.common_columns_fields_TB1 =  ','.join('TB1.' + elem for elem in self.common_columns)
        #The query was failing if the fields are empty... so populating ',' before the strings are populated here..
        if len(self.common_columns_fields_TB1) > 0:
            self.common_columns_fields_TB1 = ',' + ','.join('TB1.' + elem for elem in self.common_columns)
        
        self.common_columns_fields_TB2 = ','.join('TB2.' + elem for elem in self.common_columns)
        
        if len(self.common_columns_fields_TB2) > 0:
            self.common_columns_fields_TB2 = ',' + ','.join('TB2.' + elem for elem in self.common_columns)
        
        self.table1_columns_fields = ','.join(elem for elem in self.table1_columns)
        
        if len(self.table1_columns_fields) > 0:
            self.table1_columns_fields = ',' + ','.join(elem for elem in self.table1_columns)
            
        self.table2_columns_fields = ','.join(elem for elem in self.table2_columns)
        
        if len(self.table2_columns_fields) > 0:
            self.table2_columns_fields = ',' + ','.join(elem for elem in self.table2_columns)
        
        self.table1_columns_fields_nulls = ','.join(elem for elem in self.table1_columns_nulls)
        
        if len(self.table1_columns_fields_nulls) > 0:
            self.table1_columns_fields_nulls = ',' + ','.join(elem for elem in self.table1_columns_nulls)
        
        self.table2_columns_fields_nulls = ','.join(elem for elem in self.table2_columns_nulls)
        
        if len(self.table2_columns_fields_nulls) > 0 :
            self.table2_columns_fields_nulls = ',' + ','.join(elem for elem in self.table2_columns_nulls)
        
        print self.common_columns_fields_TB1
        
    def _insert_left_join(self):
        print 'insert_left_join'
        """This query for all unique rows in TABLE-1"""
        print 'self.table1_columns_fields = ', self.table1_columns_fields
        
        if not self.keep_columns:
            qry1 = """select  {mergeid_fields_TB1}  {common_columns_fields_TB1}  {table1_columns_fields}  {table2_columns_fields_nulls} into SAS_MERGE_1 
                from {merge_table_1} tb1 left outer join {merge_table_2} tb2 ON {mergeid_ON_fields} WHERE {mergeid_WHERE_TB2_fields}""".format(mergeid_fields_TB1 = self.mergeid_fields_TB1,
                        common_columns_fields_TB1 = self.common_columns_fields_TB1, table1_columns_fields = self.table1_columns_fields, 
                        table2_columns_fields_nulls = self.table2_columns_fields_nulls, merge_table_1 = self.merge_table_1, merge_table_2 = self.merge_table_2, 
                        mergeid_ON_fields = self.mergeid_ON_fields, mergeid_WHERE_TB2_fields = self.mergeid_WHERE_TB2_fields)
            
        else:
            qry1 = """select  {keep_columns_string} into SAS_MERGE_1 
                from {merge_table_1} tb1 left outer join {merge_table_2} tb2 ON {mergeid_ON_fields} WHERE {mergeid_WHERE_TB2_fields}""".format(merge_table_1 = self.merge_table_1, 
                merge_table_2 = self.merge_table_2, mergeid_ON_fields = self.mergeid_ON_fields, mergeid_WHERE_TB2_fields = self.mergeid_WHERE_TB2_fields, keep_columns_string = self.keep_columns_string)
        
        print 'qry1 = ', qry1
        dbcontext1 = self.rc.getDBContext(cached = False)
        dbcontext1.executeNoResults(qry1)
        dbcontext1.close()
        
    def _insert_right_join(self):
        """This query for all unique rows in TABLE-2"""
        
        if not self.keep_columns:
            qry2 = """select  {mergeid_fields_TB2}  {common_columns_fields_TB2}  {table2_columns_fields}  {table1_columns_fields_nulls} into SAS_MERGE_2 
            from {merge_table_1} tb1
            right outer join {merge_table_2} tb2 ON {mergeid_ON_fields} WHERE {mergeid_WHERE_TB1_fields}""".format(mergeid_fields_TB2 = self.mergeid_fields_TB2,
                    common_columns_fields_TB2 = self.common_columns_fields_TB2, table2_columns_fields = self.table2_columns_fields, 
                    table1_columns_fields_nulls = self.table1_columns_fields_nulls, merge_table_1 = self.merge_table_1, merge_table_2 = self.merge_table_2, 
                    mergeid_ON_fields = self.mergeid_ON_fields, mergeid_WHERE_TB1_fields = self.mergeid_WHERE_TB1_fields)
        else:
            qry2 = """select  {keep_columns_string} into SAS_MERGE_2 
            from {merge_table_1} tb1
            right outer join {merge_table_2} tb2 ON {mergeid_ON_fields} WHERE {mergeid_WHERE_TB1_fields}""".format(keep_columns_string=self.keep_columns_string, 
                    table1_columns_fields_nulls = self.table1_columns_fields_nulls, merge_table_1 = self.merge_table_1, merge_table_2 = self.merge_table_2, 
                    mergeid_ON_fields = self.mergeid_ON_fields, mergeid_WHERE_TB1_fields = self.mergeid_WHERE_TB1_fields)
        
        print 'qry2 = ', qry2
        
        dbcontext2 = self.rc.getDBContext(cached = False)
        dbcontext2.executeNoResults(qry2)
        dbcontext2.close()
    
    def _insert_inner_join(self):
        """This query for all matching rows between TABLE_1 & TABLE_2"""
        if not self.keep_columns:
            qry3 = """select  {mergeid_fields_TB1}  {common_columns_fields_TB2}  {table1_columns_fields}  {table2_columns_fields} into SAS_MERGE_3 from {merge_table_1} tb1
            inner join {merge_table_2} tb2 ON {mergeid_ON_fields} """.format(mergeid_fields_TB1 = self.mergeid_fields_TB1,
                    common_columns_fields_TB2 = self.common_columns_fields_TB2, table1_columns_fields = self.table1_columns_fields, 
                    table2_columns_fields = self.table2_columns_fields, merge_table_1 = self.merge_table_1, merge_table_2 = self.merge_table_2, 
                    mergeid_ON_fields = self.mergeid_ON_fields)
        else:
            qry3 = """select  {keep_columns_string} into SAS_MERGE_3 from {merge_table_1} tb1
            inner join {merge_table_2} tb2 ON {mergeid_ON_fields} """.format(keep_columns_string=self.keep_columns_string,
                    merge_table_1 = self.merge_table_1, merge_table_2 = self.merge_table_2, 
                    mergeid_ON_fields = self.mergeid_ON_fields)

        print 'qry3 = ', qry3
        dbcontext3 = self.rc.getDBContext(cached = False)
        dbcontext3.executeNoResults(qry3)
        dbcontext3.close()
        
    
    def _thread_processing(self):
        
        thread1 = Thread(target = self._insert_left_join)
        thread2 = Thread(target = self._insert_right_join)
        thread3 = Thread(target = self._insert_inner_join)
          
        threads = []
        thread1.start()
        thread2.start()
        thread3.start()
          
        threads.append(thread1)
        threads.append(thread2)
        threads.append(thread3)
          
        for t in threads:
            t.join()

        print "All threads are complete"
    
    def _insert_merge_data_into_table(self):
        dbcontext7 = self.rc.getDBContext(cached = False)
        select_cols_qry = """Select name from sys.columns where object_id = OBJECT_ID('{tablename}')""".format(tablename = 'SAS_MERGE_3')
        select_cols = dbcontext7.execute(select_cols_qry)
        print 'select_Cols = ', select_cols
        select_cols = ','.join([each[0].encode('ascii').upper() for each in select_cols])
        
        dbcontext7.close()
        if self.keep_columns:
            merge_qry4 = """ select * into {output_table} from (select {columnnames} from SAS_MERGE_1 union select {columnnames} from SAS_MERGE_2 union select {columnnames} from SAS_MERGE_3) tb1""".format(output_table = self.output_table,
                                                                 columnnames = select_cols)
        else:
            merge_qry4 = """ select * into {output_table} from (select {keep_columns_string} from SAS_MERGE_1 union select {keep_columns_string} from SAS_MERGE_2 union select {keep_columns_string} from SAS_MERGE_3) tb1""".format(output_table = self.output_table,
            columnnames = select_cols,keep_columns_string=self.keep_columns_string)
        print 'merge_qry4 = ', merge_qry4
        dbcontext4 = self.rc.getDBContext(cached = False)
        dbcontext4.executeNoResults(merge_qry4)
        dbcontext4.close()
    
#     def _process_drop_columns(self):
#         print '_process_drop_columns'
#         dbcontext5 = self.rc.getDBContext(cached = False)
#         for each in self.drop_columns:
#             print each
#             DROP_QUERY = "ALTER TABLE {tablename} DROP COLUMN {columnname}".format(tablename = self.output_table, columnname = each)
#             dbcontext5.executeNoResults(DROP_QUERY)
#         dbcontext5.close()
#     
#     def _process_keep_columns(self):
#         print '_process_keep_columns'
#         if len(self.keep_columns) <> 0:
#             dbcontext6 = self.rc.getDBContext(cached = False)
#             all_cols_qry = """Select name from sys.columns where object_id = OBJECT_ID('{tablename}')""".format(tablename = self.output_table)
#             all_cols = dbcontext6.execute(all_cols_qry)
#             all_cols = [each[0].encode('ascii').upper() for each in all_cols]
#             drop_cols   = [each for each in all_cols if each not in self.keep_columns]
#             for each in drop_cols:
#                 DROP_QUERY = "ALTER TABLE {tablename} DROP COLUMN {columnname}".format(tablename = self.output_table, columnname = each)
#                 print DROP_QUERY
#                 dbcontext6.executeNoResults(DROP_QUERY)
#             dbcontext6.close()
#         
if __name__ == '__main__':
    runcontext = SuiteContext('unittest')
    dbcontext = runcontext.getDBContext(cached=False)
    sm = SasMerge(dbcontext = dbcontext,runcontext = runcontext, merge_table_1 = "Employee_merge_1" , merge_table_2 = "Employee_merge_2", mergeids = ["Emp_Num"], drop_columns = [],
                  keep_columns = ['EMP_AGE'] ,output_table = "Merge_Output_table1")
    sm.process()          