'''
Created on Jun 18, 2013

@author: temp_plakshmanan
'''
from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote
RC = RunContext('unittest')
dbcontext = RC.getDBContext()

flat_tables_columns = []
qry="Select name from sys.columns where object_id = OBJECT_ID('{0}') order by column_id".format('pre_qc_flat_table1')
result = dbcontext.execute(qry)
for item in result: 
        flat_tables_columns.append(item[0].encode('ascii'))
# print 'flat_tables_columns=',flat_tables_columns
a='1&&&RD&&&N&&&0&&&     &&&0&&&390&&&2&&&1&&&A&&&384&&&0&&&3.0&&&     &&&0&&&0&&&1&&&LAURENT CLERC NATIONAL DEAF ED&&&1&&&000001&&&3&&&0030      &&&  &&&A&&&000001&&&1&&&                    &&&5.0&&&N&&& &&&  &&&          &&&2&&&KATRYNA             &&&         &&&372&&&N&&&A&&&          &&&0&&&MA&&&0&&&         &&&002768    &&&21.0&&&428&&&391&&&A  &&&0&&&18.0&&&0&&&         &&&A&&&F&&&A  &&&1&&&P53626600000076 &&&0&&&3&&&0&&&020&&&4.0&&&A&&&                              &&&0&&&5.0&&&0&&&          &&&                    &&&SV&&&          &&&0&&&0&&&     &&&1&&& &&&1&&&A&&&1&&&WR&&&SS&&&0&&&3&&&A&&&020&&&                    &&&A&&&0&&&     &&&141766   &&&                    &&&396&&&4.5&&&A  &&&2&&&020&&&A&&&SV&&&10146239            &&&A&&&0&&&         &&&                    &&&0&&&A&&&4.0&&&A  &&&6.0&&&A&&&0&&&1995&&&miss&&&1&&&A  &&&402&&&1&&&427&&&0&&&LAURENT CLERC NATIONAL DEAF ED&&&371&&&0&&&A&&&A  &&&20.0&&&A&&&1&&&A&&&         &&&1&&&0&&&                    &&&0&&&         &&&30&&&miss&&&6.0&&&397&&&A&&&A&&&     &&&2&&&LAURENT CLERC NATIONAL DEAF ED&&&420&&&0&&&388&&&2012&&&5&&&1&&&          &&&A&&&A  &&&                    &&&3.0&&&     &&&                              &&&00&&&miss&&&1&&&SC&&&1&&&000001&&&2&&&11&&&miss&&&  &&&394&&&3.0&&&  &&&0&&&   &&&0&&&0&&&                    &&&0&&&00&&&miss&&&4.5&&&0&&&0&&&10&&&miss&&&0&&&                    &&&8.0&&&A&&&04&&&418&&&   &&&BALDIVIEZ           &&&2&&&407&&&1&&&3.0&&&383&&&0&&& &&&1&&&0&&&SV&&&                    &&&A&&&      &&&5&&&miss&&&1&&&RD&&&N&&&0&&&     &&&0&&&390&&&2&&&1&&&A&&&384&&&0&&&3.0&&&     &&&0&&&0&&&1&&&LAURENT CLERC NATIONAL DEAF ED&&&1&&&000001&&&3&&&0030      &&&  &&&A&&&000001&&&1&&&                    &&&5.0&&&N&&& &&&  &&&          &&&2&&&KATRYNA             &&&         &&&372&&&N&&&A&&&          &&&0&&&MA&&&0&&&         &&&002768    &&&21.0&&&428&&&391&&&A  &&&0&&&18.0&&&0&&&         &&&A&&&F&&&A  &&&1&&&P53626600000076 &&&0&&&3&&&0&&&020&&&4.0&&&A&&&                              &&&0&&&5.0&&&0&&&          &&&                    &&&SV&&&          &&&0&&&0&&&     &&&1&&& &&&1&&&A&&&1&&&WR&&&SS&&&0&&&3&&&A&&&020&&&                    &&&A&&&0&&&     &&&141766   &&&                    &&&396&&&4.5&&&A  &&&2&&&020&&&A&&&SV&&&10146239            &&&A&&&0&&&         &&&                    &&&0&&&A&&&4.0&&&A  &&&6.0&&&A&&&0&&&1995&&&miss&&&1&&&A  &&&402&&&1&&&427&&&0&&&LAURENT CLERC NATIONAL DEAF ED&&&371&&&0&&&A&&&A  &&&20.0&&&A&&&1&&&A&&&         &&&1&&&0&&&                    &&&0&&&         &&&30&&&miss&&&6.0&&&397&&&A&&&A&&&     &&&2&&&LAURENT CLERC NATIONAL DEAF ED&&&420&&&0&&&388&&&2012&&&5&&&1&&&          &&&A&&&A  &&&                    &&&3.0&&&     &&&                              &&&00&&&miss&&&1&&&SC&&&1&&&000001&&&2&&&11&&&miss&&&  &&&394&&&3.0&&&  &&&0&&&   &&&0&&&0&&&                    &&&0&&&00&&&miss&&&4.5&&&0&&&0&&&10&&&miss&&&0&&&                    &&&8.0&&&A&&&04&&&418&&&   &&&BALDIVIEZ           &&&2&&&407&&&1&&&3.0&&&383&&&0&&& &&&1&&&0&&&SV&&&                    &&&A&&&      &&&5&&&miss'
i = 1
res= a.split('&&&')
for each in zip(flat_tables_columns,res):
    print i
    print each
    i += 1  
    
