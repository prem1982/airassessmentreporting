'''
Created on Aug 28, 2013

@author: temp_plakshmanan
'''
from threading import Thread
import time

from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec

def sql_1(dbcontext):
    print ' dbcontext executing_1 = ', dbcontext
    drop_table_if_exists( 'employee_1_test', dbcontext)
    dbcontext.executeNoResults("select * into employee_1_test from employee1")

def sql_2(dbcontext):
    print ' dbcontext executing_2 = ', dbcontext
    drop_table_if_exists( 'employee_2_test', dbcontext)
    dbcontext.executeNoResults("select * into employee_2_test from employee1")


runcontext = RunContext('unittest')

print 'runcontext = ', runcontext
dbcontext = runcontext.getDBContext(cached=False)
print 'dbcontext = ', dbcontext
thread1 = Thread( target=sql_1, args=(dbcontext, ) )


runcontext1 = RunContext('unittest')
print 'runcontext1 = ', runcontext1
dbcontext1 = runcontext1.getDBContext(cached=False)
 
thread2 = Thread( target=sql_2, args=(dbcontext1, ) )
thread1.start()
thread2.start()