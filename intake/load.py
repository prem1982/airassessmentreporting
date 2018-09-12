'''
Created on Jun 27, 2013

@author: temp_plakshmanan
'''

from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote
from airassessmentreporting.intake import preqc
import subprocess
import sys


class Load( object ):
    def __init__(self, runcontext, dbcontext, filename = '', outputdir = ''):
        self.filename = filename
        self.db = dbcontext
        self._housekeeping()
        self.outputs_dir = outputdir
        
        self.f2 = open(self.outputs_dir + 'PART002.txt','w')
        
    def _housekeeping(self):
        qry = "CREATE TABLE FILE_CONTROL_TABLE (id int,FILENAME nvarchar( 100) )"
        drop_table_if_exists( 'FILE_CONTROL_TABLE', self.db)
        self.db.executeNoResults(qry)
        print 'Housekeeping done'
        
    def process(self):
#         filecnt = 0
#         for each in open('C:\SAS\OGT\Input\original.txt','r'):
#             filecnt += 1
#             
#         splitcnt = filecnt / 2
#         i = 0
#         for each in open(self.filename):
#             if i <= splitcnt:
#                 with open(self.outputs_dir + 'PART001.txt','w') as self.f1:
#                     self.f1.write(each)
#             else:
#                 self.f2.write(each)
#             i += 1
        print 'File split completed'
        print sys.executable
        proc = subprocess.Popen([sys.executable,'C:\Mercurial projects\lib\airassessmentreporting\intake\preqc.py'],stdout=subprocess.PIPE)
    
if __name__ == '__main__':
    RC = RunContext('unittest')
    dbcontext = RC.getDBContext()
    lc = Load(runcontext = RC, dbcontext = dbcontext, filename='C:\SAS\OGT\Input\original.txt', outputdir = 'c:/SAS/OGT/Joboutput/')
    lc.process()
        