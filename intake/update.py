'''
Created on Jul 2, 2013

@author: temp_plakshmanan
'''

from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote
import layoutcheck


_TABLE_NAMES = ['MC_TABLE_W_1','MC_TABLE_C_1','MC_TABLE_S_1','MC_TABLE_R_1','MC_TABLE_M_1']

_ID_COLUMNS = ['ID', 'FLAT_TABLE_ID'] 
_LAYOUTFILE = 'C:\CVS Projects\CSSC Score Reporting\OGT Spring 2012\Input Layout\OGT_SP12_Op_DataLayout_IntakeLayout.xls'

runcontext = RunContext('unittest')
dbcontext = runcontext.getDBContext()

def prepare_update(fieldnames, table):
#     print table
#     print fieldnames
    LC = layoutcheck.LayoutCheck(runcontext=runcontext, dbcontext=dbcontext,
                                                layoutfile=_LAYOUTFILE)
    layoutdict, maxmindict, recodingsdict = LC.process()
    format_list = []
    for each in fieldnames:
        position_value = each + '_1'
#         print 'each=', each
        value = recodingsdict.get(position_value)
#         print value
        i = 0
        temp_list = []
        for k, v in value.items():
#             print k, v
            if i == 0:
                temp_list.append("{fieldname}  = CASE WHEN {fieldname}  = '{key}' THEN '{value}'".format(fieldname=each,key=k,value=v))
            else:
                temp_list.append("WHEN {fieldname}  = '{key}' THEN '{value}'".format(fieldname=each,key=k,value=v))
            
            i += 1
        format_list.append(temp_list)
#         print format_list
    i = 0        
    for each in format_list:
        if i == 0:
            update_stmt = ' '.join(each) + ' END, ' 
        else:
            update_stmt = update_stmt + ' '.join(each) + ' END, '
        i += 1
#     print update_stmt
    update_stmt = update_stmt.rstrip(',')
    print update_stmt 
    
    
for table in _TABLE_NAMES:
    qry="Select name from sys.columns where object_id = OBJECT_ID('{0}') order by column_id".format(table)
    result = dbcontext.execute(qry)
    fieldnames = []
    for each in result:
        if each[0].encode('ascii').upper() in _ID_COLUMNS or each[0].encode('ascii')[0:7].upper() == 'MISSING':
            pass
        else:
            fieldnames.append(each[0].encode('ascii').upper())
    prepare_update(fieldnames, table)
    
            

        
