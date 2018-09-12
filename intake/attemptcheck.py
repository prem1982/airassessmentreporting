'''
Created on Jul 8, 2013

@author: temp_plakshmanan
'''


from airassessmentreporting.erasure import BookMapReader
from airassessmentreporting.airutility import *


from airassessmentreporting.testutility import SuiteContext

_SQL = """ SELECT flat_table_id, CALC_ATTEMPT, ATTEMPT_VALUE
        INTO {attempt_check_errors} 
        FROM
            ( SELECT TB5.ID as flat_Table_id, TB5.CALC_ATTEMPT, TB6.ATTEMPT_VALUE 
            FROM   (SELECT TB4.id, 
                           CASE 
                             WHEN Sum(mc_items + oe_items) >= 5 THEN 1 
                             WHEN ISNUMERIC(tb4.upwxraw) = 1 THEN
                                    CASE WHEN cast(tb4.upwxraw as float) >= 5
                                        THEN 1
                                    ELSE 0 
                                    END
                             ELSE 0 
                           END calc_attempt 
                    FROM   (
                    SELECT tb1.id, 
                                   tb3.[item position] AS bookmap_id, 
                                   tb1.{upwxraw} as upwxraw,
                                    tb1.form, 
                                   tb2.{upwx_finalraw_item}, 
                                   CASE 
                                     WHEN tb2.{upwx_finalraw_item} IN ( 
                                          '1', '2', '3', '4', '*' ) 
                                   THEN 1 
                                     ELSE 0 
                                   END                 mc_items, 
                                   tb2.{upwx_oe_final}, 
                                   CASE 
                                     WHEN TB2.{upwx_oe_final} NOT IN ( 'A', 'B', '' ) THEN 1 
                                     ELSE 0 
                                   END                 oe_items, 
                                   tb3.[role], 
                                   tb3.[item format] 
                            FROM   (SELECT x.id,
                                           x.lithocode,
                                           x.{upwxraw}, 
                                           x.{ufwx_attempt}, 
                                           CASE 
                                             WHEN x.{ucwx_form} = 'BR' THEN 'B' 
                                             ELSE 'A' 
                                           END form 
                                    FROM   {flat_table} x) tb1 
                                   INNER JOIN {mc_table} tb2 
                                           ON tb1.id = tb2.flat_table_id 
                                   INNER JOIN ogt_bookmaps tb3 
                                           ON tb2.id = tb3.[item position] 
                                              AND tb3.subject_values = '{subject_value}' 
                                              AND tb1.form = tb3.form_values 
                                   WHERE TB3.[role] = 'OPERATIONAL'
                                   ) TB4 
                    GROUP  BY TB4.id, tb4.upwxraw) TB5 
                   INNER JOIN (SELECT id, 
                                {ufwx_attempt} as attempt_value
                               FROM   {flat_table}) TB6 
                           ON TB5.id = TB6.id 
                              AND TB5.calc_attempt <> tb6.attempt_value )   TB7

"""

_SQL_REPORT = """SELECT TB3.FLAT_TABLE_ID,TB3.ID,tb3.lithocode,TB3.bookmap_id, TB3.{upwxraw}, TB3.FORM, TB3.{upwx_finalraw_item}, TB3.MC_ITEMS,TB3.{upwx_oe_final},TB3.OE_ITEMS ,
TB3.ROLE, TB3.item_format, tb3.{ufwx_attempt} , tb4.ATTEMPT_VALUE, tb4.CALC_ATTEMPT
INTO {attempt_check_errors}
FROM 
                  (         SELECT tb1.id  as flat_table_id,
                                tb2.id,
                                  tb1.lithocode,
                                   tb3.[item position] AS bookmap_id, 
                                   tb1.{upwxraw},
                                    tb1.form, 
                                   tb2.{upwx_finalraw_item}, 
                                   CASE 
                                     WHEN tb2.{upwx_finalraw_item} IN ( 
                                          '1', '2', '3', '4', '*' ) 
                                   THEN 1 
                                     ELSE 0 
                                   END                 mc_items, 
                                   tb2.{upwx_oe_final}, 
                                   CASE 
                                     WHEN TB2.{upwx_oe_final} NOT IN ( 'A', 'B', '' ) THEN 1 
                                     ELSE 0 
                                   END                 oe_items, 
                                   tb3.[role], 
                                   tb3.[item format] as item_format,
                                   tb1.{ufwx_attempt} 
                            FROM   (SELECT x.id,
                                            x.lithocode,
                                           x.{upwxraw}, 
                                           x.{ufwx_attempt}, 
                                           CASE 
                                             WHEN x.{ucwx_form} = 'BR' THEN 'B' 
                                             ELSE 'A' 
                                           END form 
                                    FROM   {flat_table} x) tb1 
                                   INNER JOIN {mc_table} tb2 
                                           ON tb1.id = tb2.flat_table_id 
                                   INNER JOIN ogt_bookmaps tb3 
                                           ON tb2.id = tb3.[item position] 
                                              AND tb3.subject_values = '{subject_value}' 
                                              AND tb1.form = tb3.form_values 
                                   WHERE TB3.[role] = 'OPERATIONAL'
                                   ) TB3 
                        INNER JOIN
                        {attempt_check_error_intr} TB4
                        ON TB3.flat_table_id = TB4.flat_table_id """

subject_col_mapping = { 'W':['upwxraw','upwx_finalraw_item','upwx_oe_final','ufwx_attempt','ucwx_form','attempt_check_error_w']
                       ,'C':['upcxraw','upcx_finalraw_item','upcx_oe_final','ufcx_attempt','uccx_form','attempt_check_error_c']
                       ,'M':['upmxraw','upmx_finalraw_item','upmx_oe_final','ufmx_attempt','ucmx_form','attempt_check_error_m']
                       ,'S':['upsxraw','upsx_finalraw_item','upsx_oe_final','ufsx_attempt','ucsx_form','attempt_check_error_s']
                       ,'R':['uprxraw','uprx_finalraw_item','uprx_oe_final','ufrx_attempt','ucrx_form','attempt_check_error_r']
                       }

class AttemptCheck( object ):
    """ This module will work similar to the attempt check module in SAS. This module will calculate the number of attempts made by the student and compare it with the
    pre-calculated uf[wmcs]x_attempt value. If the values are not same the id's are written to the error_table that is given in the input.
    This module will generate multiple outputs based on the number of outputs given in the Input. If there are any errors, the table names will be displayed in the console.
    There are 2 steps 1) To identify all the mis-matches, which will be written to _intr tables 2) Load the error table with all information needed for debugging."""
    def __init__(self, dbcontext = '', runcontext = '', bookmapfile = ''):
        self.dbcontext = dbcontext
        self.runcontext = runcontext
        self.bookmapfile = bookmapfile
        XLSmaps = BookMapReader( excel='Y',inputfile=self.bookmapfile,inputsheet='BookMap',
                                 read_to_db=True,db_context = self.dbcontext, outputTable = 'ogt_Bookmaps' )
        
        for eachlist in subject_col_mapping.values():
            drop_table_if_exists( eachlist[5], self.dbcontext)
            drop_table_if_exists( eachlist[5] + '_intr', self.dbcontext)
        
        print 'BOOKMAPSLOADED'
        
    def process(self):
        print 'ATTEMPT_CHECK MODULE STARTED'
        try:
            _COUNT_QRY = "select count(*) from {table_name}"
            
            qry = "select tablename from table_names where subject_id = 'F'"
            flat_table = self.dbcontext.execute(qry)
            
            qry = "select subject_id, tablename from table_names where subject_id <> 'F'"
            for each in self.dbcontext.execute(qry):
                cols = subject_col_mapping.get(each[0][0:1])
                qry = _SQL.format(upwxraw = cols[0],upwx_finalraw_item = cols[1],upwx_oe_final = cols[2],ufwx_attempt = cols[3]
                            ,ucwx_form = cols[4],flat_table = flat_table[0][0].encode('ascii'),mc_table = each[1].encode('ascii'), subject_value = each[0].encode('ascii')
                            ,attempt_check_errors = cols[5] + '_intr')
                
                result = self.dbcontext.executeNoResults(qry)
                
                qry = _COUNT_QRY.format(table_name=cols[5] + '_intr')
                result = self.dbcontext.execute(qry)
                if result[0][0] >  0:
                    qry = _SQL_REPORT.format(upwxraw = cols[0],upwx_finalraw_item = cols[1],upwx_oe_final = cols[2],ufwx_attempt = cols[3]
                            ,ucwx_form = cols[4],flat_table = flat_table[0][0].encode('ascii'),mc_table = each[1].encode('ascii'), subject_value = each[0].encode('ascii')
                            ,attempt_check_errors = cols[5] ,attempt_check_error_intr = cols[5] + '_intr')
                
                    result = self.dbcontext.executeNoResults(qry)
                    print 'Check the following table for errors - {0}'.format(cols[5])
    
            print 'ATTEMPT_CHECK MODULE ENDED'
        except Exception as error:
                print 'Error=',error
 
if __name__ == '__main__':
    runcontext = SuiteContext('unittest')
    dbcontext = runcontext.getDBContext()
    ac = AttemptCheck(dbcontext = dbcontext,runcontext = runcontext,
                      bookmapfile = """C:\SAS\OGT\Input\Bookmaplocations1.xls""")
    #bookmaplocations2 is the bookmapfile for the OGT Fall 2012
    ac.process()

            