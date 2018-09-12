'''
Created on Aug 13, 2013

@author: temp_plakshmanan
'''

from airassessmentreporting.erasure import BookMapReader
from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from multiprocessing import Process
from threading import Thread
import sasmerge
import time
import timeit


sqls = {'DISTRICTNAMES':"""SELECT TB1.DCRXID AS DCRXID, TB1.DCRXNM  INTO DISTRICTNAMES_ASCII FROM 
                            (SELECT  DCRXID, DCRXNM FROM DISTRICT
                            UNION
                            SELECT  DCRXID, DCRXNM FROM STUDENT ) TB1
                            ORDER BY DCRXID
                            """,
        'STATE':"""SELECT * INTO STATE_ASCII FROM 
                    (SELECT CASE WHEN GRADE = 99 THEN  NULL
                            ELSE GRADE
                            END GRADE_RECODED,
                            CASE WHEN RGRADE = 99 
                            THEN  NULL
                            ELSE RGRADE
                        END GRADE_R_RECODED, *, GRADE AS SORTING_GRADE
                    FROM STATE) TB1
                            """,
        'SCHOOL_REPNUM':"""SELECT CASE WHEN REPORTNUM = 10
                                        THEN
                                            CAST(REPORTNUM AS VARCHAR) 
                                        ELSE
                                            '0' + CAST(REPORTNUM AS VARCHAR) 
                                        END AS REPORTNUM_S
                        ,CASE WHEN REPORTNUM = 10
                                    THEN  RTRIM(LTRIM(G_DCRXID)) + '_' + CAST(REPORTNUM AS VARCHAR) 
                                    ELSE
                                        RTRIM(LTRIM(G_DCRXID)) + '_' + '0' + CAST(REPORTNUM AS VARCHAR) 
                                    END G_DCRXID_N
                        ,TB2.*, GRADE AS SORTING_GRADE, SUBSTRING(G_DCRXID,4,6) AS DCRXID
                    INTO SCHOOL_REPNUM_ASCII
                    FROM
                    (SELECT    CASE WHEN COUNT >= 1 AND COUNT <= 5   THEN 1
                                 WHEN COUNT >= 6 AND COUNT <= 10  THEN 2
                                 WHEN COUNT >= 11 AND COUNT <= 15 THEN 3
                                 WHEN COUNT >= 16 AND COUNT <= 20 THEN 4
                                 WHEN COUNT >= 21 AND COUNT <= 25 THEN 5
                                 WHEN COUNT >= 26 AND COUNT <= 30 THEN 6
                                 WHEN COUNT >= 31 AND COUNT <= 35 THEN 7
                                 WHEN COUNT >= 36 AND COUNT <= 40 THEN 8
                                 WHEN COUNT >= 41 AND COUNT <= 45 THEN 9
                                 ELSE 10
                            END REPORTNUM, TB1.*,
                            CASE WHEN TB1.GRADE = 99 
                            THEN  NULL
                            ELSE TB1.GRADE
                            END GRADE_RECODED,
                            CASE WHEN TB1.RGRADE = 99 
                            THEN  NULL
                            ELSE TB1.RGRADE
                            END GRADE_R_RECODED
                            
                    FROM 
                    (SELECT ROW_NUMBER() OVER(PARTITION BY G_DCRXID ORDER BY G_BCRXID) AS COUNT, *
                    FROM SCHOOL) TB1
                    ) TB2
                    """,
            'SCHOOL':"""SELECT  * INTO SCHOOL_ASCII FROM 
                    (SELECT SUBSTRING(G_DCRXID,4,6)  AS DCRXID
                            ,CASE WHEN GRADE = 99 
                                    THEN  NULL
                                    ELSE GRADE
                                    END GRADE_RECODED
                            ,CASE WHEN RGRADE = 99 
                                THEN  NULL
                                ELSE RGRADE
                                END GRADE_R_RECODED
                            , * , GRADE AS SORTING_GRADE
                            FROM SCHOOL) TB1
                            ORDER BY TB1.G_DCRXID, G_BCRXID
                            """,
        'SCHOOL_INTERVENTION':"""SELECT DCRXID_ATTEND AS DCRXID_RECODED, * INTO SCHOOL_INTERVENTION_ASCII FROM (SELECT * FROM SCHOOL_INTERVENTION) TB1""",
        'ALLDISTRICTS':""" SELECT TB1.DCRXID AS DCRXID, TB1.DCRXID AS MERGEID  INTO ALLDISTRICTS_ASCII FROM 
                            (SELECT  DCRXID, DCRXNM FROM DISTRICT
                            UNION
                            SELECT  DCRXID, DCRXNM FROM DISTRICT ) TB1
                            ORDER BY DCRXID""",
        'SCHOOL_INTERVENTION_JD':"""SELECT * INTO SCHOOL_INTERVENTION_JD_ASCII FROM SCHOOL_INTERVENTION_ASCII WHERE schtype in ('J','D','Y')""",
        'ALLSCHOOLS':"""SELECT DCRXID, BCRXID INTO ALLSCHOOLS_ASCII FROM SCHOOL_ASCII GROUP BY DCRXID, BCRXID""",
        'ALLSCHOOLS_JD':"""SELECT TB1.BCRXID, SCHTYPE INTO ALLSCHOOLS_JD_ASCII FROM (SELECT BCRXID, SCHTYPE  FROM SCHOOL GROUP BY BCRXID, SCHTYPE) TB1 WHERE TB1.SCHTYPE IN ('J','D','Y') ORDER BY TB1.BCRXID""",
        'DISTRICT':"""SELECT GRADE AS SORTING_GRADE
                        , CASE WHEN GRADE = 99 
                            THEN  NULL
                            ELSE GRADE
                            END GRADE_RECODED,
                            CASE WHEN RGRADE = 99 
                            THEN  NULL
                            ELSE RGRADE
                            END GRADE_R_RECODED
                    ,* INTO DISTRICT_ASCII FROM DISTRICT
                    ORDER BY G_DCRXID""",
        'STUDENT':"""SELECT  CASE WHEN GRADE = 99 
                        THEN  NULL
                        ELSE GRADE
                        END GRADE_RECODED
                        ,CASE WHEN RGRADE = 99 
                        THEN  NULL
                        ELSE RGRADE
                        END GRADE_R_RECODED
                        ,CASE WHEN UFRX_BREACH = 1 
                            THEN NULL 
                            ELSE UFRX_BREACH
                            END UFRX_BREACH_RECODED
                        ,CASE WHEN UFMX_BREACH = 1 
                            THEN NULL 
                            ELSE UFMX_BREACH
                            END UFMX_BREACH_RECODED
                        ,CASE WHEN UFWX_BREACH = 1 
                            THEN NULL 
                            ELSE UFWX_BREACH
                            END UFWX_BREACH_RECODED
                        ,CASE WHEN UFSX_BREACH = 1 
                            THEN NULL 
                            ELSE UFSX_BREACH
                            END UFSX_BREACH_RECODED
                        ,CASE WHEN UFCX_BREACH = 1 
                            THEN NULL 
                            ELSE UFCX_BREACH
                            END UFCX_BREACH_RECODED
                        ,DCRXID + BCRXID AS LABELID
                        ,0 AS LATEBATCHFLAG
                        ,*
                        INTO STUDENT_ASCII
                FROM STUDENT""",
                    'ALL_DISTRICTS':"""SELECT DCRXID, MERGEID FROM 
                (SELECT DCRXID, ' ' AS MERGEID FROM STUDENT_ASCII
                WHERE SCHTYPE = 'H'
                GROUP BY DCRXID  
                UNION 
                SELECT DCRXID, MERGEID FROM ALLDISTRICTS_ASCII) TB1
                ORDER BY DCRIXID
                """,
        'STUDENTHOMEDISTRICT':"""select CASE WHEN DCRXID <> DCRXID_HOME AND DCRXID_HOME <> ' ' AND SCHTYPE <> 'Y'
                    THEN DCRXID_HOME 
                    ELSE DCRXID
                    END DCRXID,
                CASE WHEN DCRXID <> DCRXID_HOME AND DCRXID_HOME <> ' ' AND SCHTYPE <> 'Y'
                    THEN DCRXNM_HOME 
                    ELSE DCRXNM_HOME 
                    END DCRXNM,
                CASE WHEN DCRXID <> DCRXID_HOME AND DCRXID_HOME <> ' ' AND SCHTYPE <> 'Y'
                    THEN SORTING_GRADE + '_' + DCRXID_HOME 
                    ELSE G_DCRXID
                    END G_DCRXID,
                    *
        from student_ascii""",
        
        
       }


    
class AsciiWriter:
    def __init__(self, dbcontext, runcontext):
        print 'Ascii writer created'
        self.dbcontext = dbcontext
        self.runcontext = runcontext
        
    def process(self):
        #self._delete_temp_tables()
        
        process1 = Process(target = populate_districtnames(self.dbcontext))
        process2 = Process(target = populate_state(self.dbcontext))
        process3 = Process(target = populate_school_repnum(self.dbcontext))
        process4 = Process(target = populate_school_intervention(self.dbcontext))
        processes = []
        process1.start();process2.start();process3.start();process4.start()
        processes.append(process1);processes.append(process2);processes.append(process3);processes.append(process4)
        for p in processes:p.join()
        #The above 3 process can be submitted parallel without any lock contention 
        populate_school(self.dbcontext)
        
        
#         self._thread_processing()
        
    
#     def _delete_temp_tables(self):
#         for i in range(50):
#             query = "DROP TABLE temp_ascii_{tableversion}".format(tableversion=i)
#             self.dbcontext.executeNoResults( query )
        
#     def _thread_processing(self):
#         pass
        
        
#         process1 = Process(target = self._processdistrictnames())
#         #thread2 = Thread(target = self._insert_right_join)
#         #thread3 = Thread(target = self._insert_inner_join)
#           
#         processes = []
#         process1.start()
#         #thread2.start()
#         #thread3.start()
#           
#         processes.append(process1)
#         #threads.append(thread2)
#         #threads.append(thread3)
#           
#         for p in processes:
#             p.join()
# 
#         print "All process are complete"
    

def time_taken(func):
    def inner(dbcontext):
        starttime = time.time()
        func(dbcontext)
        endtime = time.time()
        print 'Time Taken = ', endtime - starttime
        print 80 * " "
    return inner
    
@time_taken
def populate_districtnames(dbcontext):
    print "Create DISTRICTNAMES_ASCII"
    drop_table_if_exists( 'DISTRICTNAMES_ASCII', dbcontext)
    dbcontext.executeNoResults(sqls['DISTRICTNAMES'])
@time_taken
def populate_state(dbcontext):
    print "Create STATE_ASCII"
    drop_table_if_exists( 'STATE_ASCII', dbcontext)
    dbcontext.executeNoResults(sqls['STATE'])
    dbcontext.executeNoResults("ALTER TABLE STATE_ASCII DROP COLUMN GRADE, RGRADE")
    dbcontext.executeNoResults("sp_RENAME 'STATE_ASCII.GRADE_RECODED','GRADE','COLUMN'")
    dbcontext.executeNoResults("sp_RENAME 'STATE_ASCII.GRADE_R_RECODED','RGRADE','COLUMN'")
@time_taken    
def populate_school_repnum(dbcontext):
    print "Create SCHOOL_REPNUM_ASCII"
    
    drop_table_if_exists( 'SCHOOL_REPNUM_ASCII', dbcontext)
    dbcontext.executeNoResults(sqls['SCHOOL_REPNUM'])
    dbcontext.executeNoResults("ALTER TABLE SCHOOL_REPNUM_ASCII DROP COLUMN GRADE, RGRADE")
    dbcontext.executeNoResults("sp_RENAME 'SCHOOL_REPNUM_ASCII.GRADE_RECODED','GRADE','COLUMN'")
    dbcontext.executeNoResults("sp_RENAME 'SCHOOL_REPNUM_ASCII.GRADE_R_RECODED','RGRADE','COLUMN'")    
@time_taken
def populate_school(dbcontext):
    print " Create SCHOOL_ASCII"
    drop_table_if_exists( 'SCHOOL_ASCII', dbcontext)
    dbcontext.executeNoResults(sqls['SCHOOL'])
    dbcontext.executeNoResults("ALTER TABLE SCHOOL_ASCII DROP COLUMN GRADE, RGRADE")
    dbcontext.executeNoResults("sp_RENAME 'SCHOOL_ASCII.GRADE_RECODED','GRADE','COLUMN'")
    dbcontext.executeNoResults("sp_RENAME 'SCHOOL_ASCII.GRADE_R_RECODED','RGRADE','COLUMN'")
@time_taken
def populate_school_intervention(dbcontext):
        print " Create SCHOOL_INTERVENTION_ASCII"
        drop_table_if_exists( 'SCHOOL_INTERVENTION_ASCII', dbcontext)
        dbcontext.executeNoResults(sqls['SCHOOL_INTERVENTION'])
        dbcontext.executeNoResults("ALTER TABLE SCHOOL_INTERVENTION_ASCII DROP COLUMN DCRXID")
        dbcontext.executeNoResults("sp_RENAME 'SCHOOL_INTERVENTION_ASCII.DCRXID_RECODED','DCRXID','COLUMN'")
        
if __name__ == '__main__':
    runcontext = SuiteContext('sharedtest')
    dbcontext = runcontext.getDBContext()
    filename_additional_label = 'C:\Projects\OGT_S12\OGT Spring 2012 Addresses & Labels 4_24_2012.xlsx'
    filename_size_selection = 'C:\Projects\OGT_S12\OGT Spring 2012 Addresses & Labels 4_24_2012.xlsx'
    starttime = time.time()
    #ac = AsciiWriter(dbcontext = dbcontext,runcontext = runcontext, filename_additional_label = filename_additional_label, filename_size_selection = filename_size_selection)
    
    ac = AsciiWriter(dbcontext = dbcontext,runcontext = runcontext)
    ac.process()
    endtime = time.time()
    print 'TOTAL TIME TAKEN = ', endtime - starttime
    