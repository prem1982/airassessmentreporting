'''
Created on Aug 13, 2013

@author: temp_plakshmanan
'''

from airassessmentreporting.erasure import BookMapReader
from airassessmentreporting.airutility import *
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec

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
                        END GRADE_R_RECODED, *
                    FROM STATE) TB1
                            """,
        'SCHOOL_REPNUM':"""SELECT CAST(REPORTNUM AS VARCHAR)    AS REPORTNUM_S
                        ,RTRIM(LTRIM(G_DCRXID)) + '_' + CAST(REPORTNUM AS VARCHAR) G_DCRXID_N
                        ,TB2.*
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
                    (SELECT ROW_NUMBER() OVER(PARTITION BY G_DCRXID ORDER BY G_DCRXID) AS COUNT, *
                    FROM SCHOOL) TB1
                    ) TB2
                    """,
        'SCHOOL':"""SELECT  * INTO SCHOOL_ASCII FROM 
                    (SELECT SUBSTRING(G_DCRXID,5,6)  AS DCRXID
                            ,CASE WHEN GRADE = 99 
                                    THEN  NULL
                                    ELSE GRADE
                                    END GRADE_RECODED
                            ,CASE WHEN RGRADE = 99 
                                THEN  NULL
                                ELSE RGRADE
                                END GRADE_R_RECODED
                            , *
                            FROM SCHOOL) TB1
                            """,
        'SCHOOL_INTERVENTION':"""SELECT DCRXID_ATTEND AS DCRXID_RECODED, * INTO SCHOOL_INTERVENTION_ASCII FROM (SELECT * FROM SCHOOL_INTERVENTION) TB1""",
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


class AsciiWriter( object ):
    def __init__(self, dbcontext = "", runcontext = "", filename_additional_label = "", filename_size_selection = ""):
        self.db = dbcontext
        self.RC = runcontext
        self.table_name = " "
        self.filename_additional_label = filename_additional_label
        self.filename_size_selection = filename_size_selection
    
    def process(self):
        try:
            self._districtnames()
#             self._state()
#             self._school_repnum()
#             self._school()
#             self._school_intervention()
#             self._school_intervention_jd()
#             self._allschools()
#             self._allschools_jd()
#             self._district_repnum()
#             self._district()
#             self._student()
# #             self._studenthomedistrict()
#             self._create_repeat_raw()
#             self._label_defaults()
#             self._largerrepeats()
#             self._smallrepeats()
            
        except Exception as error:
            print "Error while execting method for table - {0}".format(self.table_name)
            print "ERROR = ", error
        
    def _districtnames(self):
        print " Create DISTRICTNAMES_ASCII"
        self._alldistricts()
        self.table_name = 'DISTRICTNAMES_ASCII'
        drop_table_if_exists( 'DISTRICTNAMES_ASCII', self.db)
        self.db.executeNoResults(sqls['DISTRICTNAMES'])
    
    def _alldistricts(self):
        """ This is a temporary table referred while creating the alldistricts table"""
        print " Create ALLDISTRICTS_ASCII"
        self.table_name = 'ALLDISTRICTS_ASCII'
        drop_table_if_exists( 'ALLDISTRICTS_ASCII', self.db)
        self.db.executeNoResults(sqls['ALLDISTRICTS'])
    
    def _state(self):
        print " Create STATE_ASCII"
        self.table_name = 'STATE_ASCII'
        drop_table_if_exists( 'STATE_ASCII', self.db)
        self.db.executeNoResults(sqls['STATE'])
        self.db.executeNoResults("ALTER TABLE STATE_ASCII DROP COLUMN GRADE, RGRADE")
        self.db.executeNoResults("sp_RENAME 'STATE_ASCII.GRADE_RECODED','GRADE','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'STATE_ASCII.GRADE_R_RECODED','GRADE_R','COLUMN'")
        
    def _school_repnum(self):
        print " Create SCHOOL_REPNUM_ASCII"
        self.table_name = 'SCHOOL_REPNUM_ASCII'
        drop_table_if_exists( 'SCHOOL_REPNUM_ASCII', self.db)
        self.db.executeNoResults(sqls['SCHOOL_REPNUM'])
        self.db.executeNoResults("ALTER TABLE SCHOOL_REPNUM_ASCII DROP COLUMN GRADE, RGRADE")
        self.db.executeNoResults("sp_RENAME 'SCHOOL_REPNUM_ASCII.GRADE_RECODED','GRADE','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'SCHOOL_REPNUM_ASCII.GRADE_R_RECODED','GRADE_R','COLUMN'")
    
    def _school(self):
        print " Create SCHOOL_ASCII"
        self.table_name = 'SCHOOL_ASCII'
        drop_table_if_exists( 'SCHOOL_ASCII', self.db)
        self.db.executeNoResults(sqls['SCHOOL'])
        self.db.executeNoResults("ALTER TABLE SCHOOL_ASCII DROP COLUMN GRADE, RGRADE")
        self.db.executeNoResults("sp_RENAME 'SCHOOL_ASCII.GRADE_RECODED','GRADE','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'SCHOOL_ASCII.GRADE_R_RECODED','GRADE_R','COLUMN'")
    
    def _school_intervention(self):
        print " Create SCHOOL_INTERVENTION_ASCII"
        self.table_name = 'SCHOOL_INTERVENTION_ASCII'
        drop_table_if_exists( 'SCHOOL_INTERVENTION_ASCII', self.db)
        self.db.executeNoResults(sqls['SCHOOL_INTERVENTION'])
        self.db.executeNoResults("ALTER TABLE SCHOOL_INTERVENTION_ASCII DROP COLUMN DCRXID")
        self.db.executeNoResults("sp_RENAME 'SCHOOL_INTERVENTION_ASCII.DCRXID_RECODED','DCRXID','COLUMN'")
    
    def _school_intervention_jd(self):
        print " Create SCHOOL_INTERVENTION_JD_ASCII"
        self.table_name = 'SCHOOL_INTERVENTION_JD_ASCII'
        drop_table_if_exists( 'SCHOOL_INTERVENTION_JD_ASCII', self.db)
        self.db.executeNoResults(sqls['SCHOOL_INTERVENTION_JD'])
        
    
    def _allschools(self):
        print " Create ALLSCHOOLS_ASCII"
        self.table_name = 'ALLSCHOOLS_ASCII'
        drop_table_if_exists( 'ALLSCHOOLS_ASCII', self.db)
        self.db.executeNoResults(sqls['ALLSCHOOLS'])
        
    def _allschools_jd(self):
        print " Create ALLSCHOOLS_JD_ASCII"
        self.table_name = 'ALLSCHOOLS_JD_ASCII'
        drop_table_if_exists( 'ALLSCHOOLS_JD_ASCII', self.db)
        self.db.executeNoResults(sqls['ALLSCHOOLS_JD'])
    
    def _district_repnum(self):
        print " Create DISTRICT_REPNUM_ASCII"
        self.table_name = 'DISTRICT_REPNUM_ASCII'
        pass
    
    def _district(self):
        print " Create DISTRICT_ASCII"
        self.table_name = 'DISTRICT_ASCII'
        drop_table_if_exists( 'DISTRICT_ASCII', self.db)
        self.db.executeNoResults(sqls['DISTRICT'])
        self.db.executeNoResults("ALTER TABLE DISTRICT_ASCII DROP COLUMN GRADE, RGRADE")
        self.db.executeNoResults("sp_RENAME 'DISTRICT_ASCII.GRADE_RECODED','GRADE','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'DISTRICT_ASCII.GRADE_R_RECODED','GRADE_R','COLUMN'")
    
    def _student(self):
        print " Create STUDENT_ASCII"
        self.table_name = 'STUDENT_ASCII'
        drop_table_if_exists( 'STUDENT_ASCII', self.db)
        self.db.executeNoResults(sqls['STUDENT'])
        self.db.executeNoResults("ALTER TABLE STUDENT_ASCII DROP COLUMN GRADE, RGRADE, UFRX_BREACH, UFSX_BREACH, UFWX_BREACH, UFCX_BREACH, UFMX_BREACH")
        self.db.executeNoResults("sp_RENAME 'STUDENT_ASCII.GRADE_RECODED','GRADE','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'STUDENT_ASCII.GRADE_R_RECODED','GRADE_R','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'STUDENT_ASCII.UFRX_BREACH_RECODED','UFRX_BREACH','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'STUDENT_ASCII.UFMX_BREACH_RECODED','UFMX_BREACH','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'STUDENT_ASCII.UFWX_BREACH_RECODED','UFWX_BREACH','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'STUDENT_ASCII.UFSX_BREACH_RECODED','UFSX_BREACH','COLUMN'")
        self.db.executeNoResults("sp_RENAME 'STUDENT_ASCII.UFCX_BREACH_RECODED','UFCX_BREACH','COLUMN'")
        
#         self.db.executeNoResults("""ALTER TABLE STUDENT_ASCII DROP COLUMN UPRX_SECOND, UPRX_RAW, UPRX_OE, UPRX_SCORE, UPRX_FINALRAW,  
#         UPMX_SECOND, UPMX_RAW, UPMX_OE, UPMX_SCORE, UPMX_FINALRAW,  
#          UPSX_SECOND, UPSX_RAW, UPSX_OE, UPSX_SCORE, UPSX_FINALRAW,  
#          UPWX_SECOND, UPWX_RAW, UPWX_OE, UPWX_SCORE, UPWX_FINALRAW,
#          UPCMDUM, UPCEDUM, UPCSDUM, UPCHDUM""")

    def _studenthomedistrict(self):
        print " Create STUDENTHOMEDISTRICT_ASCII"
        self.table_name = 'STUDENTHOMEDISTRICT_ASCII'
        drop_table_if_exists( 'STUDENTHOMEDISTRICT_ASCII', self.db)
        self.db.executeNoResults(sqls['STUDENTHOMEDISTRICT'])
        
    def _create_repeat_raw(self):
        print " Create REPEAT_ROWS"
        self.sheet_name = "Additional Labels"
        self.output_table = "repeats_raw"
        SE = SafeExcelReader(self.RC, self.filename_additional_label, self.sheet_name , self.output_table , db_context=self.db,get_names=True, delimiter=',', import_order='import_order' , scan_all=True)
        SE.createTable()
    
    def _label_defaults(self):
        print " Create LABEL_DEFAULTS"
        SE = SafeExcelReader(self.RC, self.filename_size_selection, self.sheet_name, self.output_table, db_context=self.dbcontext,get_names=True, delimiter=',', import_order='import_order' , scan_all=True)
        SE.createTable()
    
        
if __name__ == '__main__':
    runcontext = SuiteContext('sharedtest')
    dbcontext = runcontext.getDBContext()
    filename_additional_label = 'C:\Projects\OGT_S12\OGT Spring 2012 Addresses & Labels 4_24_2012.xlsx'
    filename_size_selection = 'C:\Projects\OGT_S12\OGT Spring 2012 Addresses & Labels 4_24_2012.xlsx'
    ac = AsciiWriter(dbcontext = dbcontext,runcontext = runcontext, filename_additional_label = filename_additional_label, filename_size_selection = filename_size_selection)
    ac.process()  