import glob
import os
import datetime
from multiprocessing import Queue, Process
from airassessmentreporting.datacheck.dataset import *
from airassessmentreporting.merge import *
from airassessmentreporting.airutility.safeexcelread import SafeExcelReader
from airassessmentreporting.airutility import (RunContext, get_table_spec, Joiner)
from airassessmentreporting.airutility.dbutilities import drop_tables
from airassessmentreporting.airutility.sqlutils import (nodupkey, nodupkey2)

def readTideFile(q, idx, tideDataDirectory, runContextName):
    start = datetime.datetime.now()
    tideDataFile = q.get()
    tideDataFile = tideDataDirectory + tideDataFile
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    server=dbContext.server
    db=dbContext.db
    lengths = dbContext.execute( "SELECT length FROM TideLayout") 
    var_names = dbContext.execute( "SELECT variable_name FROM TideLayout") 
    lengths = [l[0] for l in lengths]
    var_names = [v[0] for v in var_names]
    tideData = Dataset(dbms='fixed2', lengths=lengths, fieldnames=var_names, open_mode='rb',name=tideDataFile)
    tideSqlTable = Dataset(dbms='pyodbc', table='TideFile'+str(idx), replace=True, 
                           db=db,
                           server=server,
                           open_mode='wb')
    data(tideData,tideSqlTable,unsafecopy=True)
    print "thread: " + str(idx) + " ran: " + str(datetime.datetime.now() - start)
    #print "end of file: " + str(idx) + " " + str(datetime.datetime.now())

if __name__ == '__main__':
    def runRC(version, tideLayoutFile, tideSpecFile, tideDataDirectory, tideDataTestCopy, tideDataOutputFile, masterListDTN, dbContext, runContextName):
    
        testImport = 1
        testMerge = 1
        testOutput = 1
        rcOutputDTN = 'rc%sFinal' % str(version)
        
        if testImport:
            drop_tables(['TideLayout', 'TideFile', 'TideFile0', 'TideFile1', 'TideFile2', 'TideFile3', 'TideFile4', 'TideFile5'], dbContext)
            reader = SafeExcelReader( runContext )
            reader.db_context = dbContext
            reader.filename = tideLayoutFile
            reader.sheetName = "OH Ach Pre-Reporting"
            reader.outputTable = "TideLayout"
            reader.createTable()
        
            files = []
            os.chdir(tideDataDirectory)
            for tideDataFile in glob.glob("*.txt"):
                files.append(tideDataFile)
            q = Queue()
            [q.put(x) for x in files]
            ps = [Process(target=readTideFile, args=(q, i, tideDataDirectory, runContextName)) for i,x in enumerate(files)]
            [p.start() for p in ps]
            [p.join() for p in ps]
            
            i = len(ps)
            query = "SELECT * INTO TideFile FROM ("
            query2 = "" 
            #We created a table for every text file. Now, let's merge them together
            while i > 0:
                i -= 1
                query += "SELECT * FROM TideFile" + str(i) + " "
                if (i > 0):
                    query += " UNION ALL "
                else:
                    query += ") as tmp;\n"
                query2 += " DROP TABLE TideFile%s; " % (i,)
            query2 += " ALTER TABLE tidefile ALTER COLUMN bcrxnm_attend NVARCHAR(35); "
            dbContext.executeNoResults( query ) 
            dbContext.executeNoResults( query2 ) 
            query = """
            UPDATE TideFile SET ucrxgen=1 WHERE ucrxgen='F';
            UPDATE TideFile SET ucrxgen=2 WHERE ucrxgen='M';
            UPDATE TideFile SET migrant='' WHERE ucrxgen='0';
            UPDATE TideFile SET dcrxid_home='', bcrxid_home='', dcrxnm_home='', bcrxnm_home='' WHERE dcrxid_home=dcrxid_attend;
            UPDATE TideFile SET bcrxid_home='', bcrxnm_home='' WHERE bcrxid_attend=bcrxid_home;
            UPDATE TideFile SET dcrxid_home='', bcrxid_home='', dcrxnm_home='', bcrxnm_home='' WHERE dcrxid_home=bcrxid_attend;
            ALTER TABLE TideFile DROP COLUMN filler0, filler1, filler2, filler3, filler4, filler5, filler6, filler7, drops1, drops2, drops3, preid_num1, preid_num2;
            
            UPDATE TideFile SET grade=NULL WHERE grade='.'
            UPDATE TideFile SET dob_day=NULL WHERE dob_day='.'
            UPDATE TideFile SET dob_month=NULL WHERE dob_month='.'
            UPDATE TideFile SET dob_year=NULL WHERE dob_year='.'
            """
            dbContext.executeNoResults( query ) 
            if version == 1:
                dbContext.executeNoResults( " ALTER TABLE TideFile ALTER COLUMN ucrx_preid NVARCHAR(128) NOT NULL; " ) 
                dbContext.executeNoResults( " ALTER TABLE TideFile ADD PRIMARY KEY (ucrx_preid); " ) 
            else:
                dbContext.executeNoResults( " ALTER TABLE TideFile ALTER COLUMN lithocode NVARCHAR(128) NOT NULL; " ) 
                dbContext.executeNoResults( " ALTER TABLE TideFile ADD PRIMARY KEY (lithocode); " ) 
        
        if testMerge:
            tables = ['ds1_rm1_RC1', 'ds2_rm2_RC1', 'fuzzy_RC1', 'merge1_RC1', 'OGT_ScoreFile']
            drop_tables(tables, dbContext)
            
            query = """
            SELECT * INTO OGT_ScoreFile FROM intakeFinal;
            UPDATE OGT_ScoreFile SET dcrxid_home='', bcrxid_home='', dcrxnm_home='', bcrxnm_home='', distrtype_home='', schtype_home='' WHERE dcrxid_home=dcrxid_attend;
            UPDATE OGT_ScoreFile SET bcrxid_home='', bcrxnm_home='', schtype_home='' WHERE bcrxid_attend=bcrxid_home;
            UPDATE OGT_ScoreFile SET dcrxid_home='', bcrxid_home='', dcrxnm_home='', bcrxnm_home='', distrtype_home='', schtype_home='' WHERE dcrxid_home=bcrxid_attend;
            UPDATE OGT_ScoreFile SET dcrxnm_home='', distrtype_home='' WHERE dcrxid_home='';
            ALTER TABLE OGT_ScoreFile ALTER COLUMN id INT NOT NULL;
            ALTER TABLE OGT_ScoreFile ADD PRIMARY KEY (id);
            """
            dbContext.executeNoResults( query ) 
            
            ######## MERGE ########
            merge_def = MergeDef( dbContext )
            tideFile = dbContext.getTableSpec( "TideFile" )
            scoreFile = dbContext.getTableSpec( "OGT_ScoreFile" )
            merge_def.table_name = 'merge1_RC1'
            merge_def.left_input_table = scoreFile
            merge_def.right_input_table = tideFile
            merge_def.allow_dups_both = ( False, ) 
            merge_def.join_type = JOIN_TYPE_INNER
            merge_def.fuzzy_report_table = 'fuzzy_RC1'
            merge_def.left_remain_table = 'ds1_rm1_RC1'
            merge_def.right_remain_table = 'ds2_rm2_RC1'
            read_spec_file( tideSpecFile, merge_def )
            merge_def.execute()
        
        
        tables = ['dcrxid_home', 'bcrxid_all', 'dcrxid_all', 'blanklist', 
                  'dcrxid_missing', rcOutputDTN, 'countyNameFmt', 'OGT_RC1_3', 'OGT_RC1_2', 
                  'OGT_RC1_1', 'OGT_P2', 'OGT_P1', 'merge_master1', 'dedup_d2', 'dedup_b', 
                  'dedup_d', 'finalResult_RC1', 'OGT_Tide']
        drop_tables(tables, dbContext)
        query = """
        ALTER TABLE merge1_RC1 DROP COLUMN primary1, fk_right_1, fk_left_1;
        ALTER TABLE ds1_rm1_RC1 DROP COLUMN merge_report, fk_left_1;
        """
        dbContext.executeNoResults( query ) 
        query = """
        
        SELECT *
        INTO finalResult_RC1
        FROM (
        SELECT * FROM merge1_RC1
        UNION ALL
        SELECT * FROM ds1_rm1_RC1
        ) AS tmp;
        
        UPDATE finalResult_RC1 SET dcrxid_home='052555' WHERE dcrxid_home='053165';
        UPDATE finalResult_RC1 SET dcrxid_home='052555' WHERE dcrxid_home='053645';
        UPDATE finalResult_RC1 SET dcrxid_home='052514' WHERE dcrxid_home='052647';
        UPDATE finalResult_RC1 SET dcrxid_home='052548' WHERE dcrxid_home='053652';
        UPDATE finalResult_RC1 SET dcrxid_home='052530' WHERE dcrxid_home='053231';
        UPDATE finalResult_RC1 SET dcrxid_home='000129' WHERE dcrxid_home='092247';
        UPDATE finalResult_RC1 SET dcrxid_home='000129' WHERE dcrxid_home='064915';
        UPDATE finalResult_RC1 SET dcrxid_home='052514' WHERE dcrxid_home='053454';
        UPDATE finalResult_RC1 SET dcrxid_home='052563' WHERE dcrxid_home='053637';
        UPDATE finalResult_RC1 SET dcrxid_home='052514' WHERE dcrxid_home='052878';
        UPDATE finalResult_RC1 SET dcrxid_home='000129' WHERE dcrxid_home='090456';
        UPDATE finalResult_RC1 SET dcrxid_home=''       WHERE dcrxid_home='BBBBBB';
        
        SELECT *
        INTO dedup_d
        FROM {masterList} 
        SELECT *
        INTO dedup_d2
        FROM {masterList}
        
        SELECT *
        INTO dedup_b
        FROM {masterList}
        """.format(masterList=masterListDTN)
        dbContext.executeNoResults( query ) 
        if version == 2:
            query = """
                UPDATE finalResult_RC1 SET dcrxid_home='000129' WHERE dcrxid_home IN ('008071', '067629', '097923');
                UPDATE finalResult_RC1 SET dcrxid_attend='051284' WHERE dcrxid_attend='051292';
                UPDATE finalResult_RC1 SET dcrxid_attend='051060' WHERE dcrxid_attend='064998';
            """
            dbContext.executeNoResults( query ) 
        dbContext.executeNoResults( nodupkey( "dedup_d", "dcrxid_c" ) ) 
        dbContext.executeNoResults( nodupkey( "dedup_d2", "dcrxid_c" ) ) 
        dbContext.executeNoResults( nodupkey( "dedup_b", "bcrxid_c" ) ) 
        query = """
        ALTER TABLE dedup_d DROP COLUMN bcrxid_c, schtype, source, bcrxnm
        EXEC SP_RENAME 'dedup_d.dcrxid_c', 'dcrxid_home', 'COLUMN'
        EXEC SP_RENAME 'dedup_d.distrtype', 'distrtype_home', 'COLUMN'
        EXEC SP_RENAME 'dedup_d.dcrxnm', 'dcrxnm_home', 'COLUMN'
        
        ALTER TABLE dedup_b DROP COLUMN dcrxid_c, distrtype, source, dcrxnm
        EXEC SP_RENAME 'dedup_b.bcrxid_c', 'bcrxid_home', 'COLUMN'
        EXEC SP_RENAME 'dedup_b.schtype', 'schtype_home', 'COLUMN'
        EXEC SP_RENAME 'dedup_b.bcrxnm', 'bcrxnm_home', 'COLUMN'
        ALTER TABLE finalResult_RC1 DROP COLUMN dcrxnm_home, distrtype_home;
        """
        dbContext.executeNoResults( query ) 
        query = """
        
        SELECT F.*, D.[dcxx_county] as [dcxx_county2], D.[dcrxnm_home], D.[distrtype_home]
        INTO merge_master1
        FROM finalResult_RC1 F
        LEFT JOIN dedup_d D ON (D.dcrxid_home = F.dcrxid_home);
        
        UPDATE merge_master1 
        SET dcxx_county=dcxx_county2 
        WHERE dcxx_county='' OR dcxx_county IS NULL;
        
        ALTER TABLE merge_master1 DROP COLUMN dcxx_county2, bcrxnm_home, schtype_home;
        """
        dbContext.executeNoResults( query ) 
        query = """
        
        SELECT F.*, D.[dcxx_county] as [dcxx_county2], D.[bcrxnm_home], D.[schtype_home]
        INTO OGT_RC1_1
        FROM merge_master1 F
        LEFT JOIN dedup_b D ON (D.bcrxid_home = F.bcrxid_home)
        
        UPDATE OGT_RC1_1
        SET dcxx_county=dcxx_county2 
        WHERE dcxx_county='' AND dcxx_county IS NULL;
        
        ALTER TABLE OGT_RC1_1 DROP COLUMN dcxx_county2;
        """
        dbContext.executeNoResults( query ) 
        query = """
        
        SELECT *
        INTO OGT_P2 
        FROM OGT_RC1_1
        WHERE bcrxid_attend='999999'
        SELECT *
        INTO OGT_P1 
        FROM OGT_RC1_1
        WHERE bcrxid_attend<>'999999' or bcrxid_attend is null;
        
        ALTER TABLE OGT_P1 DROP COLUMN bcrxnm_attend, dcrxnm_attend
        SELECT A.*, B.dcrxnm as dcrxnm_attend, B.bcrxnm as bcrxnm_attend
        INTO OGT_RC1_2
        FROM OGT_P1 A 
        LEFT JOIN %s B ON (A.bcrxid_attend = B.bcrxid_c and A.dcrxid_attend = B.dcrxid_c)
        
        ALTER TABLE OGT_P2 DROP COLUMN dcrxnm_attend
        SELECT A.*, B.dcrxnm as dcrxnm_attend
        INTO OGT_RC1_3
        FROM OGT_P2 A
        LEFT JOIN dedup_d2 B ON (A.dcrxid_attend = B.dcrxid_c)
        """ % (masterListDTN)
        dbContext.executeNoResults( query ) 
        
        
        ts = get_table_spec( "OGT_RC1_2", dbContext )
        ts.populate_from_connection()
        cols = [c.field_name for c in ts]
        query = """
        SELECT *
        INTO {finalTable}
        FROM
        (SELECT {column_names} FROM OGT_RC1_2 
        UNION ALL
        SELECT {column_names} FROM OGT_RC1_3) as tmp;
        
        UPDATE {finalTable} SET dcxx_county=SUBSTRING(M.dcxx_county, 0, 30)
        FROM {finalTable} F
        LEFT JOIN {masterList} M ON (M.dcrxid_c = F.dcrxid_attend);
        """.format(finalTable=rcOutputDTN,masterList=masterListDTN, column_names=Joiner(cols))
        dbContext.executeNoResults( query ) 
        
        if version == 1:
            query = """
            Select LTRIM(RTRIM(ucrx_preid)) as ucrx_preid
            INTO blanklist  
            FROM {finalTable} 
            where ucrx_preid not in (SELECT ucrx_preid from Tidefile)
            and ucrx_preid<>'' and ucrx_preid is not null;
            
            UPDATE {finalTable} SET ucrx_preid='' WHERE ucrx_preid IN (SELECT ucrx_preid FROM blanklist);
            
            SELECT * 
            INTO OGT_Tide
            FROM {finalTable}
            
            UPDATE OGT_Tide SET ucrxgen='F' WHERE ucrxgen='1'
            UPDATE OGT_Tide SET ucrxgen='M' WHERE ucrxgen='2'
            UPDATE OGT_Tide SET studentmid='' WHERE studentmid='*'
            """.format(finalTable=rcOutputDTN)
            dbContext.executeNoResults( query ) 
        
            lengths = dbContext.execute( "SELECT length FROM TideLayout") 
            var_names = dbContext.execute( "SELECT variable_name FROM TideLayout WHERE variable_name NOT IN ('filler0', 'filler1', 'filler2', 'filler3', 'filler4', 'filler5', 'filler6', 'filler7', 'drops1', 'drops2', 'drops3', 'preid_num1', 'preid_num2', 'ufxx_accel_test', 'upcx_DNS', 'upcx_INV', 'upmx_DNS', 'upmx_INV', 'uprx_DNS', 'uprx_INV', 'uprx_TOG', 'upsx_DNS', 'upsx_INV', 'upwx_DNS', 'upwx_INV' )") 
            var_names_all = dbContext.execute( "SELECT variable_name FROM TideLayout") 
            lengths = [l[0] for l in lengths]
            var_names = [v[0] for v in var_names]
            var_names_all = [v[0] for v in var_names_all]
            # Some RC versions seperate homeschool, some do not
            #rows = dbContext.execute( "SELECT {cols} FROM OGT_Tide ORDER BY CONVERT(binary(30), ssid), lithocode".format(cols=Joiner(var_names)) )
            rows = dbContext.execute( "SELECT {cols} FROM OGT_Tide WHERE schtype_attend <> 'H' ORDER BY CONVERT(binary(30), ssid), lithocode".format(cols=Joiner(var_names)) ) 
        
            file_s = datetime.datetime.now()
            fh = open(tideDataOutputFile, "w")
            for row in rows:
                for i,v in enumerate(var_names_all):
                    s = "{:<" + str(lengths[i]) + "}"
                    if v in var_names:
                        for j,v2 in enumerate(var_names):
                            if (v == v2): 
                                if v2 in ["grade", "dob_day", "dob_month", "dob_year"]:
                                    if row[j] is None:
                                        s = "{:>" + str(lengths[i]) + "}"
                                        fh.write(s.format(".")) 
                                    else:
                                        s = "{:0>" + str(lengths[i]) + "}"
                                        fh.write(s.format(int(row[j]))[0:lengths[i]])
                                else:
                                    fh.write(s.format(row[j] or "")[0:lengths[i]])
                                break
                    else:
                        fh.write(s.format(""))
                fh.write("\n")
            fh.close()
            print "time to write file: " + str(datetime.datetime.now() - file_s)
            
            if testOutput:
                f1 = open(tideDataOutputFile,'r')
                f2 = open(tideDataTestCopy,'r')
                lines1 = f1.readlines()
                lines2 = f2.readlines()
                for i,l1 in enumerate(lines1):
                    if l1 != lines2[i]:
                        print i
                        print l1
                        print lines2[i]
                        raise Exception
                print "Complete Match!"
         
        print "******** FINISHED ********" + str(datetime.datetime.now())
        
    masterListDTN = "masterList"
    '''
    runContextName = 'OGT_12SU'
    #runContextName = 'unittest'
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    basepath = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Summer 2012\\Code\\Development\\TideRecordChange\\"
    tideLayoutFile     = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Spring 2012\\Code\\Development\\TideRecordChange\\TIDE_FINAL2Pearsonedits100209.xls"
    tideSpecFile       = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Fall 2012\\Code\\Development\\TideRecordChange\\spec_rc1.xls"
    tideDataDirectory  = "H:\\Assessment\\PreProductionSystems\\TIDE\\OGT\\Summer 2012\\Prod\\RC1 For SAS\\"
    tideDataTestCopy   = "H:\\share\\Ohio Graduation Tests\\Technical\\2012 July\\ScoreReports\\SAS_TIDE_Data\\RecordChange1\\tidefile_ogt.txt"
    tideDataOutputFile = "C:\\out.txt"
    runRC(1, tideLayoutFile, tideSpecFile, tideDataDirectory, tideDataTestCopy, tideDataOutputFile, masterListDTN, dbContext, runContextName)
        
    
    '''
    runContextName = 'OGT_12FA'
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    tideLayoutFile     = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Spring 2012\\Code\\Development\\TideRecordChange\\TIDE_FINAL2Pearsonedits100209.xls"
    tideSpecFile       = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Fall 2012\\Code\\Development\\TideRecordChange\\spec_rc1.xls"
    tideDataDirectory  = "H:\\Assessment\\PreProductionSystems\\TIDE\\OGT\\Fall 2012\\Prod\\RC1 Extract\\"
    #tideDataTestCopy   = "H:\\share\\Ohio Graduation Tests\\Technical\\2012 October\\ScoreReports\\SAS_TIDE_Data\\RecordChange1\\tidefile_ogt.txt"
    tideDataTestCopy   = "C:\\tidefile_ogt.txt"
    tideDataOutputFile = "C:\\out.txt"
    rc1_s = datetime.datetime.now()
    runRC(1, tideLayoutFile, tideSpecFile, tideDataDirectory, tideDataTestCopy, tideDataOutputFile, masterListDTN, dbContext, runContextName)
    
    tideLayoutFile     = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Spring 2012\\Code\\Development\\TideRecordChange\\TIDE_FINAL2Pearsonedits100209.xls"
    tideSpecFile       = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Fall 2012\\Code\\Development\\TideRecordChange\\spec_rc2.xls"
    tideDataDirectory  = "H:\\Assessment\\PreProductionSystems\\TIDE\\OGT\\Fall 2012\\Prod\\RC2 Extract\\"
    tideDataTestCopy   = None
    tideDataOutputFile = None
    rc2_s = datetime.datetime.now()
    runRC(2, tideLayoutFile, tideSpecFile, tideDataDirectory, tideDataTestCopy, tideDataOutputFile, masterListDTN, dbContext, runContextName)
    print "RC1 ran: " + str(rc2_s - rc1_s)
    print "RC2 ran: " + str(datetime.datetime.now() - rc2_s)    
    
