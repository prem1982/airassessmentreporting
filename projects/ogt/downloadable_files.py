import glob
from multiprocessing import Queue, Process, Lock
import os
from airassessmentreporting.datacheck.dataset import *
from airassessmentreporting.merge import *
from airassessmentreporting.airutility.safeexcelread import SafeExcelReader
from airassessmentreporting.airutility import (RunContext, get_table_spec, Joiner)
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.airutility.dbutilities import drop_tables, get_column_names
from airassessmentreporting.airutility.sqlutils import (nodupkey, nodupkey2)
import datetime

NUM_PROCESSES = 1
#NUM_PROCESSES = 6

def sasFormat(value, form):
    ### Returns value formatted according to the matching form rules ###
    form = form.lower()
    def within(v0, v1, out): return lambda v: out.format(v) if v0 <= v <= v1 else None
    def gt(v0, out): return lambda v: out if v > v0 else None
    def lt(v0, out): return lambda v: out if v < v0 else None
    def ge(v0, out): return lambda v: out if v >= v0 else None
    def le(v0, out): return lambda v: out if v <= v0 else None
    def eq(v0, out): return lambda v: out if v == v0 else None
    def ne(v0, out): return lambda v: out if v != v0 else None
    
    if form in ['rawfmt']:
        try:
            value = int(value*10)
        except ValueError:
            value = value
    if form in ['padzerofmt', 'dayfmt', 'yearfmt', 'perflevfmt', 'scalefmt', 'levfmt', 'revfmt', 'binaryfmt', 'yesnofmt']:
        try:
            value = int(value)
        except ValueError:
            value = value
    if form in ['perflevfmt', 'ethnicityfmt', 'migrantfmt', 'binaryfmt', 'writingpromptfmt', 'levfmt', 'yesnofmt', 'revfmt']:
        value = str(value)
     
    default = '!'
    if form == 'padzerofmt':
        match_ops = [within(1, 12, "{:0>2}"), eq('.', '  '), eq('', '  '), eq(None, '  ')]
        default = '! '
    elif form == 'dayfmt':
        match_ops = [within(1, 31, "{:0>2}"), eq('.', '  '), eq(99, '  '), eq('', '  '), eq('', '  '), eq(None, '  ')]
        default = '! '
    elif form == 'yearfmt':
        match_ops = [within(1, 9999, "{:0>4}"), eq('.', '    '), eq('', '    '), eq(None, '    ')]
        default = '! '
    elif form == 'genderfmt':
        match_ops = [eq('1', 'F'), eq('2', 'M'), eq('*', ' '), eq(' ', ' '), eq('', ' ')]
    elif form == 'scalefmt':
        match_ops = [within(0, 998, "{:0>3}"), eq(999, 'INV'), eq('.', 'DNA'), eq('.A', 'DNA'), eq('', 'DNA'), eq(None, 'DNA')]
    elif form == 'perflevfmt':
        match_ops = [eq('1', '15'), eq('2', '14'), eq('3', '13'), eq('4', '12'), eq('5', '11'), eq('.I', '99'), eq('.A', '  '), eq('.', '  '), eq('', '  '), eq(None, '  ')]
    elif form == 'ethnicityfmt':
        match_ops = [eq('1', '1'), eq('2', '2'), eq('3', '3'), eq('4', '4'), eq('5', '5'), eq('6', '6'), eq('7', '7'), eq('*', '  '), eq(' ', '  '), eq('', '  ')]
    elif form == 'migrantfmt':
        match_ops = [eq('1', 'Y'), eq('', ' '), eq(' ', ' '), eq('0', ' ')]
    elif form == 'binaryfmt':
        match_ops = [eq('1', 'Y'), eq('0', ' ')]
    elif form == 'writingpromptfmt':
        match_ops = [eq('A', 'A '), eq('B', 'B '), eq('C', 'C '), eq('D', 'D '), eq('E', 'E '), eq('F', 'F '), eq('G', 'G '), eq('T', 'T '), eq('', '  '), eq(' ', '  ')]
        default = '! '
    elif form == 'rawfmt':
        match_ops = [within(0, 999, "{:0>3}"), eq('.A', '   '), eq('.I', '   '), eq('.S', '   '), eq('.', '   '), eq('', '   '), eq(None, '   ')]
    elif form == 'levfmt':
        match_ops = [eq('1', '-'), eq('2', '*'), eq('3', '+'), eq('.A', ' '), eq('.I', ' '), eq('.S', ' '), eq('.', ' '), eq('', ' '), eq(None, ' ')]
    elif form == 'yesnofmt':
        match_ops = [eq('1', 'Y'), eq('0', 'N'), eq('.', ' '), eq('', ' '), eq(None, ' ')]
    elif form == 'revfmt':
        match_ops = [eq('0', 'Y'), eq('1', 'N')]
    else:
        raise ValueError( "Format form not found!" )
    
    m = map(lambda x: x(value), match_ops)
    return reduce(lambda x, y: x or y, m) or default

def writeToFile(rows, fh, fhc, state, fields, starts, names, lengths, types, var_names, outformats):
    for row in rows:
        pointer = 1
        for i,v in enumerate(fields):
            if state == 1 and i < 4:
                continue
            #if the current pointer is less than the 'start' value, write a space 
            while starts[i] > pointer:
                pointer += 1
                fh.write(" ") 
                
            try:
                previousname = names[i-1]
            except IndexError:
                previousname = ''
            if fhc is not None and previousname != names[i]:
                fhc.write('"')
                
            s = "{:<" + str(lengths[i]) + "}"
            if types[i] == "variable":
                outputString = row[var_names.index(v)] 
                outputString = "" if outputString is None else outputString
                if '$' not in outformats[i]:
                    outputString = sasFormat(outputString, outformats[i])
                if v == 'dasites' and outputString != "":    
                    s = "{:0>" + str(lengths[i]) + "}"
                fh.write(s.format(outputString)[0:lengths[i]])
                if fhc is not None:
                    fhc.write(s.format(outputString)[0:lengths[i]])
            else:
                fh.write(s.format(fields[i].replace("'","") or "")[0:lengths[i]])
                if fhc is not None:
                    fhc.write(fields[i].replace("'","")[0:lengths[i]])
            pointer += lengths[i]
            
            try:
                futurename = names[i+1]
            except IndexError:
                futurename = ''
            if fhc is not None and futurename != names[i]:
                fhc.write('",')
        fh.write("\n")
        if fhc is not None:
            fhc.write("\n")

def outputStateFile(i, outputDir, runContextName, stateTable, fields, starts, names, lengths, types, var_names, outformats):
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    
    num_rows = dbContext.execute( "SELECT count(*) FROM {stateTable}".format(stateTable=stateTable))
    length = num_rows[0][0] / NUM_PROCESSES
    
    print 'starting to make wholestate' + str(datetime.datetime.now())
    fh = open(outputDir+"WholeState%s.txt" % i, "w")
    rows = dbContext.execute( "SELECT {cols} FROM {stateTable} ORDER BY dcrxid, grade, ucrxlnm, ucrxfnm, lithocode, studentmid".format(cols=Joiner(var_names), stateTable=stateTable))
    #writeToFile(rows, fh, None, 1, fields, starts, names, lengths, types, var_names, outformats)
    if i == NUM_PROCESSES-1:
        writeToFile(rows[(length*i):], fh, None, 1, fields, starts, names, lengths, types, var_names, outformats)
    else:
        writeToFile(rows[(length*i):(length*i+length)], fh, None, 1, fields, starts, names, lengths, types, var_names, outformats)
    fh.close()
    
def outputDistrictFiles(q, i, lock, outputDir, runContextName, districtTable, fields, starts, names, lengths, types, var_names, outformats):
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    while (True):
        lock.acquire()
        if q.empty():
            lock.release()
            print "Thread %s is OVER " % i + str(datetime.datetime.now())
            return
        else:
            district = q.get()
            lock.release()
        
        fh = open(outputDir+district+".txt", "w")
        fhc = open(outputDir+district+".csv", "w")
        rows = dbContext.execute( "SELECT {cols} FROM {districtTable} WHERE dcrxid='{district}' ORDER BY dcrxid, grade, ucrxlnm, ucrxfnm, lithocode, studentmid".format(cols=Joiner(var_names), district=district, districtTable=districtTable))
        lastname = ''
        for n in names:
            if n != lastname:
                fhc.write(n.upper()+',')
            lastname = n
        fhc.write('\n')
        writeToFile(rows, fh, fhc, 0, fields, starts, names, lengths, types, var_names, outformats)
        fh.close()
        fhc.close()


if __name__ == '__main__':
    
    basepath  = "C:\\CVS Projects\\ScoreReportMacros\\UnitTested\\"
    inputFile = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Spring 2012\\Code\\Development\\DownloadableFiles\\LongformatSP12.xls"
    dataFile1  = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Fall 2008\\code\\ODE downloadable files\\longformat\\Copy of JVSschool.xls"
    #outputDir = "H:\share\Ohio Graduation Tests\Technical\2013 March\ScoreReports\ODEdownloadleFiles"
    outputDir = "C:\\out\\"
    
    #runContext = RunContext('sharedtest')
    runContextName = 'OGT_12SP'
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    
    
    #studentTable = 'StudentOGT_2012P_'
    studentTable = 'sas_student_'
    #dasitesTable = 'OGT_dasites'
    dasitesTable = 'sas_dasites'
    
    sql_s = datetime.datetime.now()
    
    if True:
        print "downloadable files start: " + str(datetime.datetime.now())
        drop_tables(['OGT_DL_Vars', 'OGT_DL_Data'], dbContext)
        reader = SafeExcelReader( runContext )
        reader.db_context = dbContext
        reader.filename = inputFile
        reader.sheetName = "Sheet1"
        reader.outputTable = "OGT_DL_Vars"
        reader.createTable()
        
        reader = SafeExcelReader( runContext )
        reader.db_context = dbContext
        reader.filename = dataFile1 
        reader.sheetName = "AA"
        reader.outputTable = "OGT_DL_Data"
        reader.createTable()
        dbContext.executeNoResults('''
            EXEC SP_RENAME 'OGT_DL_Data.bcrxid', 'dcrxid', 'COLUMN';
            
            DECLARE @SQL VARCHAR(4000)
            SET @SQL = 'ALTER TABLE OGT_DL_Data DROP CONSTRAINT |ConstraintName| '
            SET @SQL = REPLACE(@SQL, '|ConstraintName|', ( SELECT   name
                                                           FROM     sysobjects
                                                           WHERE    xtype = 'PK'
                                                                    AND parent_obj = OBJECT_ID('OGT_DL_Data')
                                                         ))
            EXEC (@SQL)
            
            ALTER TABLE OGT_DL_Data DROP COLUMN import_order, bcrxnm;
        ''')
    
        drop_tables(['OGT_DL0'], dbContext)
        valid_cols  = get_column_names( "{studentTable}0".format(studentTable=studentTable), db_context=dbContext)
        valid_cols += get_column_names( "{studentTable}1".format(studentTable=studentTable), db_context=dbContext)
        valid_cols += get_column_names( "{studentTable}2".format(studentTable=studentTable), db_context=dbContext)
        valid_cols = [i.replace('[','') for i in valid_cols]
        valid_cols = [i.replace(']','').lower() for i in valid_cols]
        cols = dbContext.execute("SELECT variable FROM ogt_dl_vars WHERE LOWER([value type])='variable' --variable IS NOT NULL AND variable<>'''''' AND variable<>'' AND  ")
        cols = [c[0].lower() for c in cols]
        valid_cols = [c for c in valid_cols if c in cols]
        valid_cols += ['ufxx_sample', 'A.lithocode'.format(studentTable=studentTable)]
        query = "SELECT {cols} INTO OGT_DL0 FROM {studentTable}0 A JOIN {studentTable}1 B ON (A.lithocode = B.lithocode) JOIN {studentTable}2 C ON (A.lithocode = C.lithocode)".format(cols=Joiner(valid_cols), studentTable=studentTable)
        dbContext.executeNoResults(query)
        
        drop_tables(['OGT_DL1', 'OGT_DL2', 'OGT_DL3', 'OGT_DL4', 'OGT_DL_tmp0', 'OGT_DL_tmp1'], dbContext)
        query = """
        UPDATE OGT_DL0 SET ucsx_classname=REPLACE(ucsx_classname,'[','(');
        UPDATE OGT_DL0 SET ucrxlnm=REPLACE(ucrxlnm,'`','''');
        UPDATE OGT_DL0 SET ucrxfnm=REPLACE(ucrxfnm,'`','''');
        UPDATE OGT_DL0 SET ucxx_room_number=REPLACE(ucxx_room_number,'?','');
        UPDATE OGT_DL0 SET ucmx_coursecode=REPLACE(ucmx_coursecode,'`','');
        DELETE FROM OGT_DL0 WHERE bcrxid_attend='000001';
        UPDATE OGT_DL0 SET ssid='' WHERE ssid NOT LIKE '[0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z]_' 
        UPDATE OGT_DL0 SET dcrxid_home=' ' WHERE dcrxid_home='     .';
        UPDATE OGT_DL0 SET dcrxid=dcrxid_attend;
        UPDATE OGT_DL0 SET dcrxnm_attend=dbo.ToProperCase(dcrxnm_attend, 14);
        UPDATE OGT_DL0 SET bcrxnm_attend=dbo.ToProperCase(bcrxnm_attend, 14);
        UPDATE OGT_DL0 SET dcrxnm_home=dbo.ToProperCase(dcrxnm_home, 14);
        --This doesn't exist in ogt, may need a conditional statement if other clients use this
        --UPDATE OGT_DL0 SET bcrxnm_home=dbo.ToProperCase(bcrxnm_home, 14);
        UPDATE OGT_DL0 SET ucrxlnm=dbo.ToProperCase(ucrxlnm, 14);
        UPDATE OGT_DL0 SET ucrxfnm=dbo.ToProperCase(ucrxfnm, 14);
        
        SELECT * 
        INTO OGT_DL_tmp1
        FROM OGT_DL0 
        WHERE (schtype='J' AND LTRIM(RTRIM(dcrxid_home))<>'') OR
        (schtype NOT IN ('J', 'D', 'Y') AND dcrxid_home IS NOT NULL AND LTRIM(RTRIM(dcrxid))<>'');
        UPDATE OGT_DL_tmp1 SET dcrxid=dcrxid_home 
        
        SELECT * INTO
        OGT_DL_tmp0
        FROM OGT_DL0 WHERE schtype IN ('J', 'D', 'Y')
        UPDATE OGT_DL_tmp0 SET dcrxid=bcrxid_attend 
        
        SELECT *
        INTO 
        OGT_DL1
        FROM (
         SELECT * FROM OGT_DL_tmp0 
         UNION ALL
         SELECT * FROM OGT_DL_tmp1 
         UNION ALL
         SELECT * FROM OGT_DL0
        ) as tmp; 
        
        SELECT *
        INTO 
        OGT_DL2
        FROM (
         SELECT * FROM {dasitesTable} WHERE dcrxid IS NOT NULL AND LTRIM(RTRIM(dcrxid))<>''
         UNION ALL
         SELECT * FROM OGT_DL_Data WHERE dcrxid IS NOT NULL AND LTRIM(RTRIM(dcrxid))<>''
        ) as tmp; 
        
        SELECT A.*, B.dasites INTO OGT_DL3 FROM OGT_DL1 A
        LEFT JOIN OGT_DL2 B ON A.dcrxid=B.dcrxid
        
        SELECT A.*, B.dasites INTO OGT_DL4 FROM OGT_DL0 A
        LEFT JOIN OGT_DL2 B ON A.dcrxid=B.dcrxid
        
        """.format(studentTable=studentTable, dasitesTable=dasitesTable)
        #print query
        dbContext.executeNoResults(query)
    


    def odeFilesMain(formatTable, stateTable, districtTable, runContextName):
        #output a text and cvs file for each dcrxid  (i.e. 000123.cvs)
            #select  data where dcrxid = this district's dcrxid
            #select entire dataset (order by dcrxid, grade, ucrxlnm, ucrxfnm
        var_names = dbContext.execute( "SELECT variable FROM {formatTable} WHERE [value type]='variable' order by start".format(formatTable=formatTable)) 
        var_names = [v[0] for v in var_names]
        
        #Check if variables listed in excel file exist in data - raise exception if they do not
        rows0 = dbContext.execute( "SELECT TOP 1 {cols} FROM {districtTable}".format(cols=Joiner(var_names), districtTable=districtTable)) 
        rows0 = dbContext.execute( "SELECT TOP 1 {cols} FROM {stateTable}".format(cols=Joiner(var_names), stateTable=stateTable)) 
        
        districts = dbContext.execute( "SELECT DISTINCT dcrxid FROM {districtTable} order by dcrxid".format(districtTable=districtTable)) 
        lengths = dbContext.execute( "SELECT length FROM {formatTable} order by start".format(formatTable=formatTable)) 
        fields = dbContext.execute( "SELECT variable FROM {formatTable} WHERE variable <> 'dcrxid' order by start".format(formatTable=formatTable)) 
        types = dbContext.execute( "SELECT [value type] FROM {formatTable} order by start".format(formatTable=formatTable)) 
        const_names = dbContext.execute( "SELECT variable FROM {formatTable} WHERE [value type]<>'variable' order by start".format(formatTable=formatTable)) 
        starts = dbContext.execute( "SELECT start FROM {formatTable} order by start".format(formatTable=formatTable)) 
        outformats = dbContext.execute( "SELECT [out format] FROM {formatTable} order by start".format(formatTable=formatTable)) 
        names = dbContext.execute( "SELECT [ODE col name] FROM {formatTable} WHERE variable <> 'dcrxid' order by start".format(formatTable=formatTable)) 
        
        districts = [d[0] for d in districts]
        lengths = [l[0] for l in lengths]
        fields = [t[0] for t in fields]
        types = [t[0] for t in types]
        const_names = [c[0] for c in const_names]
        starts = [s[0] for s in starts]
        outformats = [o[0] for o in outformats]
        names = [n[0] for n in names]
        
        #outputDistrictFiles()
        #outputStateFile()
        
        #ps_state = Process(target=outputStateFile, args=(outputDir, runContextName, stateTable, fields, starts, names, lengths, types, var_names, outformats))
        ps = [Process(target=outputStateFile, args=(i, outputDir, runContextName, stateTable, fields, starts, names, lengths, types, var_names, outformats)) for i,x in enumerate(range(NUM_PROCESSES))]
        #ps_state.start()
        [p.start() for p in ps]
        [p.join() for p in ps]
        
        fh = open(outputDir+"WholeState.txt", "w")
        for i in range(NUM_PROCESSES):
            f = open(outputDir+"WholeState%s.txt" % i, "r")
            fh.write(f.read())
            f.close()
        fh.close()

        print "STATE FILE COMPLETE " + str(datetime.datetime.now())
        
        q = Queue()
        lock = Lock()
        [q.put(x) for x in districts]
        ps = [Process(target=outputDistrictFiles, args=(q, i, lock, outputDir, runContextName, districtTable, fields, starts, names, lengths, types, var_names, outformats)) for i,x in enumerate(range(NUM_PROCESSES))]
        [p.start() for p in ps]
        [p.join() for p in ps]
        #ps_state.join()
            
    sql_e = datetime.datetime.now()
    if True:
        odeFilesMain("OGT_DL_Vars", "OGT_DL4", "OGT_DL3", runContextName)
    
    files_e = datetime.datetime.now()
    
    if True:
        print "testing start: " + str(datetime.datetime.now())
        #testDir = 'H:\\share\\Ohio Graduation Tests\\Technical\\2012 March\\ScoreReports\\ODEdownloadleFiles\\'
        testDir = 'C:\\out2\\'
        os.chdir(testDir)
        i = 0
        for idx,file in enumerate(glob.glob("*.*")):
            fh_mastercopy = open(testDir+file,'r')
            fh_out = open(outputDir+file,'r')
            lines_master = fh_mastercopy.readlines()
            lines_out = fh_out.readlines()
            for i,l1 in enumerate(lines_master):
                #TODO delete this once we are pulling data from a python derived dataset
                if 'csv' in file:
                    if l1[331:333] == '99':
                        s = list(l1)
                        s[331] = ' '
                        s[332] = ' '
                        l1 = "".join(s)
                    if l1[463:465] == '99':
                        s = list(l1)
                        s[463] = ' '
                        s[464] = ' '
                        l1 = "".join(s)
                    if l1[201:203] == '99':
                        s = list(l1)
                        s[201] = ' '
                        s[202] = ' '
                        l1 = "".join(s)
                    if l1[398:400] == '99':
                        s = list(l1)
                        s[398] = ' '
                        s[399] = ' '
                        l1 = "".join(s)
                    if l1[266:268] == '99':
                        s = list(l1)
                        s[266] = ' '
                        s[267] = ' '
                        l1 = "".join(s)    
                else:
                    if l1[336:338] == '99':
                        s = list(l1)
                        s[336] = ' '
                        s[337] = ' '
                        l1 = "".join(s)
                    if l1[288:290] == '99':
                        s = list(l1)
                        s[288] = ' '
                        s[289] = ' '
                        l1 = "".join(s)
                    if l1[240:242] == '99':
                        s = list(l1)
                        s[240] = ' '
                        s[241] = ' '
                        l1 = "".join(s)    
                    if l1[145:147] == '99':
                        s = list(l1)
                        s[145] = ' '
                        s[146] = ' '
                        l1 = "".join(s)
                    if l1[192:194] == '99':
                        s = list(l1)
                        s[192] = ' '
                        s[193] = ' '
                        l1 = "".join(s)
                    
                if l1.lower() != lines_out[i].lower():
                    print len(l1)
                    print len(lines_out[i])
                    print file
                    print i
                    print l1
                    print lines_out[i]
                    
                    for j,c in enumerate(l1):
                        if (c != lines_out[i][j]):
                            print j
                            print c
                            print lines_out[i][j]
                    raise Exception
        print "Complete Match!"
        
    print "Finished: " + str(datetime.datetime.now())
    print ""
    print "Sql: " + str(sql_e - sql_s)
    print "Files: " + str(files_e - sql_e)
    print "Testing: " + str(datetime.datetime.now() - files_e)
    