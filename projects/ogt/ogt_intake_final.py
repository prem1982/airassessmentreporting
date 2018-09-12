from airassessmentreporting.airutility import (RunContext, get_table_spec )
from airassessmentreporting.airutility.formatutilities import Joiner
from airassessmentreporting.airutility.dbutilities import drop_tables
from airassessmentreporting.intake import preqc 
from multiprocessing import Process, Queue, Manager, Lock
import airassessmentreporting.datacheck.rescore
import airassessmentreporting.datacheck.idcheck
import airassessmentreporting.datacheck.longcomp
import airassessmentreporting.datacheck.raw_converter
from airassessmentreporting.datacheck.dataset import *
import datetime
import copy


def call_preqc(q, lock, rc, layoutFile, flat_tablename, i, row_count):
    runContext = RunContext(rc)
    dbContext = runContext.getDBContext()
    inputFile = q.get()
    x = preqc.PreQC(runcontext=runContext, dbcontext=dbContext, 
                    layoutfile=layoutFile,
                    inputfile=inputFile,
                    patterns=[ ('[u][pcf][w]x_.*{icnt}.*','[u][pcf][w]x__OE.*Pos.*{icnt}','[u][pcf][w]x_OE_.*{icnt}','MC_TABLE_W_'+str(i),'W')
                              ,('[u][pcf][c]x_.*{icnt}.*','[u][pcf][c]x__OE.*Pos.*{icnt}','[u][pcf][c]x_OE_.*{icnt}','MC_TABLE_C_'+str(i),'C')
                              ,('[u][pcf][s]x_.*{icnt}.*','[u][pcf][s]x__OE.*Pos.*{icnt}','[u][pcf][s]x_OE_.*{icnt}','MC_TABLE_S_'+str(i),'S')
                              ,('[u][pcf][r]x_.*{icnt}.*','[u][pcf][r]x__OE.*Pos.*{icnt}','[u][pcf][r]x_OE_.*{icnt}','MC_TABLE_R_'+str(i),'R')
                              ,('[u][pcf][m]x_.*{icnt}.*','[u][pcf][m]x__OE.*Pos.*{icnt}','[u][pcf][m]x_OE_.*{icnt}','MC_TABLE_M_'+str(i),'M')],
                    flat_tablename=flat_tablename,debug=False,bulk_insert=False, errorfile='c:\SAS\OGT\Error%s.txt' % i, 
                    table_names='TABLE_NAMES_'+str(i), output_table='Layout_Temp_'+str(i), lock=lock, batchfile='bcpfile%s.bat' % i,
                    outputsdir = 'c:/SAS/OGT/Joboutput%s/' % i, row_count=row_count)
    x.process()
    

def call_raw_converter(q, table, workbookFile, ctPath_breach, ctPath, form=None, server=None, db=None, conn=None):
    subject = q.get()
    
    ogtReg = Dataset(dbms='pyodbc', 
                     conn=conn,
                     db=db,
                     server=server,
                     query="""   
                      SELECT
                      ufrx_breach,    ufwx_breach,   ufmx_breach,   ufsx_breach,   ufcx_breach,    ufrx_invalid,    ufwx_invalid,    ufmx_invalid,    ufsx_invalid,    ufcx_invalid,    distrtype_attend,    ethnicity,    schtype_attend,    uccx_form,    ucmx_form,    ucrx_form,    ucrxgen,    ucsx_form,    ucwx_form,    ufxx_Gr11_graduation,    SSID,    BCRXID_ATTEND,    BCRXNM_ATTEND,    DCRXID_ATTEND,    DCRXID_HOME,    DCRXNM_ATTEND,    LITHOCODE,    id,    Grade,    preidflag,    ufcx_attempt,    ufmx_attempt,    ufrx_attempt,    ufsx_attempt,    ufwx_attempt,    ufxx_SAMPLE,    ufxx_TEST_TYPE,    upxxiep,    upxxlep,    dcxx_county,    form_r,    form_m,    form_w,    form_s,    form_c,    distrtype_home,    migrant,    schtype_home,    uccx_sub_name,    ucmx_sub_name,    ucrx_sub_name,    ucsx_sub_name,    ucwx_sub_name,    ucxx_spec_test_ver,    upcelev,    upceraw,    upcescal,    upchlev,    upchraw,    upchscal,    upcmlev,    upcmraw,    upcmscal,    upcslev,    upcsraw,    upcsscal,    upcxlev,    upcxraw,    upcxscal,    upmalev,    upmaraw,    upmascal,    upmdlev,    upmdraw,    upmdscal,    upmglev,    upmgraw,    upmgscal,    upmmlev,    upmmraw,    upmmscal,    upmnlev,    upmnraw,    upmnscal,    upmxlev,    upmxraw,    upmxscal,    upralev,    upraraw,    uprascal,    uprilev,    upriraw,    upriscal,    uprllev,    uprlraw,    uprlscal,    uprrlev,    uprrraw,    uprrscal,    uprxlev,    uprxraw,    uprxscal,    upselev,    upseraw,    upsescal,    upsllev,    upslraw,    upslscal,    upsplev,    upspraw,    upspscal,    upsslev,    upssraw,    upssscal,    upsxlev,    upsxraw,    upsxscal,    upwalev,    upwaraw,    upwascal,    upwclev,    upwcraw,    upwcscal,    upwplev,    upwpraw,    upwpscal,    upwxlev,    upwxraw,    upwxscal,    SERIAL_NUMBER,    BATCH_NUMBER,    BCRXID_HOME,    BCRXNM_HOME,    DCRXNM_HOME,    STUDENTID,    STUDENTMID,    UCCX_CLASSNAME,    UCCX_COURSECODE,    UCCX_SECTION_NUM,    UCCX_TEACHERID,    UCCX_TEACHERNAME,    UCCX_TESTGROUPNUMBER,    UCMX_CLASSNAME,    UCMX_COURSECODE,    UCMX_SECTION_NUM,    UCMX_TEACHERID,    UCMX_TEACHERNAME,    UCMX_TESTGROUPNUMBER,    UCRX_CLASSNAME,    UCRX_COURSECODE,    UCRX_PREID,    UCRX_SECTION_NUM,    UCRX_TEACHERID,    UCRX_TEACHERNAME,    UCRX_TESTGROUPNUMBER,    UCRXFNM,    UCRXLNM,    UCSX_CLASSNAME,    UCSX_COURSECODE,    UCSX_SECTION_NUM,    UCSX_TEACHERID,    UCSX_TEACHERNAME,    UCSX_TESTGROUPNUMBER,    UCWX_CLASSNAME,    UCWX_COURSECODE,    UCWX_SECTION_NUM,    UCWX_TEACHERID,    UCWX_TEACHERNAME,    UCWX_TESTGROUPNUMBER,    UCXX_DATA_ACQUISITION,    UCXX_ROOM_NUMBER,    dob_year,    upxx_WAWC_FirstPrompt,    upxx_WAWC_SecondPrompt,    ucxx_ADMIN_DATE_YR,    dob_day,    dob_month,    ucxx_ADMIN_DATE_MO,    ucxx_TEST_NAME,    upcx_DICTIONARY,    upcx_EXTENDED,    upcx_OTHER,    upcx_READ_ALOUD,    upcx_SCRIBE,    upcx_TAKEN,    upmx_CALCULATOR,    upmx_DICTIONARY,    upmx_EXTENDED,    upmx_OTHER,    upmx_READ_ALOUD,    upmx_SCRIBE,    upmx_TAKEN,    uprx_DICTIONARY,    uprx_EXTENDED,    uprx_OTHER,    uprx_READ_ALOUD,    uprx_SCRIBE,    uprx_TAKEN,    upsx_CALCULATOR,    upsx_DICTIONARY,    upsx_EXTENDED,    upsx_OTHER,    upsx_READ_ALOUD,    upsx_SCRIBE,    upsx_TAKEN,    upwx_DICTIONARY,    upwx_EXTENDED,    upwx_OTHER,    upwx_READ_ALOUD,    upwx_SCRIBE,    upwx_TAKEN,    upxx504
                      FROM %s 
                     """ % table,
                     columns = ['ufrx_breach', 'ufwx_breach', 'ufmx_breach', 'ufsx_breach', 'ufcx_breach', 'ufrx_invalid', 'ufwx_invalid', 'ufmx_invalid', 'ufsx_invalid', 'ufcx_invalid', 'distrtype_attend', 'ethnicity', 'schtype_attend', 'uccx_form', 'ucmx_form', 'ucrx_form', 'ucrxgen', 'ucsx_form', 'ucwx_form', 'ufxx_Gr11_graduation', 'ssid', 'BCRXID_ATTEND', 'BCRXNM_ATTEND', 'DCRXID_ATTEND', 'DCRXID_HOME', 'DCRXNM_ATTEND', 'LITHOCODE', 'id', 'grade', 'preidflag', 'ufcx_attempt', 'ufmx_attempt', 'ufrx_attempt', 'ufsx_attempt', 'ufwx_attempt', 'ufxx_SAMPLE', 'ufxx_TEST_TYPE', 'upxxiep', 'upxxlep', 'dcxx_county', 'form_r', 'form_m', 'form_w', 'form_s', 'form_c', 'distrtype_home', 'migrant', 'schtype_home', 'uccx_sub_name', 'ucmx_sub_name', 'ucrx_sub_name', 'ucsx_sub_name', 'ucwx_sub_name', 'ucxx_spec_test_ver', 'upcelev', 'upceraw', 'upcescal', 'upchlev', 'upchraw', 'upchscal', 'upcmlev', 'upcmraw', 'upcmscal', 'upcslev', 'upcsraw', 'upcsscal', 'upcxlev', 'upcxraw', 'upcxscal', 'upmalev', 'upmaraw', 'upmascal', 'upmdlev', 'upmdraw', 'upmdscal', 'upmglev', 'upmgraw', 'upmgscal', 'upmmlev', 'upmmraw', 'upmmscal', 'upmnlev', 'upmnraw', 'upmnscal', 'upmxlev', 'upmxraw', 'upmxscal', 'upralev', 'upraraw', 'uprascal', 'uprilev', 'upriraw', 'upriscal', 'uprllev', 'uprlraw', 'uprlscal', 'uprrlev', 'uprrraw', 'uprrscal', 'uprxlev', 'uprxraw', 'uprxscal', 'upselev', 'upseraw', 'upsescal', 'upsllev', 'upslraw', 'upslscal', 'upsplev', 'upspraw', 'upspscal', 'upsslev', 'upssraw', 'upssscal', 'upsxlev', 'upsxraw', 'upsxscal', 'upwalev', 'upwaraw', 'upwascal', 'upwclev', 'upwcraw', 'upwcscal', 'upwplev', 'upwpraw', 'upwpscal', 'upwxlev', 'upwxraw', 'upwxscal', 'SERIAL_NUMBER', 'BATCH_NUMBER', 'BCRXID_HOME', 'BCRXNM_HOME', 'DCRXNM_HOME', 'STUDENTID', 'STUDENTMID', 'UCCX_CLASSNAME', 'UCCX_COURSECODE', 'UCCX_SECTION_NUM', 'UCCX_TEACHERID', 'UCCX_TEACHERNAME', 'UCCX_TESTGROUPNUMBER', 'UCMX_CLASSNAME', 'UCMX_COURSECODE', 'UCMX_SECTION_NUM', 'UCMX_TEACHERID', 'UCMX_TEACHERNAME', 'UCMX_TESTGROUPNUMBER', 'UCRX_CLASSNAME', 'UCRX_COURSECODE', 'UCRX_PREID', 'UCRX_SECTION_NUM', 'UCRX_TEACHERID', 'UCRX_TEACHERNAME', 'UCRX_TESTGROUPNUMBER', 'UCRXFNM', 'UCRXLNM', 'UCSX_CLASSNAME', 'UCSX_COURSECODE', 'UCSX_SECTION_NUM', 'UCSX_TEACHERID', 'UCSX_TEACHERNAME', 'UCSX_TESTGROUPNUMBER', 'UCWX_CLASSNAME', 'UCWX_COURSECODE', 'UCWX_SECTION_NUM', 'UCWX_TEACHERID', 'UCWX_TEACHERNAME', 'UCWX_TESTGROUPNUMBER', 'UCXX_DATA_ACQUISITION', 'UCXX_ROOM_NUMBER', 'dob_year', 'upxx_WAWC_FirstPrompt', 'upxx_WAWC_SecondPrompt', 'ucxx_ADMIN_DATE_YR', 'dob_day', 'dob_month', 'ucxx_ADMIN_DATE_MO', 'ucxx_TEST_NAME', 'upcx_DICTIONARY', 'upcx_EXTENDED', 'upcx_OTHER', 'upcx_READ_ALOUD', 'upcx_SCRIBE', 'upcx_TAKEN', 'upmx_CALCULATOR', 'upmx_DICTIONARY', 'upmx_EXTENDED', 'upmx_OTHER', 'upmx_READ_ALOUD', 'upmx_SCRIBE', 'upmx_TAKEN', 'uprx_DICTIONARY', 'uprx_EXTENDED', 'uprx_OTHER', 'uprx_READ_ALOUD', 'uprx_SCRIBE', 'uprx_TAKEN', 'upsx_CALCULATOR', 'upsx_DICTIONARY', 'upsx_EXTENDED', 'upsx_OTHER', 'upsx_READ_ALOUD', 'upsx_SCRIBE', 'upsx_TAKEN', 'upwx_DICTIONARY', 'upwx_EXTENDED', 'upwx_OTHER', 'upwx_READ_ALOUD', 'upwx_SCRIBE', 'upwx_TAKEN', 'upxx504'],
                     open_mode='rb')
    ds_standards = Dataset(dbms='excel_srcn', 
                           workbook_file=workbookFile,
                           sheet_name="Standards", 
                           open_mode='rb')
    if form == 'BR':
        ds_sumcheck_out = Dataset(name="sumcheck_outBR_%s.csv" % subject, open_mode='wb')
        ds_sumcheck_report2 = Dataset(name="sumcheck_report2BR_%s.csv" % subject, open_mode='wb')
        ds_out = Dataset(name="conv_out1BR_%s.cvs" % subject,open_mode='wb')
        ds_report2 = Dataset(name="conv_out0BR_%s.cvs" % subject,open_mode='wb')
    else:
        ds_sumcheck_out = Dataset(name="sumcheck_out_%s.csv" % subject, open_mode='wb')
        ds_sumcheck_report2 = Dataset(name="sumcheck_report2_%s.csv" % subject, open_mode='wb')
        ds_out = Dataset(name="conv_out1_%s.cvs" % subject,open_mode='wb')
        ds_report2 = Dataset(name="conv_out0_%s.cvs" % subject,open_mode='wb')
    dict_std_subs = {r'&ctpath_breach\\': ctPath_breach, r'&ctpath.\\': ctPath }
    return airassessmentreporting.datacheck.raw_converter.raw_converter(
         grade='all',
         form=form, 
         subject=subject, 
         ds_raw_scores=ogtReg,
         ds_standards=ds_standards,
         odict_loc_subs=dict_std_subs,
         ds_sumcheck_out=ds_sumcheck_out,
         ds_sumcheck_report2=ds_sumcheck_report2,
         ds_out=ds_out, 
         #compare ds_out
         ds_report2=ds_report2,
         )
        
def call_rescore_check(q, table, bookmapFile, bkmap_breach_basepath, bkmap_basepath, server=None, db=None, conn=None):
    subject = q.get()
    if ("social_studies"):
        abrv = "c"
    elif ("science"):
        abrv = "s"
    elif ("writing"):
        abrv = "w"
    elif ("math"):
        abrv = "m"
    elif ("reading"):
        abrv = "r"
    bookMap = Dataset(dbms='excel_srcn',
            workbook_file=(bookmapFile),
            sheet_name=None,
            open_mode='rb')
    ogtReg = Dataset(dbms='pyodbc', conn=conn, server=server, db=db,
                     query=""" 
                      SELECT
                       t.id, t.ucmx_form, s.id, s.up%sx_finalraw_item, s.up%sx_score_item
                      FROM %s t
                       join mc_table_%s s on (t.id = s.flat_table_id)
                     """ % (abrv,abrv,table,abrv),
                     columns = ['test_id', 'form_id', 'item_id', 'finalraw_item',
                                'score_item'],
                     open_mode='rb')
    rescoreOut = Dataset(
      name="rescore_out_%s_%s.cvs" % (table,abrv),open_mode='wb')                  
    rescoreReportOut = Dataset(
      name="rescore_report_out_%s_%s.cvs" % (table, abrv),open_mode='wb')                  
    dict_bml_subs = {
      r'&ctpath_breach\\': bkmap_basepath, 
      r'&bkmap_breach_basepath.\\': bkmap_breach_basepath}
    return airassessmentreporting.datacheck.rescore.rescorecheck(grade='All', 
             subject=subject, 
             ds_input=ogtReg, 
             ds_bookmaplocs=bookMap,
             ds_out=rescoreOut, 
             odict_loc_subs=dict_bml_subs,
             ds_report2=rescoreReportOut,
             bml_form_style = 1 
             )


if __name__ == '__main__':

    starttime = datetime.datetime.now()
    
    print "started" + str(starttime)
    
    run_preqc = 1
    run_sql = 1
    test_idcheck = 1
    test_longcomp = 1
    test_rescorecheck = 1
    test_converter = 1
    run_cleanup = 0
    
    #runContext = RunContext('unittest')
    
    connect_time = datetime.datetime.now()
    
    
    print "Got db connection at time: " , str(connect_time)
    print "Time required to get connection:" , str(connect_time - starttime)
    ###### FILE NAMES ######
    ### 2012 ###
    bkmap_breach_basepath = "H:/share/Ohio Graduation Tests/Test Development/Bookmaps and 1x1s/Breach 2012/"
    ctPath_breach = "H:/share/Ohio Graduation Tests/Technical/Conversion Tables/2012 Breach/"
    ### FALL 2012 ###
    runContextName = 'OGT_12FA'
    studentDTN = '[Python_OGT_11FA].[dbo].sas_student_'
    baseDirectory = 'C:\\CVS Projects\\CSSC Score Reporting\\OGT Fall 2012\\'
    layoutFile = baseDirectory+'Intake Layout\\OGT_FA12_Op_DataLayout_IntakeLayout.xls'
    preqcInputFile = "H:/share/Ohio Graduation Tests/Technical/2012 October/ScoreReports/TextFileFromDRC/536215_2012OhioOGTFall_Regular.txt"
    ctPath = "H:/share/Ohio Graduation Tests/Technical/Conversion Tables/2012 October/"
    bkmap_basepath = "H:/share/Ohio Graduation Tests/Test Development/Bookmaps and 1x1s/Fall 2012/"
    
    ### SUMMER 2012 ###
    #runContextName = 'OGT_12SU'
    #studentDTN = '[Python_OGT_11SU].[dbo].sas_student_'
    #baseDirectory = 'C:\\CVS Projects\\CSSC Score Reporting\\OGT Summer 2012\\'
    #layoutFile = baseDirectory+'IntakeLayout\\OGT_SU12_Op_DataLayout_IntakeLayout.xls'
    #preqcInputFile = "H:/share/Ohio Graduation Tests/Technical/2012 July/ScoreReports/TextFileFromDRC/536214_2012OhioOGTSummer_Regular.txt"
    #ctPath = "H:/share/Ohio Graduation Tests/Technical/Conversion Tables/2012 July/"
    #bkmap_basepath = "H:/share/Ohio Graduation Tests/Test Development/Bookmaps and 1x1s/Summer 2012/"
    
    ### COMMON FILES ###
    bmFile = baseDirectory+"Code\Development\Intake\BookMapLocations.xls"
    bmFileBreach = baseDirectory+"Code\Development\Intake\BookMapLocations_breach.xls"
    idCheckVariableFile = baseDirectory+"Code\Development\Intake\OGT_ID_Sheet.xls"
    longCompFile = baseDirectory+"Code\Development\Intake\OGT_LongComp.xls"
    idCheckVariableFile_HomeDistrict=baseDirectory+"Code\Development\Intake\OGT_ID_HomeDistrict_Sheet.xls"
    semanticBreachFile = baseDirectory+"Code\\Development\\Intake\\OGT_Semantic_breach.xls"
    semanticFile = baseDirectory+"Code\\Development\\Intake\\OGT_Semantic.xls"
    
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()
    conn = dbContext.conn
    
    NUM_CORES = 4
    ###### DB TABLE NAMES ######
    preQcDTN = 'pre_qc_flat_table'
    masterDTN = 'masterList'
    
    preqctime_s = datetime.datetime.now()
        
        
    if run_preqc:
        '''
        pre = preqc.PreQC(runcontext=runContext, dbcontext=dbContext, 
                        layoutfile=layoutFile,
                        inputfile=preqcInputFile,
                        patterns=[ ('[u][pcf][w]x_.*{icnt}.*','[u][pcf][w]x__OE.*Pos.*{icnt}','[u][pcf][w]x_OE_.*{icnt}','MC_TABLE_W','W')
                                  ,('[u][pcf][c]x_.*{icnt}.*','[u][pcf][c]x__OE.*Pos.*{icnt}','[u][pcf][c]x_OE_.*{icnt}','MC_TABLE_C','C')
                                  ,('[u][pcf][s]x_.*{icnt}.*','[u][pcf][s]x__OE.*Pos.*{icnt}','[u][pcf][s]x_OE_.*{icnt}','MC_TABLE_S','S')
                                  ,('[u][pcf][r]x_.*{icnt}.*','[u][pcf][r]x__OE.*Pos.*{icnt}','[u][pcf][r]x_OE_.*{icnt}','MC_TABLE_R','R')
                                  ,('[u][pcf][m]x_.*{icnt}.*','[u][pcf][m]x__OE.*Pos.*{icnt}','[u][pcf][m]x_OE_.*{icnt}','MC_TABLE_M','M')],
                        debug=False,flat_tablename=preQcDTN,bulk_insert=False, errorfile='c:\SAS\OGT\Error.txt')
        pre.process()
        '''
        
        #prepare input file here
        f = open(preqcInputFile, 'r')
        lines = f.readlines()
        
        if len(lines) % NUM_CORES == 0:
            length = len(lines)/NUM_CORES
        else:
            length = (len(lines)/NUM_CORES)+1
        files = [open('C:\SAS\OGT\intake%s.txt' % i, 'w') for i in range(NUM_CORES)]
        
        def chunks(l, n):
            for i in xrange(0, len(l), n):
                yield l[i:i+n]
                
        perfile = list(chunks(lines, length))
        for i,x in enumerate(files):
            [x.write(y) for y in perfile[i]]
        [fh.close() for fh in files]
        files = ['C:\SAS\OGT\intake%s.txt' % i for i in range(NUM_CORES)]
        q0 = Queue()
        q1 = Queue()
        [q0.put(x) for x in files]
        lock = Lock()
        rn = [1]
        for i,x in enumerate(perfile):
            rn.append(rn[i]+len(x))
        ps = [Process(target=call_preqc, args=(q0, lock, runContextName, layoutFile, preQcDTN+'_'+str(i), i, rn[i])) for i,x in enumerate(files)]
        [p.start() for p in ps]
        [p.join() for p in ps]
        
        subsql_s = datetime.datetime.now()
        itemtables = ['mc_table_c', 'mc_table_m', 'mc_table_r', 'mc_table_s', 'mc_table_w']
        tables = ['pre_qc_flat_table'] + itemtables
        drop_tables(tables, dbContext)
        #REID_TABLES = """
        #    DECLARE @mycnt INT;
        #    SET @mycnt = (SELECT count(id) from pre_qc_flat_table_0) 
        #    {fs:delimiter=' ; ', item='X',itemfmt=' update {table}_{{X}} set {id_field}={id_field}+@mycnt from {table}_{{X}} SET @mycnt = @mycnt + (SELECT count(id) from pre_qc_flat_table_{{X}}) '}
        #"""
        #dbContext.executeNoResults(REID_TABLES.format(table='pre_qc_flat_table', fs=Joiner(range(1, NUM_CORES)), id_field='id'))
        
        #for table in itemtables:
        #    dbContext.executeNoResults(REID_TABLES.format(table=table, fs=Joiner(range(1, NUM_CORES)), id_field='flat_table_id'))
        subsql_m = datetime.datetime.now()
        
        COMBINE_TABLES = """
            select * into {table} from (
            {fs:delimiter=' union all ', item='X',itemfmt='select * from {table}_{{X}}'}
            ) as tmp
        """
        for table in tables:
            dbContext.executeNoResults(COMBINE_TABLES.format(table=table, fs=Joiner(range(NUM_CORES))))
        
    preqctime_e = datetime.datetime.now()
        
    # Drop tables to recreate them for more utility runs.
    #TODO change the drop tables code to something more efficient
    if run_sql:
        tables = ['idCheckHomeDistrict2', 'idCheckOutput2', 'missingFromMaster', 
                  'missingFromMaster2', 'intake0', 'DiscrepancyReport', 'conflictingRecords', 
                  'missingCounty', 'missingCounty2', 'missingDioceseId', 'intake1', 
                  'intakeDummy', 'homeDistrict', 'intakeFinal', 'intakeBreach', 'intakeRegular', 
                  'idCheckFailcnt', 'bad_bcrxid', 'bad_dcrxid', 'bad_schoolnames']
        drop_tables(tables, dbContext)
    
    query = """
    DECLARE @mine TABLE 
    (
        Resolution VARCHAR(150),
        Reason VARCHAR(150),
        ufrx_breach BIT,
        ufwx_breach BIT,
        ufmx_breach BIT,
        ufsx_breach BIT,
        ufcx_breach BIT,
        form_r VARCHAR(8),
        form_m VARCHAR(8),
        form_w VARCHAR(8),
        form_s VARCHAR(8),
        form_c VARCHAR(8),
        ufrx_invalid INT,
        ufwx_invalid INT, 
        ufmx_invalid INT,
        ufsx_invalid INT,
        ufcx_invalid INT
    )
    INSERT INTO @mine VALUES ('','',0,0,0,0,0,'','','','','',0,0,0,0,0);
    
    SELECT
        P.*, M.*
        INTO intake0
        FROM {preqc} P
        join @mine M on (1=1);
        
    UPDATE intake0 SET dcxx_county = SUBSTRING(M.dcxx_county,0,31)
    FROM intake0 O
    JOIN {masterList} M ON (M.bcrxid_c = O.bcrxid_attend)
    
    UPDATE intake0 SET dcxx_county=''
    FROM intake0 O
    LEFT JOIN {masterList} M ON (M.bcrxid_c = O.bcrxid_attend)
    WHERE M.dcxx_county IS NULL
    
    
    --These two lines are not in Summer 2012
    --UPDATE intake0 SET dcrxid_attend='046755' where lithocode='10080895'; 
    --UPDATE intake0 SET dcrxid_attend='048025' where lithocode='10087344'; 
    
    UPDATE intake0 SET dcrxid_home='', bcrxid_home='' WHERE dcrxid_home = dcrxid_attend; 
    UPDATE intake0 SET bcrxid_home='' WHERE bcrxid_attend = bcrxid_home; 
    UPDATE intake0 SET dcrxid_home='', bcrxid_home='', dcrxnm_home='', bcrxnm_home='' WHERE dcrxid_home = bcrxid_attend; 
    UPDATE intake0 SET bcrxnm_attend = 'Home School', schtype_attend = 'H'  WHERE bcrxid_attend = '999999'; 
    
    SELECT O.Schtype_attend, O.lithocode, O.ssid, O.bcrxnm_attend, O.bcrxid_attend, O.dcrxid_attend, O.dcrxnm_attend 
    INTO missingFromMaster2 
    FROM intake0 O
    LEFT JOIN {masterList} M ON (M.bcrxid_c = O.bcrxid_attend)
    WHERE M.bcrxid_c IS NULL;
    
    SET ANSI_NULLS OFF;
    """.format(preqc=preQcDTN, masterList=masterDTN)
    
    UPDATE_OGT_MASTER = """
    UPDATE intake0 SET
    {updates} 
    FROM intake0 O
    LEFT JOIN %s M ON (M.bcrxid_c = O.bcrxid_attend)
    WHERE {conds}
    AND (M.bcrxid_c IS NOT NULL);
    """ % masterDTN
    
    query = "\n".join([query, 
    UPDATE_OGT_MASTER.format(updates="dcrxid_home = O.dcrxid_attend",
                                   conds="O.bcrxid_home = '' AND O.dcrxid_attend <> M.dcrxid_c AND O.dcrxid_attend <> O.bcrxid_attend"),
    UPDATE_OGT_MASTER.format(updates="Reason = 'DistrictID'",
                                   conds="O.dcrxid_attend <> M.dcrxid_c"),
    UPDATE_OGT_MASTER.format(updates="Reason = LTRIM(RTRIM(O.Reason)) + '*' + 'DistrictName'",
                                   conds="SUBSTRING(LOWER(O.dcrxnm_attend),0,31) <> SUBSTRING(LOWER(M.dcrxnm),0,31)"),
    UPDATE_OGT_MASTER.format(updates="Reason = LTRIM(RTRIM(O.Reason)) + '*' + 'SchoolName'",
                                   conds="LOWER(O.bcrxnm_attend) <> SUBSTRING(LOWER(M.bcrxnm),0,31)"),
    UPDATE_OGT_MASTER.format(updates="Reason = LTRIM(RTRIM(O.Reason)) + '*' +'SchoolType'",
                                   conds="LOWER(O.Schtype_attend) <> LOWER(M.Schtype)"),
    UPDATE_OGT_MASTER.format(updates="Reason = LTRIM(RTRIM(O.Reason)) + '*' +'DistrictType'",
                                   conds="LOWER(O.distrtype_attend) <> LOWER(M.distrtype)")])
    
    query = "\n".join([query, """
    SET ANSI_NULLS ON;
    
    ;
    WITH cte0 as (
        SELECT DISTINCT O.Reason, M.bcrxid_c, M.dcrxid_c, O.Schtype_attend, O.bcrxnm_attend, O.bcrxid_attend, O.dcrxid_attend, O.dcrxnm_attend 
        FROM intake0 O
        LEFT JOIN %s M ON (M.bcrxid_c = O.bcrxid_attend)
        WHERE M.bcrxid_c IS NOT NULL
        AND LTRIM(RTRIM(O.Reason)) <> ''
    ),
    cte1 as (SELECT *, ROW_NUMBER() OVER (PARTITION BY bcrxid_attend, Reason ORDER BY Reason DESC ) rowNumber FROM cte0)
    SELECT Reason, bcrxid_c, dcrxid_c, Schtype_attend, bcrxnm_attend, bcrxid_attend, dcrxid_attend, dcrxnm_attend
    INTO discrepancyReport FROM cte1 WHERE rowNumber = 1
    
    """ % (masterDTN),
    UPDATE_OGT_MASTER.format(updates="dcrxid_attend = SUBSTRING(M.dcrxid_c,0,11)",
                                   conds="CHARINDEX('DistrictID', O.Reason) > 0"),
    UPDATE_OGT_MASTER.format(updates="dcrxnm_attend = SUBSTRING(M.dcrxnm,0,31), bcrxnm_attend = SUBSTRING(M.bcrxnm,0,31)",
                                   conds="1=1"),
    UPDATE_OGT_MASTER.format(updates="schtype_attend = M.schtype",
                                   conds="CHARINDEX('SchoolType', O.Reason) > 0"),
    UPDATE_OGT_MASTER.format(updates="distrtype_attend = M.distrtype",
                                   conds="CHARINDEX('DistrictType', O.Reason) > 0")])
    
    query += """
    UPDATE intake0 SET dcrxid_home='' WHERE dcrxid_home = 'BBBBBB' AND schtype_attend = 'J'; 
    UPDATE intake0 SET Reason='DCRXID=BCRXID for JVS schools', Resolution='Not Fixed' WHERE bcrxid_attend=dcrxid_attend AND schtype_attend = 'J'; 
    UPDATE intake0 SET Reason='Missing dcrxid_home', Resolution='Not Fixed' WHERE dcrxid_home='' AND schtype_attend = 'J'; 
    SELECT * INTO conflictingRecords FROM intake0 WHERE (bcrxid_attend=dcrxid_attend AND schtype_attend = 'J') OR (dcrxid_home=''); 
    
    SET ANSI_NULLS OFF;
    UPDATE intake0 SET Reason='School type N has bcrxid <> dcrxid', Resolution='Fixed' WHERE schtype_attend='N' AND bcrxid_attend <> dcrxid_attend; 
    SET ANSI_NULLS ON;
    
    INSERT INTO conflictingRecords SELECT * FROM intake0 WHERE schtype_attend='N' AND bcrxid_attend <> dcrxid_attend; 
    
    SET ANSI_NULLS OFF;
    UPDATE intake0 SET dcrxid_home=dcrxid_attend, dcrxid_attend=bcrxid_attend, dcrxnm_attend=bcrxnm_attend, distrtype_attend='N' WHERE schtype_attend='N' AND bcrxid_attend <> dcrxid_attend; 
    SET ANSI_NULLS ON;
    
    UPDATE intake0 SET dcxx_county=dcrxnm_attend WHERE schtype_attend='D'; 
    
    SELECT bcrxnm_attend, bcrxid_attend, dcrxid_attend, dcxx_county, dcrxnm_attend, schtype_attend INTO missingCounty2 FROM intake0 WHERE LTRIM(RTRIM(dcxx_county))='' AND bcrxid_attend<>'999999';
    
    UPDATE intake0 SET form_r='B', ufrx_breach=1 WHERE ucrx_form='BR'; 
    UPDATE intake0 SET form_r='A', ufrx_breach=0 WHERE ucrx_form<>'BR'; 
    
    UPDATE intake0 SET form_m='B', ufmx_breach=1 WHERE ucmx_form='BR'; 
    UPDATE intake0 SET form_m='A', ufmx_breach=0 WHERE ucmx_form<>'BR'; 
            
    UPDATE intake0 SET form_w='B', ufwx_breach=1 WHERE ucwx_form='BR';
    UPDATE intake0 SET form_w='A', ufwx_breach=0 WHERE ucwx_form<>'BR'; 
    
    UPDATE intake0 SET form_s='B', ufsx_breach=1 WHERE ucsx_form='BR';
    UPDATE intake0 SET form_s='A', ufsx_breach=0 WHERE ucsx_form<>'BR'; 
    
    UPDATE intake0 SET form_c='B', ufcx_breach=1 WHERE uccx_form='BR';
    UPDATE intake0 SET form_c='A', ufcx_breach=0 WHERE uccx_form<>'BR'; 
            
    UPDATE intake0 SET dcrxid_home='', bcrxid_home='' WHERE dcrxid_home=dcrxid_attend; 
    UPDATE intake0 SET bcrxid_home='' WHERE bcrxid_attend=bcrxid_home; 
    UPDATE intake0 SET dcrxid_home='' WHERE schtype_attend='Y'; 
    UPDATE intake0 SET dcrxid_home='', bcrxid_home='', dcrxnm_home='', bcrxnm_home='' WHERE dcrxid_home=bcrxid_attend; 
    
    SELECT O.bcrxid_attend, O.bcrxnm_attend, O.schtype_attend INTO missingDioceseId
    FROM intake0 O
    LEFT JOIN {masterList} M ON (M.bcrxid_c = O.bcrxid_attend)
    WHERE (O.schtype_attend = 'D' and M.schtype <> 'D') or (M.schtype='D' and O.schtype_attend <> 'D')
    
    SELECT
    M.distrtype, M.dcrxnm, O.*
    INTO intake1
    FROM intake0 O
    LEFT JOIN (SELECT DISTINCT distrtype, dcrxnm, dcrxid_c from {masterList}) M ON (M.dcrxid_c = O.dcrxid_attend)
    
    UPDATE intake1
    SET bcrxid_home = P.bcrxid_home
    FROM intake1 O
    JOIN {preqc} P ON (P.lithocode = O.lithocode)
    
    SET ANSI_NULLS OFF;
    UPDATE intake1 SET dcrxnm_attend=SUBSTRING(dcrxnm,0,31), distrtype_attend=distrtype WHERE schtype_attend='H' AND LOWER(dcrxnm_attend) <> SUBSTRING(LOWER(dcrxnm),0,31);
    SET ANSI_NULLS ON;
    
    SELECT *
    INTO intakeFinal
    FROM intake1
    ALTER TABLE intakeFinal DROP COLUMN Reason, distrtype, dcrxnm, resolution, missing_grade, missing_dob_year, missing_upxx_wawc_firstprompt, missing_ufxx_test_type, missing_dob_month, missing_upxx_wawc_secondprompt, missing_dob_day;
    
    UPDATE intake1 SET ethnicity='7'  where ethnicity NOT IN ('1','2','3','4','5','6'); 
    
    SELECT * INTO intakeDummy FROM intake1 WHERE LTRIM(RTRIM(dcrxid_home)) <> '';
    UPDATE intakeDummy SET dcrxid_attend=dcrxid_home, dcrxnm_attend=dcrxnm_home, distrtype_attend=distrtype_home FROM intakeDummy
    
    SELECT dcrxid_attend, dcrxnm_attend, distrtype_attend, row_number() over (order by (select 0)) as id
    INTO homeDistrict 
    FROM (SELECT id, dcrxid_attend, dcrxnm_attend, distrtype_attend FROM intake1
    UNION ALL
    SELECT id, dcrxid_attend, dcrxnm_attend, distrtype_attend FROM intakeDummy) as tmp;
    UPDATE homeDistrict SET dcrxnm_attend=LTRIM(RTRIM(LOWER(dcrxnm_attend)));
    
    """.format(preqc=preQcDTN, masterList=masterDTN)
    
    
    if run_sql:
        dbContext.executeNoResults( query ) 
    
    
    # Get connection info for Dataset class
    server=dbContext.server
    database=dbContext.db
    
    if test_idcheck == 1:
        print "\n*************** TESTING IDCHECK *************\n"
        drop_tables(['idCheckHomeDistrict', 'idCheckOutput'], dbContext)
        airassessmentreporting.datacheck.idcheck.idCheckSheet(idCheckVariableFile, "intake1", 'idCheckOutput', runContext, dbContext)
    sqltime_e = datetime.datetime.now()
    
    if test_longcomp == 1:
        ogt = Dataset(dbms='pyodbc', conn=conn,
        query="""SELECT
          schtype_attend, upxxlep, upxxiep, bcrxid_attend, dcrxid_attend,
          migrant, ucrxgen, grade, ethnicity
          FROM intake1;
                    """,
                    columns = ['schtype_attend','upxxlep','upxxiep','bcrxid_attend','dcrxid_attend','migrant','ucrxgen','grade','ethnicity'],
                    open_mode='rb')
        # NOTE: longcomp() uses its input datasets sequentially, so can reuse
        # same connection 'conn'
        student = Dataset(dbms='pyodbc', conn=conn,
                    query="""
                     SELECT
                     C.schtype_attend, C.upxxlep, C.upxxiep,
                     C.bcrxid_attend, C.dcrxid_attend, C.migrant, C.ucrxgen,
                     C.grade, C.ethnicity
                     FROM {studentList}0 A
                     JOIN {studentList}1 B ON (B.id = A.id)
                     JOIN {studentList}2 C ON (C.id = A.id)
                    """.format(studentList=studentDTN),
                    columns = ['schtype_attend','upxxlep','upxxiep','bcrxid_attend','dcrxid_attend','migrant','ucrxgen','grade','ethnicity'],
                    open_mode='rb')
        longcompsheet = Dataset(dbms='excel_srcn', 
                               workbook_file=longCompFile,
                               sheet_name=None, 
                               open_mode='rb')
        full_report = Dataset(name="datacheck_report_0.cvs",open_mode='wb')                  
        #brief_report = Dataset(name="datacheck_report_1.cvs",open_mode='wb')                  
        brief_report = Dataset(dbms='pyodbc', conn=conn,
          table='longCompReport', replace=True, 
          columns=["ByVariable","ByValue","Variable","Value","MatchType",
          "base","compare","diff","Comparison","baseN","compareN"],
          open_mode='wb')
        output = airassessmentreporting.datacheck.longcomp.longcomp(
          dsr_time0=student, 
          dsr_time1=ogt, 
          dsr_longcomp=longcompsheet ,
          dsw_full_report=full_report ,
          dsw_brief_report=brief_report  )
        conn.commit()
        del brief_report
    longcomptime_e = datetime.datetime.now()
    # Delete extant tables before populating with source data.
    query="""
    
    SELECT bad_variable, bad_reason, count(*) as count 
    INTO idCheckFailcnt 
    FROM idCheckOutput
    group by bad_variable, bad_reason;
    
    SELECT A.bad_variable, A.bad_value, A.bad_label,A.id  
    INTO bad_bcrxid 
    FROM idCheckOutput A
    WHERE bad_variable = 'bcrxid_attend';
    
    SELECT A.bad_variable, A.bad_value, A.bad_label,A.id,B.dcrxnm_attend,B.dcrxnm_home 
    INTO bad_dcrxid
    FROM idCheckOutput A
    LEFT JOIN intake1 B ON (A.id=B.id)
    WHERE A.bad_variable = 'dcrxid_attend' OR A.bad_variable='dcrxid_home';
    
    SELECT distinct bad_value as bcrxid 
    INTO bad_schoolnames 
    FROM idCheckOutput
    WHERE bad_variable = 'bcrxid_attend';
    
    ;WITH cte0 AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY [bcrxid_attend] ORDER BY [bcrxid_attend]) rowNumber FROM [missingFromMaster2])
    SELECT * 
    INTO missingFromMaster
    FROM cte0 WHERE rowNumber = 1;
    DROP TABLE missingFromMaster2;
    
    ;WITH cte0 AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY [dcrxid_attend] ORDER BY [dcrxid_attend]) rowNumber FROM [missingCounty2])
    SELECT * 
    INTO missingCounty
    FROM cte0 WHERE rowNumber = 1;
    DROP TABLE missingCounty2;
    """
    if run_sql:
        dbContext.executeNoResults( query ) 
    
    if test_idcheck == 1:
        print "\n*************** TESTING IDCHECK 2 *************\n"
        airassessmentreporting.datacheck.idcheck.idCheckSheet(idCheckVariableFile_HomeDistrict, "homeDistrict", 'idCheckHomeDistrict', runContext, dbContext)
    
    query="""
    UPDATE intake1 SET ucrx_form='XX' WHERE ucrx_form=NULL;
    UPDATE intake1 SET ucmx_form='XX' WHERE ucmx_form=NULL;
    UPDATE intake1 SET ucwx_form='XX' WHERE ucwx_form=NULL;
    UPDATE intake1 SET ucsx_form='XX' WHERE ucsx_form=NULL;
    UPDATE intake1 SET uccx_form='XX' WHERE uccx_form=NULL;
    
    SELECT * 
    INTO intakeBreach 
    FROM intake1
    WHERE ufrx_breach=1
    
    SELECT * 
    INTO intakeRegular
    FROM intake1
    WHERE ufrx_breach<>1
    """
    if run_sql:
        dbContext.executeNoResults( query ) 
    sql2time_e = datetime.datetime.now()
    
    if test_rescorecheck == 1:
        print "\n*************** TESTING rescorecheck() *************\n"
        subjects = ["reading", "math", "writing", "science", "social_studies"]
        q0 = Queue()
        q1 = Queue()
        [q0.put(s) for s in subjects]
        [q1.put(s) for s in subjects]
        ps = [Process(target=call_rescore_check, args=(q0, 'intakeRegular', bmFile, bkmap_breach_basepath, bkmap_basepath, server, database, None)) for s in subjects]
        ps = ps + [Process(target=call_rescore_check, args=(q1, 'intakeBreach', bmFileBreach, bkmap_breach_basepath, bkmap_basepath, server, database, None)) for s in subjects]
        [p.start() for p in ps]
        [p.join() for p in ps]
    rescoretime_e = datetime.datetime.now()
    
    if test_converter == 1:
        print "\n**************** TESTING raw_converter() ***************\n"
        subjects = ["reading", "math", "writing", "science", "soc_stud"]
        q0 = Queue()
        q1 = Queue()
        [q0.put(s) for s in subjects]
        [q1.put(s) for s in subjects]
        ps = [Process(target=call_raw_converter, args=(q0,'intakeRegular', semanticFile, ctPath_breach, ctPath, None, server, database, None)) for i,s in enumerate(subjects)]
        ps = ps + [Process(target=call_raw_converter, args=(q1,'intakeBreach', semanticBreachFile, ctPath_breach, ctPath, "BR", server, database, None)) for i,s in enumerate(subjects)]
        [p.start() for p in ps]
        [p.join() for p in ps]
    convertertime_e = datetime.datetime.now()
    
    
    print "Time elapsed: " + str(datetime.datetime.now() - starttime)
    print "preqc: " + str(preqctime_e - preqctime_s)
    print "sql: " + str(sqltime_e - preqctime_e)
    print "longcomp: " + str(longcomptime_e - sqltime_e)
    print "sql2: " + str(sql2time_e - longcomptime_e) 
    print "rescore: "  + str(rescoretime_e - sql2time_e)
    print "convertertime: " + str(datetime.datetime.now() - rescoretime_e)
    
    if run_cleanup:
        tables = ['missingFromMaster2', 'intake0', 'intake1', 'intakeDummy', 'homeDistrict', 'intakeBreach', 'intakeRegular']
        drop_tables(tables, dbContext)
    