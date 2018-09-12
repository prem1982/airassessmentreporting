import os
import datetime
import xlrd
from multiprocessing import Queue, Process
from airassessmentreporting.airutility import (RunContext, Joiner)
from airassessmentreporting.airutility.dbutilities import drop_tables 
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, cm, inch
from reportlab.platypus import Image, SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet

#genAll = True
genAll = False 
datamove = False 
#datamove = True 
#pdfGen = False
pdfGen = True 

if __name__ == '__main__':
    runContextName = 'OGT_12FA'
    runContext = RunContext(runContextName)
    dbContext = runContext.getDBContext()

    if genAll:
        grade_list = range(10,15)
        subject_list= ['r', 'm', 'w', 's', 'c']
        subject_description_list = ['Reading', 'Math', 'Writing', 'Science', 'Social Studies']
        aggregation_levels = ['Public Schools', 'Nonpublic Schools', 'Community Schools']
    else: # For testing
        grade_list = range(10,11)
        subject_list= ['r']
        subject_description_list = ['Reading']
        aggregation_levels = ['Public Schools']
        
    dirs = ['State Reports of All Public Schools', 'State Reports of All Nonpublic Schools', 'State Reports of All Community Schools']
    dirs2 = ['District Reports', 'School Reports', 'School Reports']
    aggregation_condition = ["schtype not in('N', 'H', 'D')", "schtype in ('N', 'D')", "schtype='C'"]
    admin_date = 'Spring 2012'
    
    strandLevs = {}
    strandLevs['r'] = ['upralev', 'uprilev', 'uprllev', 'uprrlev']
    strandLevs['m'] = ['upmnlev', 'upmdlev', 'upmmlev', 'upmglev', 'upmalev']
    strandLevs['w'] = ['upwclev', 'upwalev', 'upwplev']
    strandLevs['s'] = ['upselev', 'upsplev', 'upsllev', 'upsslev']
    strandLevs['c'] = ['upcelev', 'upchlev', 'upcmlev', 'upcslev']
    
    fmts = {}
    fmts['r']=["                Acquisition of Vocabulary", "                      Informational Text", "                          Literary Text", "                       Reading Process"]
    fmts['m']=["Number, Number Sense and Operations", "Data Analysis and Probability", "          Measurement", "Geometry and Spatial Sense", "Patterns, Functions and Algebra"]
    fmts['w']=["     Writing Conventions", "     Writing Applications", "     Writing Processes"]
    fmts['s']=["Earth and Space Sciences", "   Physical Sciences", "     Life Sciences", "Scientific Processes: Inquiry, Technology, and Ways of Knowing"]    
    fmts['c']=["Economics, Government & Citizenship Rights and Responsibilities", "               History", "Social Studies Skills and Methods", "People in Societies and Geography"]
    
    #logfile = "H:\share\Ohio Graduation Tests\Technical\LibCD\Spr12\QC\librarycd_cs_06132012.txt"
    #cvsroot = 'C:\CVS Projects\'
    #root='H:\\share\\Ohio Graduation Tests\\Technical\\LibCD\\Spr12\\'
    root='C:\\dvds_p\\'
    ctpath = 'H:\\share\\Ohio Graduation Tests\\Technical\\Conversion Tables\\2012 March\\Final'
    agg_file = 'H:\\share\\Ohio Graduation Tests\\Technical\\LibCD\\Spr12\\AggregationSheet.xls'
    logo = "H:\\share\\Ohio Graduation Tests\\Technical\\LibCD\\Spr12\\ODE_logo.bmp"
    #input "H:\share\Ohio Graduation Tests\Technical\2012 March\ScoreReports\Consolidateddata";
    #QC "H:\share\Ohio Graduation Tests\Technical\LibCD\Spr12\QC";
    #student_table = 'Python_OGT_12SP.dbo.sas_student_'
    student_table = 'Python_OGT_12SP.dbo.sas_alternate_' #the student table didn't match the previous student table by about 12 entries???
    
    
    if datamove:
        for x in grade_list:
            #drop_tables(['student_g%s' % x, 'student2_g%s' % x], dbContext)
            drop_tables(['student_g%s_0' % x], dbContext)
            drop_tables(['student_g%s_1' % x], dbContext)
            drop_tables(['student_g%s_2' % x], dbContext)
    for grade in grade_list:
        #dbContext.executeNoResults("SELECT * INTO student_g%s FROM %s2 WHERE grade = '%s' AND bcrxid_attend NOT IN ('000001', '000002', '000003', '999999')" % (grade, student_table, grade))
        
        if datamove:
            #TODO optimize this we don't really need student_g%s_0 do we?
            dbContext.executeNoResults("SELECT A.* INTO student_g%s_0 FROM %s0 A JOIN %s1 B ON (A.lithocode = B.lithocode) JOIN %s2 C ON (A.lithocode = C.lithocode) WHERE C.grade = '%s' AND C.bcrxid_attend NOT IN ('000001', '000002', '000003', '999999')" % (grade, student_table, student_table, student_table, grade))
            dbContext.executeNoResults("SELECT B.* INTO student_g%s_1 FROM %s0 A JOIN %s1 B ON (A.lithocode = B.lithocode) JOIN %s2 C ON (A.lithocode = C.lithocode) WHERE C.grade = '%s' AND C.bcrxid_attend NOT IN ('000001', '000002', '000003', '999999')" % (grade, student_table, student_table, student_table, grade))
            dbContext.executeNoResults("SELECT C.* INTO student_g%s_2 FROM %s0 A JOIN %s1 B ON (A.lithocode = B.lithocode) JOIN %s2 C ON (A.lithocode = C.lithocode) WHERE C.grade = '%s' AND C.bcrxid_attend NOT IN ('000001', '000002', '000003', '999999')" % (grade, student_table, student_table, student_table, grade))
            dbContext.executeNoResults("UPDATE student_g%s_2 SET dcrxid_attend=bcrxid_attend, dcrxnm_attend=bcrxnm_attend, dcxx_county=dcrxnm_attend WHERE schtype = 'D';" % grade)
    
        #use these instead of:
        #bcrxnm=bcrxnm_attend;
        #dcrxnm=dcrxnm_attend;
        #dcrxid=dcrxid_attend;
        #bcrxid=bcrxid_attend;
    
    csf_dir = root+'Content Standard Frequency\\'
    if not os.path.exists(csf_dir):
        os.makedirs(csf_dir)
    
    
    for i,agg_lvl in enumerate(aggregation_levels):
        report_type = agg_lvl[0:3]
        group = report_type
        drop_tables(['state_out_%s' % group], dbContext)
        dbContext.executeNoResults("""CREATE TABLE state_out_%s (
                                        strand INT,
                                        proflev INT,
                                        --proflev NVARCHAR(30), 
                                        freq INT, 
                                        percent_freq FLOAT, 
                                        cum_freq INT, 
                                        cum_percent_frequency FLOAT,
                                        grade INT,
                                        subject NVARCHAR(1),
                                        flag BIT
                                        );""" % group)
        agg_dir = csf_dir+agg_lvl+'\\'
        if not os.path.exists(agg_dir):
            os.makedirs(agg_dir)
        sub_dir = agg_dir + dirs[i] + '\\'
        if not os.path.exists(sub_dir):
            os.makedirs(sub_dir)
        sub_dir2 = agg_dir + dirs2[i] + '\\'
        if not os.path.exists(sub_dir2):
            os.makedirs(sub_dir2)
        
        #temporary? really create it everytime?
        #for g in grade_list:
        #    drop_tables(['student2_g%s' % g], dbContext)
        
        for j,grade in enumerate(grade_list):
            grade_dir = sub_dir + 'grade' + str(grade) + '\\'
            if not os.path.exists(grade_dir):
                os.makedirs(grade_dir)
                
                
            # TODO change Python_OGT_12SP to something else
            # TODO don't use the sas tables
            cols = dbContext.execute("""
               SELECT column_name FROM Python_OGT_12SP.INFORMATION_SCHEMA.COLUMNS 
                where 
                (
                table_name = 'sas_student_0' 
                or table_name = 'sas_student_1' 
                or table_name = 'sas_student_2' 
                ) AND (
                 UPPER(column_name) like '%%LEV'
                 or UPPER(column_name) like '%%XSCAL'  
                 or UPPER(column_name) like '%%XIEP' 
                 or UPPER(column_name) like '%%XLEP' 
                )
                """)
            cols = [c[0] for c in cols]
            cols += ['dcxx_county', 'ethnicity', 'ucrxgen', 'dcrxid_attend', 'bcrxid_attend', 'bcrxnm_attend', 'dcrxnm_attend', 'schtype']
            
            #dbContext.executeNoResults("SELECT {cols} INTO student2_g{grade} FROM student_g{grade}_0 A JOIN student_g{grade}_1 B ON (A.lithocode = B.lithocode) JOIN student_g{grade}_2 C ON (A.lithocode = C.lithocode) WHERE {agg_cond}".format(cols=Joiner(cols), grade=grade, agg_cond=aggregation_condition[i] ) )
            
            #TODO filter out up[m/r]xscal in ['', NULL, '.', 999, and other invalids here]?
                #looks like this isn't needed, just don't generate a report for these values
            for s,subject in enumerate(subject_list):
                
                '''
                for m,strand in enumerate(strandLevs[subject]):
                    drop_tables(['strand_freq'+str(m)], dbContext)
                    dbContext.executeNoResults("""
                        declare @total float;
                        set @total = 0;
                        set @total = (select count(*) from student2_g{grade} where {strand} IS NOT NULL)
                        select 
                            {strand_num} as strand,
                            {strand} as proflev,
                            (select count(*) from student2_g{grade} where {strand} = rt.{strand}) as freq,
                            (select count(*) from student2_g{grade} where {strand} = rt.{strand})/@total*100 as percent_freq,
                            (select count(*) from student2_g{grade} where {strand} <= rt.{strand}) as cum_freq,
                            (select count(*) from student2_g{grade} where {strand} <= rt.{strand})/@total*100 as cum_percent_frequency
                            
                            --(select count(*) from student2_g{grade} where {strand} = rt.{strand} OR ({strand} IS NULL AND rt.{strand} IS NULL)) as freq,
                            --(select count(*) from student2_g{grade} where {strand} = rt.{strand} OR ({strand} IS NULL AND rt.{strand} IS NULL))/@total as percent_freq,
                            --(select count(*) from student2_g{grade} where {strand} <= rt.{strand} OR ({strand} IS NULL)) as cum_freq,
                            --(select count(*) from student2_g{grade} where {strand} <= rt.{strand} OR ({strand} IS NULL))/@total as cum_percent_frequency
                            
                        into strand_freq{num}
                        from (select distinct {strand} from student2_g{grade}) as rt order by {strand}
                        --from (select distinct {strand} from student2_g{grade} where {strand} in ('1','2','3')) as rt order by {strand}
                    """.format(strand=strand, grade=grade,num=m, strand_num=(m+1)))
                #sas computes these 4 stats (above) in 3 different ways for some reason - we are ignoring that
                
                drop_tables(['strand_freq'], dbContext)
                #TODO technically this below statement is a left join, so we might have to alter this
                #print ("select * into strand_freq from ({tables:delimiter=' union all ',item='X',itemfmt=' select * from strand_freq{{X}} where {strand} in (\"1\",\"2\",\"3\") '}) as tmp".format(tables=Joiner(range(m)), strand=strand)) 
                #dbContext.executeNoResults("select * into strand_freq from ({tables:delimiter=' union all ',item='X',itemfmt=' select * from strand_freq{{X}} where {strand} in (\"1\",\"2\",\"3\") '}) as tmp".format(tables=Joiner(range(m)), strand=strand).replace('"',"'")) 
                
                #Note: below can be optimized
                dbContext.executeNoResults("select * into strand_freq from ({tables:delimiter=' union all ',item='X',itemfmt=' select * from strand_freq{{X}} '}) as tmp".format(tables=Joiner(range(m+1)))) 
                # TODO fix this flag?
                flag = 0
                dbContext.executeNoResults("insert into state_out_%s select *, %s, '%s', %s from strand_freq where proflev is not NULL" % (group, grade, subject, flag) )
                '''
                
                table = 'Python_OGT_12SP.dbo.sas_alternate_'
                flag = 0
                for m,strand in enumerate(strandLevs[subject]):
                    dbContext.executeNoResults("""
                        declare @total float;
                        set @total = 0;
                        set @total = (select count(*) from student2_g10 where {strand} is not null)
                        
                        insert into state_out_{group} 
                        select 
                            {strand_num} as strand,
                            {strand} as proflev,
                            (select count(*) from student2_g10 where {strand} = rt.{strand}) as num,
                            (select count(*) from student2_g10 where {strand} = rt.{strand})/@total*100 as precent_freq,
                            (select count(*) from student2_g10 where {strand} <= rt.{strand}) as cum_count,
                            (select count(*) from student2_g10 where {strand} <= rt.{strand})/@total*100 as cum_frequency,
                            {grade},
                            '{subject}',
                            0
                        from (select distinct {strand} from {table}0 A join {table}1 B ON (A.lithocode = B.lithocode) join {table}2 C ON (A.lithocode = C.lithocode) where {agg} AND C.bcrxid_attend NOT IN ('000001', '000002', '000003', '999999') AND C.grade = {grade} and {strand} IS NOT NULL) as rt order by {strand}
                    """.format(strand=strand, table=table, group=group, strand_num=(m+1), grade=grade, subject=subject, flag=flag, agg=aggregation_condition[i]))
                
                '''
                #TODO? if the first entry has cum_frequency_count (or cum_freq) of < 10 then set everything _new to NULL or '.' or '' or '-'
                Title2 = "Grade %s %s, %s" % (grade, subject, admin_date)
                '''
                
                # TODO the sas code is marking NULL/'.'/'' cum_frequency and cum_percent_frequency to 0 
                
                if pdfGen:
                    styles = getSampleStyleSheet()
                    elements = []
                    I = Image('H:\\share\\Ohio Graduation Tests\\Technical\\LibCD\\Spr12\\ODE_logo.bmp')
                    h0 = Paragraph('<para align=center fontName="Times-Roman"><b>Proficiency \nLevel</b></para>', styles["BodyText"])
                    h1 = Paragraph('<para align=center fontName="Times-Roman"><b>Frequency</b></para>', styles["BodyText"])
                    h2 = Paragraph('<para align=center fontName="Times-Roman"><b>Cumulative \nFrequency</b></para>', styles["BodyText"])
                    h3 = Paragraph('<para align=center fontName="Times-Roman"><b>Percent</b></para>', styles["BodyText"])
                    h4 = Paragraph('<para align=center fontName="Times-Roman"><b>Cumulative \nPercent</b></para>', styles["BodyText"])
                    
                    title0 = Paragraph('<para align=center fontName="Times-Roman" fontSize="14"><b><i>Content Standard Frequency Distribution</i></b></para>', styles["BodyText"])
                    title1 = Paragraph('<para align=center fontName="Times-Roman" fontSize="14"><b><i>Grade %s %s, %s</i></b></para>' % (grade, subject_description_list[s], admin_date), styles["BodyText"])
                    title2 = Paragraph('<para align=center fontName="Times-Roman" fontSize="14"><b><i>%s</i></b></para>' % (agg_lvl), styles["BodyText"])
                    title3 = Paragraph('<para> </para>', styles["BodyText"])
                    title4 = Paragraph('<para> </para>', styles["BodyText"])
                    elements.append(title0)
                    elements.append(title1)
                    elements.append(title2)
                    elements.append(title3)
                    elements.append(title4)
                    
                    data = []
                    data.append([h0, h1, h2, h3, h4])
                    for n,fmt in enumerate(fmts[subject]):
                        sql_data = dbContext.execute("select * from state_out_%s where grade=%s AND subject='%s' AND strand=%s" % (group, grade, subject, n+1))
                        data.append(['', fmt])
                        data.append(['Above Proficient', sql_data[2][2], sql_data[2][4], '%.1f%%' % sql_data[2][3], '%.1f%%' % sql_data[2][5] ])
                        data.append(['Near Proficient', sql_data[1][2], sql_data[1][4], '%.1f%%' % sql_data[1][3], '%.1f%%' % sql_data[1][5] ])
                        data.append(['Below Proficient', sql_data[0][2], sql_data[0][4], '%.1f%%' % sql_data[0][3], '%.1f%%' % sql_data[0][5] ])
                    data.append([I])
                    data.append(['''Note: Data are not displayed if the total number of students is fewer than ten. The total number of
students may not agree with the Local Report Card because LRC accountability rules are not applied.
The cumulative frequency at each proficiency level is the number of students at or below that level,
and the cumulative percentages are the percentages of students at or below that level.'''])
                    heights = [0.4*inch] 
                    for fmt in fmts[subject]:
                        heights += [0.3*inch, 0.2*inch, 0.2*inch, 0.2*inch] 
                    heights += [0.5*inch, 0.7*inch]
                    t=Table(data,5*[0.9*inch],heights)
                    style =  [
                              ('ALIGN',     (0,0), (-1,0),'CENTER'),
                              ('BACKGROUND',(0,0), (-1,0),colors.darkgrey),
                              ('INNERGRID', (0,0), (-1,0), 0.25, colors.black),
                              ('BOX',       (0,0), (-1,0), 0.25, colors.black),
                             ]
                    for n,fmt in enumerate(fmts[subject]):
                        style += [
                                  ('BACKGROUND',(0,n*4+1),(-1,(n*4+1)),colors.lightgrey),
                               
                                  ('ALIGN',     (0,n*4+2), (-1,n*4+4),'CENTER'),
                                  ('FONTSIZE',  (0,n*4+2), (-1,n*4+4), 8),
                                  ('INNERGRID', (0,n*4+2), (-1,n*4+4), 0.25, colors.black),
                                  ('BOX',       (0,n*4+2), (-1,n*4+4), 0.25, colors.black),
                                 ]
                    style += [
                              ('FONTSIZE',  (0,-1), (-1,-1), 7),
                               
                              ('BOX',     (0,0), (-1,-1), 0.60, colors.black),
                              ('VALIGN',  (0,1), (-1,-3), 'MIDDLE'),
                              ('FONTNAME',(0,0), (-1,-1), 'Times-Roman'),
                             ]
                    t.setStyle(TableStyle( style ))
                    elements.append(t)
                    
                    # write the document to disk
                    doc = SimpleDocTemplate(grade_dir + subject_description_list[s] + '_CSFreq_StateReport.pdf', pagesize=letter)
                    doc.build(elements)
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                """
                if pdfGen:
                    styles = getSampleStyleSheet()
                    elements = []
                    I = Image('H:\\share\\Ohio Graduation Tests\\Technical\\LibCD\\Spr12\\ODE_logo.bmp')
                    h0 = Paragraph('<para align=center fontName="Times-Roman"><b>Proficiency \nLevel</b></para>', styles["BodyText"])
                    h1 = Paragraph('<para align=center fontName="Times-Roman"><b>Frequency</b></para>', styles["BodyText"])
                    h2 = Paragraph('<para align=center fontName="Times-Roman"><b>Cumulative \nFrequency</b></para>', styles["BodyText"])
                    h3 = Paragraph('<para align=center fontName="Times-Roman"><b>Percent</b></para>', styles["BodyText"])
                    h4 = Paragraph('<para align=center fontName="Times-Roman"><b>Cumulative \nPercent</b></para>', styles["BodyText"])
                    h5 = Paragraph('<para align=center fontName="Times-Roman"><b>Cumulative \nState \nPercent</b></para>', styles["BodyText"])
                    
                    title0 = Paragraph('<para align=center fontName="Times-Roman" fontSize="14"><b><i>Content Standard Frequency Distribution</i></b></para>', styles["BodyText"])
                    title1 = Paragraph('<para align=center fontName="Times-Roman" fontSize="14"><b><i>Grade 10 Reading, Spring 2012</i></b></para>', styles["BodyText"])
                    title2 = Paragraph('<para align=center fontName="Times-Roman" fontSize="14"><b><i>District: Abraxas-Mid Ohio ESC (118372)</i></b></para>', styles["BodyText"])
                    title3 = Paragraph('<para> </para>', styles["BodyText"])
                    title4 = Paragraph('<para> </para>', styles["BodyText"])
                    elements.append(title0)
                    elements.append(title1)
                    elements.append(title2)
                    elements.append(title3)
                    elements.append(title4)
                    
                    data= [[h0, h1, h2, h3, h4, h5],
                           ['', '', '  Acquisition of Vocabulary'],
                           ['Above Proficient', '-', '-', '-', '-', '100.0%'],
                           ['Near Proficient', '-', '-', '-', '-', '54.0%'],
                           ['Below Proficient', '-', '-', '-', '-', '14.9%'],
                           ['', '', '         Informational Text'],
                           ['Above Proficient', '-', '-', '-', '-', '100.0%'],
                           ['Near Proficient', '-', '-', '-', '-', '33.9%'],
                           ['Below Proficient', '-', '-', '-', '-', '13.3%'],
                           ['', '', '             Literary Text'],
                           ['Above Proficient', '-', '-', '-', '-', '100.0%'],
                           ['Near Proficient', '-', '-', '-', '-', '37.1%'],
                           ['Below Proficient', '-', '-', '-', '-', '12.4%'],
                           ['', '', '          Reading Process'],
                           ['Above Proficient', '-', '-', '-', '-', '100.0%'],
                           ['Near Proficient', '-', '-', '-', '-', '36.5%'],
                           ['Below Proficient', '-', '-', '-', '-', '16.0%'],
                           [I],
                           ['''Note: Data are not displayed if the total number of students is fewer than ten. The total number of
students may not agree with the Local Report Card because LRC accountability rules are not applied.
The cumulative frequency at each proficiency level is the number of students at or below that level,,
and the cumulative percentages are the percentages of students at or below that level.''']]
                    t=Table(data,6*[0.9*inch], 
                            [0.6*inch, 
                             0.3*inch, 0.2*inch, 0.2*inch, 0.2*inch, 
                             0.3*inch, 0.2*inch, 0.2*inch, 0.2*inch, 
                             0.3*inch, 0.2*inch, 0.2*inch, 0.2*inch, 
                             0.3*inch, 0.2*inch, 0.2*inch, 0.2*inch, 
                             0.5*inch, 0.7*inch])
                    t.setStyle(TableStyle([
                                           ('ALIGN',     (0,0), (-1,0),'CENTER'),
                                           ('BACKGROUND',(0,0), (-1,0),colors.darkgrey),
                                           ('INNERGRID', (0,0), (-1,0), 0.25, colors.black),
                                           ('BOX',       (0,0), (-1,0), 0.25, colors.black),
                                           
                                           ('BACKGROUND',(0,1),(-1,1),colors.lightgrey),
                                           
                                           ('ALIGN',     (0,2), (-1,4),'CENTER'),
                                           ('FONTSIZE',  (0,2), (-1,4), 8),
                                           ('INNERGRID', (0,2), (-1,4), 0.25, colors.black),
                                           ('BOX',       (0,2), (-1,4), 0.25, colors.black),
                                           
                                           ('BACKGROUND',(0,5),(-1,5),colors.lightgrey),
                                           
                                           ('ALIGN',     (0,6), (-1,8),'CENTER'),
                                           ('FONTSIZE',  (0,6), (-1,8), 8),
                                           ('INNERGRID', (0,6), (-1,8), 0.25, colors.black),
                                           ('BOX',       (0,6), (-1,8), 0.25, colors.black),
                                           
                                           ('BACKGROUND',(0,9),(-1,9),colors.lightgrey),
                                           
                                           ('ALIGN',     (0,10), (-1,12),'CENTER'),
                                           ('FONTSIZE',  (0,10), (-1,12), 8),
                                           ('INNERGRID', (0,10), (-1,12), 0.25, colors.black),
                                           ('BOX',       (0,10), (-1,12), 0.25, colors.black),
                                           
                                           ('BACKGROUND',(0,13),(-1,13),colors.lightgrey),
                                           
                                           ('ALIGN',     (0,14), (-1,16),'CENTER'),
                                           ('FONTSIZE',  (0,14), (-1,16), 8),
                                           ('INNERGRID', (0,14), (-1,16), 0.25, colors.black),
                                           ('BOX',       (0,14), (-1,16), 0.25, colors.black),
                                           
                                           ('FONTSIZE',  (0,18), (-1,18), 8),
                                           
                                           ('BOX',     (0,0), (-1,-1), 0.60, colors.black),
                                           ('VALIGN',  (0,1), (-1,-3), 'MIDDLE'),
                                           ('FONTNAME',(0,0), (-1,-1), 'Times-Roman'),
                                           ]))
                    elements.append(t)
                    
                    # write the document to disk
                    doc = SimpleDocTemplate("C:\dvds\out.pdf", pagesize=letter)
                    doc.build(elements)
                
                #TODO append data to state_out_{report_type}
                """
                    
                    
                    
                    
                    
                '''
                    drop_tables(['means_strand_call'], dbContext)
                    dbContext.executeNoResults("""
                        CREATE TABLE means_strand_call ( 
                         variable_name NVARCHAR(50),
                         levelVar NVARCHAR(50),
                         InputVar NVARCHAR(50),
                         Type NVARCHAR(50),
                         rdValue NVARCHAR(50),
                         subject NVARCHAR(50),
                         wherevar NVARCHAR(50),
                         wherevalue NVARCHAR(50),
                         grade NVARCHAR(50)
                        )
                        --INSERT INTO means_strand_call VALUES (
                    """)
                    means_class = Means( excel='N',
                                         agg_ds='means_strand_call',
                                         db_context=dbContext,
                                         inputds='student_g%s' % y,
                                         #odbcconn=,
                                         overwrite='Y' )
                    means_class.execute()
                    '''
                    
                
                

    #fh = open(root+'Content Standard Frequency', 'w')
    #subject_description_list 
    #aggregation_levels
    
    print "\nFINISHED"

                            