from dataset import *
from airassessmentreporting.airutility.safeexcelread import SafeExcelReader
from airassessmentreporting.airutility.sqlutils import (nodupkey, nodupkey2)
from airassessmentreporting.airutility import (RunContext, get_table_spec )

def idCheckSheet(checkFile, inTable, outTable, runContext, dbContext):
    checkData = Dataset(dbms='excel_srcn', workbook_file=checkFile, sheet_name=None, open_mode='rb')
    rows = checkData.dict_reader()
    dbContext.executeNoResults("""
    CREATE TABLE {table} (
        bad_variable NVARCHAR(128),
        bad_value NVARCHAR(128),
        bad_reason NVARCHAR(128),
        id INT,
        bad_label NVARCHAR(128)
    )
    """.format(table=outTable))
    nmiss = []
    unique = []
    uniqueLabel = {}
    for row in rows:
        if (row['checktype'].lower() == "uniquelabel"):
            if (not row['variable'] in uniqueLabel.keys()):
                uniqueLabel[row['variable']] = []
            uniqueLabel[row['variable']].append(row['labelvariable'])
        if (row['checktype'].lower() == "uniquekey"):
            unique.append(row['variable'])
        if (row['checktype'].lower() == "nonmissing"):
            nmiss.append(row['variable'].lower())
            
    for field in nmiss:
        rows = dbContext.execute("SELECT {field}, id FROM {table} WHERE {field} IS NULL OR {field}='' OR {field}='.'; ".format(field=field, table=inTable))
        for row in rows:
            #print "INSERT INTO {table} VALUES ( '{field}', '{entry}', 'missing ID', {id}, '' ) ".format(table=outTable, field=field, entry=row[0], id=row[1])
            dbContext.executeNoResults("INSERT INTO {table} VALUES ( '{field}', '{entry}', 'missing ID', {id}, '' ) ".format(table=outTable, field=field, entry=row[0], id=row[1]))
    
    for field in unique:
        rows = dbContext.execute("""
            SELECT T1.{field}, T1.id
            FROM {table} T1
            JOIN
            (
                SELECT {field} 
                FROM {table} 
                GROUP BY {field} 
                HAVING COUNT(*) >= 2
            ) T2
            ON T1.{field} = T2.{field} 
        """.format(field=field, table=inTable))
        for row in rows:
            #print "INSERT INTO {table} VALUES ( '{field}', '{entry}', 'not unique', {id}, '' ) ".format(table=outTable, field=field, entry=row[0], id=row[1])
            dbContext.executeNoResults("INSERT INTO {table} VALUES ( '{field}', '{entry}', 'not unique', {id}, '' ) ".format(table=outTable, field=field, entry=row[0], id=row[1]))
            
    for myid in uniqueLabel:
        for label in uniqueLabel[myid]:
            rows = dbContext.execute("""
                ;with
                cte0 as (SELECT T1.{idfield}, T1.{labelfield}, T1.id, row_number() over(partition by T1.{idfield}, T1.{labelfield} order by (select(0))) AS rn from {table} T1)
                ,cte1 as (select {idfield}, rn from cte0 GROUP BY {idfield}, rn HAVING COUNT(*) >= 2)
                ,cte2 as (SELECT T1.{idfield}, T1.{labelfield}, T1.id, row_number() over(partition by T1.{idfield}, T1.{labelfield} order by (select(0))) AS rn2 from cte1 c join {table} T1 on (c.{idfield} = T1.{idfield}))
                select * from cte2 where rn2 = 1
            """.format(idfield=myid, labelfield=label, table=inTable))
            for row in rows:
                #print "INSERT INTO {table} VALUES ( '{field}', '{entry}', 'bad label', {id}, '{label}' ) ".format(table=outTable, field=myid, entry=row[0], id=row[2], label=row[1])
                dbContext.executeNoResults("INSERT INTO {table} VALUES ( '{field}', '{entry}', 'bad label', {id}, '{label}' ) ".format(table=outTable, field=myid, entry=row[0], id=row[2], label=row[1]))
   


#checkFile = "C:\\CVS Projects\\CSSC Score Reporting\\OGT Summer 2012\\Code\\Development\\Intake\\OGT_ID_Sheet.xls"
#inTable = "intakeFinal"
#outTable = "idCheck4"
#runContext = RunContext('OGT_12SU')
#dbContext = runContext.getDBContext()
#idCheckSheet2(checkFile, inTable, outTable, runContext, dbContext)
