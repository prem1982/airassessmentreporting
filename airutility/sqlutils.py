
######## SAS command 'nodupkey' ########
DEDUPLICATE_2FIELDS = """
;with 
cte0 as (SELECT *, ROW_NUMBER() OVER (PARTITION BY {field0} ORDER BY ({orderby0})) rowNumber FROM {table} where {cond})
,cte1 as (SELECT *, ROW_NUMBER() OVER (PARTITION BY {field1} ORDER BY ({orderby1})) rowNumber2 FROM cte0 where rowNumber = 1)
SELECT * 
INTO {temp_table} 
FROM cte1 WHERE rowNumber = 1 and rowNumber2 = 1;

delete from {table} where {cond};
ALTER TABLE {temp_table} DROP COLUMN rowNumber, rowNumber2
INSERT INTO {table} SELECT * FROM {temp_table};
drop table {temp_table};
"""
def nodupkey2( table, field0, field1, temp_table="#mytemp", orderby0="SELECT(0)", orderby1="SELECT(0)", cond="1=1" ):
    return DEDUPLICATE.format(table=table, temp_table=temp_table, field0=field0, field1=field1, orderby0=orderby0, orderby1=orderby1, cond=cond)

DEDUPLICATE = """
;with 
cte0 as (SELECT *, ROW_NUMBER() OVER (PARTITION BY {field0} ORDER BY ({orderby0})) rowNumber FROM {table} where {cond})
SELECT * 
INTO {temp_table} 
FROM cte0 WHERE rowNumber = 1

delete from {table} where {cond};
ALTER TABLE {temp_table} DROP COLUMN rowNumber 
INSERT INTO {table} SELECT * FROM {temp_table};
drop table {temp_table};
"""
def nodupkey( table, field, temp_table="#mytemp", orderby="SELECT(0)", cond="1=1" ):
    return DEDUPLICATE.format(table=table, temp_table=temp_table, field0=field, orderby0=orderby, cond=cond)

