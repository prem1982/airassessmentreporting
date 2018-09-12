'''
Created on Jul 24, 2013

@author: temp_plakshmanan
'''


from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote
from airassessmentreporting.testutility import SuiteContext
from airassessmentreporting.erasure import BookMapReader
from collections import OrderedDict


SA_SQL = """ 
SELECT TB7.ID, TB7.FLAT_TABLE_ID, TB7.CALC_OE_FINAL, TB7.{upwx_oe_final} OE_FINAL
        INTO {Handscore_check_errors} from 
(        
select TB6.id,tb6.flat_Table_id, tb6.calc_oe_final,tb6.{upwx_oe_final} from
(
select tb5.id, tb5.flat_table_id, tb5.{upwx_oe_first}, tb5.{upwx_oe_final}, tb5.{upwx_oe_second} ,
case isnumeric(tb5.calc_{upwx_oe_final})
    when 1 then cast(cast(tb5.calc_{upwx_oe_final} as DECIMAL(3,1)) AS VARCHAR(3))
    else tb5.calc_{upwx_oe_final} 
    end calc_oe_final
from 
(SELECT tb4.id,tb4.flat_table_id,tb4.{upwx_oe_first}, tb4.{upwx_oe_second}, tb4.{upwx_oe_final},TB4.abs_value_first_second,TB4.abs_value_first_second_2,
CASE WHEN TB4.{upwx_oe_first} = TB4.{upwx_oe_second}
            THEN TB4.{upwx_oe_first}
    WHEN TB4.abs_value_first_second = 1
            THEN TB4.abs_value_first_second_2
    WHEN TB4.{upwx_oe_first} = TB4.{upwx_oe_third}
            THEN TB4.{upwx_oe_first}
    WHEN TB4.{upwx_oe_second} = TB4.{upwx_oe_third}
            THEN TB4.{upwx_oe_second}
    ELSE cast(dbo.MEDIAN(cast({upwx_oe_first} as float), cast({upwx_oe_second} as float),cast({upwx_oe_final} as float)) as varchar)
        END calc_{upwx_oe_final}
 FROM
(        
                SELECT tb2.id, 
                       tb2.flat_table_id,
                       tb3.[item position] AS bookmap_id, 
                       tb1.form, 
                       tb3.[role], 
                       tb3.[item format],
                       tb2.{upwx_oe_first},
                       tb2.{upwx_oe_second},
                       tb2.{upwx_oe_third},
                       tb2.{upwx_oe_final},
                       ISNUMERIC(tb2.{upwx_oe_first}) first_numeric,
                       ISNUMERIC(tb2.{upwx_oe_second}) second_numeric,
                       CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_first} as float) - cast(tb2.{upwx_oe_second} as float)) as varchar)
                            END abs_value_first_second,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_first} as float) + cast(tb2.{upwx_oe_second} as float) ) / 2 as varchar)
                            END abs_value_first_second_2
                FROM   (
                SELECT x.id, 
                               CASE 
                                 WHEN x.{ucwx_form} = 'BR' THEN 'B' 
                                 ELSE 'A' 
                               END form 
                        FROM   {flat_table} x
                        ) tb1 
                       INNER JOIN {mc_table} tb2 
                               ON tb1.id = tb2.flat_table_id 
                       INNER JOIN ogt_Bookmaps tb3 
                               ON tb2.id = tb3.[item position] 
                                  AND tb3.subject_values = '{subject_value}'
                                  AND tb1.form = tb3.form_values 
                WHERE  TB3.[item format] IN ('SA')
                       AND TB3.[role] = 'OPERATIONAL'
                       ) TB4
 ) tb5
 ) tb6
 where tb6.{upwx_oe_final} <> tb6.calc_oe_final
) TB7
"""

SQ_SQL_REPORT = """
SELECT TB4.ID,TB4.FLAT_TABLE_ID,TB4.LITHOCODE,TB4.bookmap_id, TB4.FORM, TB4.{upwx_oe_first}, TB4.{upwx_oe_second},TB4.{upwx_oe_third},TB4.{upwx_oe_final}, 
TB4.ROLE, TB4.item_format , TB5.OE_FINAL, TB5.CALC_OE_FINAL
INTO {Handscore_sa_check_errors}  from
(               SELECT tb2.id,
                       tb1.lithocode,
                       tb2.flat_table_id,
                       tb3.[item position] AS bookmap_id, 
                       tb1.form, 
                       tb3.[role], 
                       tb3.[item format] as item_format,
                       tb2.{upwx_oe_first},
                       tb2.{upwx_oe_second},
                       tb2.{upwx_oe_third},
                       tb2.{upwx_oe_final},
                       ISNUMERIC(tb2.{upwx_oe_first}) first_numeric,
                       ISNUMERIC(tb2.{upwx_oe_second}) second_numeric,
                       CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_first} as float) - cast(tb2.{upwx_oe_second} as float)) as varchar)
                            END abs_value_first_second,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_first} as float) + cast(tb2.{upwx_oe_second} as float) ) / 2 as varchar)
                            END abs_value_first_second_2
                FROM   (
                SELECT x.id, 
                        x.lithocode,
                               CASE 
                                 WHEN x.{ucwx_form} = 'BR' THEN 'B' 
                                 ELSE 'A' 
                               END form 
                        FROM   {flat_table} x
                        ) tb1 
                       INNER JOIN {mc_table} tb2 
                               ON tb1.id = tb2.flat_table_id 
                       INNER JOIN ogt_Bookmaps tb3 
                               ON tb2.id = tb3.[item position] 
                                  AND tb3.subject_values = '{subject_value}'
                                  AND tb1.form = tb3.form_values 
                WHERE  TB3.[item format] IN ('SA')
                       AND TB3.[role] = 'OPERATIONAL' ) TB4
                INNER JOIN
                        {Handscore_sa_check_errors_intr} TB5
                        ON TB4.ID = TB5.ID 
                        AND TB4.FLAT_TABLE_ID = TB5.FLAT_TABLE_ID """


ER_SQL_W = """SELECT tb7.ID, tb7.FLAT_TABLE_ID, tb7.CALC_OE_FINAL, tb7.UPWX_OE_FINAL INTO {Handscore_check_errors}
from 
(
select tb6.lithocode,TB6.id,tb6.flat_Table_id, tb6.calc_oe_final,tb6.upwx_oe_final from
(
select tb5.lithocode,tb5.id, tb5.flat_table_id, tb5.upwx_oe_first, tb5.upwx_oe_final, tb5.upwx_oe_Second ,tb5.upwx_oe_third,tb5.upwx_oe_reso,
case isnumeric(tb5.calc_upwx_oe_final)
    when 1 then cast(cast(tb5.calc_upwx_oe_final as DECIMAL(3,1)) AS VARCHAR(5))
    else tb5.calc_upwx_oe_final 
    end calc_oe_final
from 
(
SELECT tb4.lithocode,tb4.id,tb4.flat_table_id,tb4.upwx_oe_first, tb4.upwx_oe_Second, tb4.upwx_oe_final,TB4.abs_value_first_second,TB4.abs_value_first_second_2,tb4.upwx_oe_third,tb4.upwx_oe_reso,
        CASE WHEN ISNUMERIC(TB4.upwx_oe_first) = 1 and ISNUMERIC(TB4.upwx_oe_second) = 1
        THEN
            CASE WHEN TB4.upwx_oe_first = TB4.upwx_oe_Second
                    THEN abs_value_first_second_2
                    WHEN TB4.abs_value_first_second = 1
                    THEN TB4.abs_value_first_second_2
                    WHEN TB4.upwx_oe_first = TB4.upwx_oe_third
                    THEN abs_value_first_third_2
                    WHEN TB4.upwx_oe_Second = TB4.upwx_oe_third
                    THEN abs_value_second_third_2
                    WHEN TB4.abs_value_first_third = 1 and tb4.abs_value_second_third = 1 and  tb4.min_value <> '0'
                    THEN cast(2 * cast(upwx_oe_third as float) as varchar)
                    WHEN TB4.abs_value_first_third = 1
                    THEN TB4.abs_value_first_third_2
                    WHEN TB4.abs_value_second_third = 1
                    THEN TB4.abs_value_second_third_2
                    ELSE
                    cast(2 * tb4.upwx_oe_reso as varchar)
            END 
        ELSE 
            CASE WHEN TB4.upwx_oe_first = TB4.upwx_oe_second
                THEN upwx_oe_first
            END
        END calc_upwx_oe_final  FROM
(        
SELECT tb2.id, 
                tb2.flat_table_id,
                       tb3.[item position] AS bookmap_id, 
                       tb1.lithocode,
                       tb1.form, 
                       tb3.[role], 
                       tb3.[item format],
                       tb2.upwx_oe_first,
                       tb2.upwx_oe_Second,
                       tb2.upwx_oe_third,
                       tb2.upwx_oe_final,
                       tb2.upwx_oe_reso,
                       ISNUMERIC(tb2.upwx_oe_first) first_numeric,
                       ISNUMERIC(tb2.upwx_oe_Second) second_numeric,
                       ISNUMERIC(tb2.upwx_oe_third) third_numeric,
                       CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_Second) = 1
                            THEN cast(ABS(CAST(tb2.upwx_oe_first as float) - cast(tb2.upwx_oe_Second as float)) as varchar)
                            END abs_value_first_second,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_Second) = 1
                            THEN cast((CAST(tb2.upwx_oe_first as float) + cast(tb2.upwx_oe_Second as float) )  as varchar)
                            END abs_value_first_second_2,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast(ABS(CAST(tb2.upwx_oe_first as float) - cast(tb2.upwx_oe_third as float)) as varchar)
                            END abs_value_first_third,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast((CAST(tb2.upwx_oe_first as float) + cast(tb2.upwx_oe_third as float) )  as varchar)
                            END abs_value_first_third_2,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_second) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast(ABS(CAST(tb2.upwx_oe_second as float) - cast(tb2.upwx_oe_third as float)) as varchar)
                            END abs_value_second_third,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast((CAST(tb2.upwx_oe_second as float) + cast(tb2.upwx_oe_third as float) )  as varchar)
                            END abs_value_second_third_2,
                            CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1 AND ISNUMERIC(tb2.upwx_oe_second) = 1
                            THEN dbo.mini(cast(upwx_oe_first as float),cast(upwx_oe_second as float),cast(upwx_oe_third as float)) 
                            ELSE '0'
                            END min_value
                FROM   (
                SELECT x.id, 
                    x.lithocode,
                               CASE 
                                 WHEN x.ucwx_form = 'BR' THEN 'B' 
                                 ELSE 'A' 
                               END form 
                        FROM   {flat_table} x
                        ) tb1 
                       INNER JOIN {mc_table} tb2 
                               ON tb1.id = tb2.flat_table_id 
                       INNER JOIN ogt_Bookmaps tb3 
                               ON tb2.id = tb3.[item position] 
                                  AND tb3.subject_values = 'W'
                                  AND tb1.form = tb3.form_values 
                WHERE  TB3.[item format] IN ('ER')
                       AND TB3.[role] = 'OPERATIONAL'
                       ) 
                       TB4
 ) tb5
 ) tb6
 where tb6.upwx_oe_final <> tb6.calc_oe_final) tb7 """

ER_SQL_W_REPORT = """SELECT TB6.ID,TB6.FLAT_TABLE_ID,TB6.LITHOCODE,TB6.bookmap_id, TB6.FORM, TB6.upwx_oe_first, TB6.upwx_oe_second,TB6.upwx_oe_third, 
TB6.ROLE, TB6.item_format , TB7.upwx_OE_FINAL, TB7.CALC_OE_FINAL
INTO {Handscore_er_w_check_errors}  from
(        
    select tb5.lithocode,tb5.id, tb5.flat_table_id, tb5.upwx_oe_first, tb5.upwx_oe_final, tb5.upwx_oe_Second ,tb5.upwx_oe_third,tb5.upwx_oe_reso,tb5.form,TB5.bookmap_id,tb5.role,tb5.item_format,
case isnumeric(tb5.calc_upwx_oe_final)
    when 1 then cast(cast(tb5.calc_upwx_oe_final as DECIMAL(3,1)) AS VARCHAR(5))
    else tb5.calc_upwx_oe_final 
    end calc_oe_final
from 
(
SELECT tb4.lithocode,tb4.id,tb4.flat_table_id,tb4.upwx_oe_first, tb4.upwx_oe_Second, tb4.upwx_oe_final,TB4.abs_value_first_second,TB4.abs_value_first_second_2,tb4.upwx_oe_third,tb4.upwx_oe_reso,tb4.role,tb4.item_format,
    
    tb4.bookmap_id,tb4.form,
        CASE WHEN ISNUMERIC(TB4.upwx_oe_first) = 1 and ISNUMERIC(TB4.upwx_oe_second) = 1
        THEN
            CASE WHEN TB4.upwx_oe_first = TB4.upwx_oe_Second
                    THEN abs_value_first_second_2
                    WHEN TB4.abs_value_first_second = 1
                    THEN TB4.abs_value_first_second_2
                    WHEN TB4.upwx_oe_first = TB4.upwx_oe_third
                    THEN abs_value_first_third_2
                    WHEN TB4.upwx_oe_Second = TB4.upwx_oe_third
                    THEN abs_value_second_third_2
                    WHEN TB4.abs_value_first_third = 1 and tb4.abs_value_second_third = 1 and  tb4.min_value <> '0'
                    THEN cast(2 * cast(upwx_oe_third as float) as varchar)
                    WHEN TB4.abs_value_first_third = 1
                    THEN TB4.abs_value_first_third_2
                    WHEN TB4.abs_value_second_third = 1
                    THEN TB4.abs_value_second_third_2
                    ELSE
                    cast(2 * tb4.upwx_oe_reso as varchar)
            END 
        ELSE 
            CASE WHEN TB4.upwx_oe_first = TB4.upwx_oe_second
                THEN upwx_oe_first
            END
        END calc_upwx_oe_final  FROM
(        
SELECT tb2.id, 
                tb2.flat_table_id,
                       tb3.[item position] AS bookmap_id, 
                       tb1.lithocode,
                       tb1.form, 
                       tb3.[role], 
                       tb3.[item format] as item_Format,
                       tb2.upwx_oe_first,
                       tb2.upwx_oe_Second,
                       tb2.upwx_oe_third,
                       tb2.upwx_oe_final,
                       tb2.upwx_oe_reso,
                       ISNUMERIC(tb2.upwx_oe_first) first_numeric,
                       ISNUMERIC(tb2.upwx_oe_Second) second_numeric,
                       ISNUMERIC(tb2.upwx_oe_third) third_numeric,
                       CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_Second) = 1
                            THEN cast(ABS(CAST(tb2.upwx_oe_first as float) - cast(tb2.upwx_oe_Second as float)) as varchar)
                            END abs_value_first_second,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_Second) = 1
                            THEN cast((CAST(tb2.upwx_oe_first as float) + cast(tb2.upwx_oe_Second as float) )  as varchar)
                            END abs_value_first_second_2,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast(ABS(CAST(tb2.upwx_oe_first as float) - cast(tb2.upwx_oe_third as float)) as varchar)
                            END abs_value_first_third,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast((CAST(tb2.upwx_oe_first as float) + cast(tb2.upwx_oe_third as float) )  as varchar)
                            END abs_value_first_third_2,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_second) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast(ABS(CAST(tb2.upwx_oe_second as float) - cast(tb2.upwx_oe_third as float)) as varchar)
                            END abs_value_second_third,
                        CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1
                            THEN cast((CAST(tb2.upwx_oe_second as float) + cast(tb2.upwx_oe_third as float) )  as varchar)
                            END abs_value_second_third_2,
                            CASE WHEN ISNUMERIC(tb2.upwx_oe_first) = 1 AND ISNUMERIC(tb2.upwx_oe_third) = 1 AND ISNUMERIC(tb2.upwx_oe_second) = 1
                            THEN dbo.mini(cast(upwx_oe_first as float),cast(upwx_oe_second as float),cast(upwx_oe_third as float)) 
                            ELSE '0'
                            END min_value
                FROM   (
                SELECT x.id, 
                    x.lithocode,
                               CASE 
                                 WHEN x.ucwx_form = 'BR' THEN 'B' 
                                 ELSE 'A' 
                               END form 
                        FROM   PRE_QC_FLAT_TABLE_2 x
                        ) tb1 
                       INNER JOIN MC_TABLE_W tb2 
                               ON tb1.id = tb2.flat_table_id 
                       INNER JOIN ogt_Bookmaps tb3 
                               ON tb2.id = tb3.[item position] 
                                  AND tb3.subject_values = 'W'
                                  AND tb1.form = tb3.form_values 
                WHERE  TB3.[item format] IN ('ER')
                       AND TB3.[role] = 'OPERATIONAL'
                       ) 
                       TB4
 ) tb5
                       ) 
                       TB6
                     INNER JOIN
                        {Handscore_er_w_check_errors_intr} TB7
                        ON TB6.ID = TB7.ID 
                        AND TB6.FLAT_TABLE_ID = TB7.FLAT_TABLE_ID  """


ER_SQL_NOT_W = """SELECT tb7.ID, tb7.FLAT_TABLE_ID, tb7.CALC_OE_FINAL, tb7.oe_final INTO {Handscore_check_errors_intr}
from 
(        
select tb6.lithocode,TB6.id,tb6.flat_Table_id, tb6.calc_oe_final,tb6.oe_final from
(
select tb5.lithocode,tb5.id, tb5.flat_table_id, tb5.oe_first, tb5.oe_final, tb5.oe_Second ,tb5.oe_third,tb5.oe_reso,
case isnumeric(tb5.calc_upmx_oe_final)
    when 1 then cast(cast(tb5.calc_upmx_oe_final as DECIMAL(3,1)) AS VARCHAR(5))
    else tb5.calc_upmx_oe_final 
    end calc_oe_final
from 
(SELECT tb4.lithocode,tb4.id,tb4.flat_table_id,tb4.oe_first, tb4.oe_Second, tb4.oe_final,TB4.abs_value_first_second,TB4.abs_value_first_second_2,tb4.oe_third,tb4.oe_reso,
CASE WHEN TB4.oe_first = TB4.oe_Second
            THEN TB4.oe_first
            WHEN TB4.oe_first = TB4.oe_third
            THEN TB4.oe_first
            WHEN TB4.oe_second = TB4.oe_third
            THEN TB4.oe_Second
            WHEN TB4.abs_value_first_third = 1 and tb4.abs_value_second_third = 1
            THEN cast(dbo.MEDIAN(cast(oe_first as float), cast(oe_second as float),cast(oe_final as float)) as varchar)
            WHEN TB4.abs_value_first_second = 1
            THEN TB4.abs_value_first_second_2
            WHEN TB4.abs_value_first_third = 1
            THEN TB4.abs_value_first_third_2
            WHEN TB4.abs_value_second_third = 1
            THEN TB4.abs_value_second_third_2
            ELSE
                tb4.oe_reso
        END calc_upmx_oe_final
 FROM
(        
SELECT tb2.id, 
                tb2.flat_table_id,
                       tb3.[item position] AS bookmap_id, 
                       tb1.lithocode,
                       tb1.form, 
                       tb3.[role], 
                       tb3.[item format],
                       tb2.{upwx_oe_first} as oe_first,
                       tb2.{upwx_oe_second} as oe_second,
                       tb2.{upwx_oe_third} as oe_third,
                       tb2.{upwx_oe_final} as oe_final,
                       tb2.{upwx_oe_reso} as oe_reso,
                       ISNUMERIC(tb2.{upwx_oe_first}) first_numeric,
                       ISNUMERIC(tb2.{upwx_oe_second}) second_numeric,
                       ISNUMERIC(tb2.{upwx_oe_third}) third_numeric,
                       CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_first} as float) - cast(tb2.{upwx_oe_second} as float)) as varchar)
                            END abs_value_first_second,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_first} as float) + cast(tb2.{upwx_oe_second} as float) ) / 2 as varchar)
                            END abs_value_first_second_2,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_first} as float) - cast(tb2.{upwx_oe_third} as float)) as varchar)
                            END abs_value_first_third,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_first} as float) + cast(tb2.{upwx_oe_third} as float) ) / 2 as varchar)
                            END abs_value_first_third_2,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_second}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_second} as float) - cast(tb2.{upwx_oe_third} as float)) as varchar)
                            END abs_value_second_third,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_second} as float) + cast(tb2.{upwx_oe_third} as float) ) / 2 as varchar)
                            END abs_value_second_third_2
                FROM   (
                SELECT x.id, 
                    x.lithocode,
                               CASE 
                                 WHEN x.{ucwx_form} = 'BR' THEN 'B' 
                                 ELSE 'A' 
                               END form 
                        FROM   {flat_table} x
                        ) tb1 
                       INNER JOIN {mc_table} tb2 
                               ON tb1.id = tb2.flat_table_id 
                       INNER JOIN ogt_Bookmaps tb3 
                               ON tb2.id = tb3.[item position] 
                                  AND tb3.subject_values = '{subject_value}'
                                  AND tb1.form = tb3.form_values 
                WHERE  TB3.[item format] IN ('ER')
                       AND TB3.[role] = 'OPERATIONAL'
                       ) TB4
 ) tb5
 ) tb6
 where tb6.oe_final <> tb6.calc_oe_final
) TB7
"""

ER_SQL_NOT_W_REPORT = """
select tb6.lithocode,tb6.id, tb6.flat_table_id, tb6.oe_first, tb6.oe_second, tb6.oe_third, tb6.oe_final, tb6.oe_reso
INTO {Handscore_check_errors} from
(select tb5.lithocode,tb5.id, tb5.flat_table_id, tb5.oe_first,tb5.oe_third,tb5.oe_Second ,tb5.oe_final, tb5.oe_reso,
case isnumeric(tb5.calc_upmx_oe_final)
    when 1 then cast(cast(tb5.calc_upmx_oe_final as DECIMAL(3,1)) AS VARCHAR(5))
    else tb5.calc_upmx_oe_final 
    end calc_oe_final
from 
(SELECT tb4.lithocode,tb4.id,tb4.flat_table_id,tb4.oe_first, tb4.oe_Second, tb4.oe_final,TB4.abs_value_first_second,TB4.abs_value_first_second_2,tb4.oe_third,tb4.oe_reso,
CASE WHEN TB4.oe_first = TB4.oe_Second
            THEN TB4.oe_first
            WHEN TB4.oe_first = TB4.oe_third
            THEN TB4.oe_first
            WHEN TB4.oe_second = TB4.oe_third
            THEN TB4.oe_Second
            WHEN TB4.abs_value_first_third = 1 and tb4.abs_value_second_third = 1
            THEN cast(dbo.MEDIAN(cast(oe_first as float), cast(oe_second as float),cast(oe_final as float)) as varchar)
            WHEN TB4.abs_value_first_second = 1
            THEN TB4.abs_value_first_second_2
            WHEN TB4.abs_value_first_third = 1
            THEN TB4.abs_value_first_third_2
            WHEN TB4.abs_value_second_third = 1
            THEN TB4.abs_value_second_third_2
            ELSE
                tb4.oe_reso
        END calc_upmx_oe_final
 FROM
(        
SELECT tb2.id, 
                tb2.flat_table_id,
                       tb3.[item position] AS bookmap_id, 
                       tb1.lithocode,
                       tb1.form, 
                       tb3.[role], 
                       tb3.[item format],
                       tb2.{upwx_oe_first} as oe_first,
                       tb2.{upwx_oe_second} as oe_second,
                       tb2.{upwx_oe_third} as oe_third,
                       tb2.{upwx_oe_final} as oe_final,
                       tb2.{upwx_oe_reso} as oe_reso,
                       ISNUMERIC(tb2.{upwx_oe_first}) first_numeric,
                       ISNUMERIC(tb2.{upwx_oe_second}) second_numeric,
                       ISNUMERIC(tb2.{upwx_oe_third}) third_numeric,
                       CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_first} as float) - cast(tb2.{upwx_oe_second} as float)) as varchar)
                            END abs_value_first_second,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_second}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_first} as float) + cast(tb2.{upwx_oe_second} as float) ) / 2 as varchar)
                            END abs_value_first_second_2,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_first} as float) - cast(tb2.{upwx_oe_third} as float)) as varchar)
                            END abs_value_first_third,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_first} as float) + cast(tb2.{upwx_oe_third} as float) ) / 2 as varchar)
                            END abs_value_first_third_2,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_second}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast(ABS(CAST(tb2.{upwx_oe_second} as float) - cast(tb2.{upwx_oe_third} as float)) as varchar)
                            END abs_value_second_third,
                        CASE WHEN ISNUMERIC(tb2.{upwx_oe_first}) = 1 AND ISNUMERIC(tb2.{upwx_oe_third}) = 1
                            THEN cast((CAST(tb2.{upwx_oe_second} as float) + cast(tb2.{upwx_oe_third} as float) ) / 2 as varchar)
                            END abs_value_second_third_2
                FROM   (
                SELECT x.id, 
                    x.lithocode,
                               CASE 
                                 WHEN x.{ucwx_form} = 'BR' THEN 'B' 
                                 ELSE 'A' 
                               END form 
                        FROM   {flat_table} x
                        ) tb1 
                       INNER JOIN {mc_table} tb2 
                               ON tb1.id = tb2.flat_table_id 
                       INNER JOIN ogt_Bookmaps tb3 
                               ON tb2.id = tb3.[item position] 
                                  AND tb3.subject_values = '{subject_value}'
                                  AND tb1.form = tb3.form_values 
                WHERE  TB3.[item format] IN ('ER')
                       AND TB3.[role] = 'OPERATIONAL'
                       ) TB4
 ) tb5 
 ) tb6
 INNER JOIN
                        {Handscore_check_errors_intr} TB7
                        ON TB6.ID = TB7.ID 
                        AND TB6.FLAT_TABLE_ID = TB7.FLAT_TABLE_ID  
"""


subject_col_mapping = {'W':['ucwx_form','upwx_oe_first','upwx_oe_second','upwx_oe_third','upwx_oe_final','Handscore_check_error_sa_w','Handscore_check_error_er_w','upwx_oe_reso']
                      ,'C':['uccx_form','upcx_oe_first','upcx_oe_Second','upcx_oe_third','upcx_oe_final','Handscore_check_error_sa_c','Handscore_check_error_er_c','upcx_oe_reso']
                      ,'M':['ucmx_form','upmx_oe_first','upmx_oe_Second','upmx_oe_third','upmx_oe_final','Handscore_check_error_sa_m','Handscore_check_error_er_m','upmx_oe_reso']
                      ,'S':['ucsx_form','upsx_oe_first','upsx_oe_Second','upsx_oe_third','upsx_oe_final','Handscore_check_error_sa_s','Handscore_check_error_er_s','upsx_oe_reso']
                      ,'R':['ucrx_form','uprx_oe_first','uprx_oe_Second','uprx_oe_third','uprx_oe_final','Handscore_check_error_sa_r','Handscore_check_error_er_r','uprx_oe_reso']
                       }

class HandScore( object ):
    """ This module will work similar to the Handscore check module in SAS. This module will calculate the scores and compare it with the
    pre-calculated OE_FINAL value. If the values are not same the id's are written to the error_table that is given in the input.
    This module will generate multiple outputs based on the number of outputs given in the Input. If there are any errors, the table names will be displayed in the console.
    There are 2 steps 1) To identify all the mis-matches, which will be written to _intr tables 2) Load the error table with all information needed for debugging."""
    def __init__(self, dbcontext = '', runcontext = '', bookmapfile = ''):
        self.dbcontext = dbcontext
        self.runcontext = runcontext
        self.bookmapfile = bookmapfile
        XLSmaps = BookMapReader( excel='Y',inputfile=self.bookmapfile,inputsheet='BookMap',
                                 read_to_db=True,db_context = self.dbcontext, outputTable = 'ogt_Bookmaps' )
        print 'Bookmaps LOADED'
        
    def process(self):
        print 'HandScore_check_module_started'
        try:
            for eachlist in subject_col_mapping.values():
                drop_table_if_exists( eachlist[5], self.dbcontext)
                drop_table_if_exists( eachlist[6], self.dbcontext)
                drop_table_if_exists( eachlist[5] + '_intr', self.dbcontext)
                drop_table_if_exists( eachlist[6] + '_intr', self.dbcontext)
#             self._process_sa_items()
            self._process_er_items()
        except Exception as error:
                print 'Error=',error
        
    
    def _process_sa_items(self):
        print 'Checks for SA items OE-SCORES - Started'
           
        qry = "select tablename from table_names where subject_id = 'F'"
        flat_table = self.dbcontext.execute(qry)
          
        _COUNT_QRY  = "select count(*) from {table_name}"
          
        qry = "select subject_id, tablename from table_names where subject_id <> 'F'"
          
        for each in self.dbcontext.execute(qry):
            cols = subject_col_mapping.get(each[0][0:1])
            qry = SA_SQL.format(upwx_oe_first = cols[1],upwx_oe_second = cols[2],upwx_oe_third = cols[3],upwx_oe_final = cols[4]
                        ,ucwx_form = cols[0],flat_table = flat_table[0][0].encode('ascii'),mc_table = each[1].encode('ascii'), subject_value = each[0].encode('ascii')
                        ,Handscore_check_errors = cols[5] + '_intr')
#             print 'qry=', qry
            result = self.dbcontext.executeNoResults(qry)
              
            count_qry = _COUNT_QRY.format(table_name=cols[5] + '_intr')
            result = self.dbcontext.execute(count_qry)
              
            if result[0][0] > 0:
                report_qry = SQ_SQL_REPORT.format(upwx_oe_first = cols[1],upwx_oe_second = cols[2],upwx_oe_third = cols[3],upwx_oe_final = cols[4]
                        ,ucwx_form = cols[0],flat_table = flat_table[0][0].encode('ascii'),mc_table = each[1].encode('ascii'), subject_value = each[0].encode('ascii')
                        ,Handscore_sa_check_errors = cols[5],Handscore_sa_check_errors_intr = cols[5] + '_intr')
#                 print 'report_qry=', report_qry
                result = self.dbcontext.executeNoResults(report_qry)
                  
                print 'Check the following table for errors [SA] items- {0}'.format(cols[5])
              
        print 'Checks for SA items OE-SCORES - Completed'
  
    def _process_er_items(self):
        print 'Checks for ER items - WRITING OE-SCORES - Started'
        qry = "select tablename from table_names where subject_id = 'F'"
        flat_table = self.dbcontext.execute(qry)
          
        qry = "select subject_id, tablename from table_names where subject_id <> 'F'"
          
        qry = ER_SQL_W.format(flat_table=flat_table[0][0].encode('ascii'),mc_table='MC_TABLE_W',Handscore_check_errors='Handscore_check_error_er_w_intr')
#         print 'qry=', qry
        result = self.dbcontext.executeNoResults(qry)
          
        print 'Checks for ER items - WRITING OE-SCORES-ERROR - Started' 
        _COUNT_QRY  = "select count(*) from {table_name}"    
        qry = _COUNT_QRY.format(table_name='Handscore_check_error_er_w_intr')
        result = self.dbcontext.execute(qry)
  
        if result[0][0] > 0:
            qry = ER_SQL_W_REPORT.format(flat_table=flat_table[0][0].encode('ascii'),mc_table='MC_TABLE_W',Handscore_er_w_check_errors='Handscore_check_error_er_w',
                                          Handscore_er_w_check_errors_intr='Handscore_check_error_er_w_intr')
#             print 'qry=', qry
            result = self.dbcontext.executeNoResults(qry)
            print 'Check the following table for errors [ER] WRITING - {0}'.format('HANDSCORE_CHECK_ERROR_ER_W')
              
        print 'Checks for ER items - WRITING OE-SCORES - Completed'
         
        print 'Checks for ER items - OTHER THAN WRITING OE-SCORES - Started'
        qry = "select subject_id, tablename from table_names where subject_id NOT IN ('F','W')"
        for each in self.dbcontext.execute(qry):
            cols = subject_col_mapping.get(each[0][0:1])
            qry = ER_SQL_NOT_W.format(upwx_oe_first=cols[1],upwx_oe_second=cols[2],upwx_oe_third=cols[3],upwx_oe_final=cols[4]
                        ,ucwx_form=cols[0],flat_table=flat_table[0][0].encode('ascii'),mc_table=each[1].encode('ascii'), subject_value=each[0].encode('ascii')
                        ,Handscore_check_errors_intr=cols[6]+'_intr',upwx_oe_reso=cols[7])
#             print 'qry=', qry 
            result = self.dbcontext.executeNoResults(qry)
             
            _COUNT_QRY  = "select count(*) from {table_name}"    
            qry = _COUNT_QRY.format(table_name=cols[6]+'_intr')
            result = self.dbcontext.execute(qry)
            
            if result[0][0] > 0:
                qry = ER_SQL_NOT_W_REPORT.format(upwx_oe_first=cols[1],upwx_oe_second=cols[2],upwx_oe_third=cols[3],upwx_oe_final=cols[4]
                        ,ucwx_form=cols[0],flat_table=flat_table[0][0].encode('ascii'),mc_table=each[1].encode('ascii'), subject_value=each[0].encode('ascii')
                        ,Handscore_check_errors=cols[6],upwx_oe_reso=cols[7],Handscore_check_errors_intr=cols[6]+'_intr')
                result = self.dbcontext.executeNoResults(qry)
                print 'Check the following table for errors [ER] WRITING - {0}'.format(cols[6].upper())
        print 'Checks for ER items - OTHER THAN WRITING OE-SCORES - Completed'
        print 'HandScore_check_module_Ended'
 
if __name__ == '__main__':
    runcontext = SuiteContext('unittest')
    dbcontext = runcontext.getDBContext()
    ac = HandScore(dbcontext = dbcontext,runcontext = runcontext,bookmapfile = """C:\SAS\OGT\Input\Bookmaplocations1.xls""")
    ac.process()        