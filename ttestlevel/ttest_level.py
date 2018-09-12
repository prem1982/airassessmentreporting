'''
Created on May 22, 2013

@author: temp_dmenes
'''

from airassessmentreporting.airutility import ( get_table_spec, table_exists,
        drop_table_if_exists, FieldSpec, Joiner, get_temp_table )

DEFAULT_CRITICAL_VALUE = 1.96

_MISSING_INVAR_QUERY = """
SELECT COUNT(1) FROM {ds_in} WHERE ({input_col} IS NULL) AND ({where})
"""

_SUMMARY_QUERY="""
SELECT
    SUM({input_col}) AS t,
    COUNT({input_col}) AS n,
    VARP({input_col}) * COUNT({input_col}) AS css,
    {group_cols}
INTO {summary_table}
FROM {ds_in}
WHERE {where}
GROUP BY {group_cols}
"""

_CONFLICTING_GROUPS_QUERY = """
SELECT MAX(B.N) FROM (
    SELECT COUNT(1) AS N
    FROM (
        SELECT DISTINCT {all_group_cols}
        FROM {target_summary_table}
    ) AS A
    GROUP BY {target_group_cols}
) AS B
"""

_MISSING_PARENTS_QUERY = """
SELECT COUNT(1)
FROM {target_summary_table}
WHERE ({parent_group_cols:' OR ',itemfmt='({{}} IS NULL)'})
    AND ({target_group_cols:' AND ',itemfmt='({{}} IS NOT NULL)'})
"""

_TTEST_QUERY_1 = """
INSERT INTO {ds_out}({group_cols}, p_css, t_css, p_t, t_t, p_n, t_n )
SELECT {group_cols:itemfmt='T.{{}}'}, P.[css], T.[css], P.[t], T.[t], P.[n], T.[n]
FROM {target_summary} AS T
INNER JOIN {parent_summary} AS P
ON ({parent_group_cols:' AND ',itemfmt='(T.{{0}}=P.{{0}})'})
WHERE ({target_group_cols:' AND ',itemfmt='(T.{{}} IS NOT NULL)'})
"""

_TTEST_QUERY_2 = """
UPDATE {ds_out} SET numerator =
    CASE WHEN p_n = t_n THEN 0 ELSE
        p_css - t_css
        + p_t * p_t / p_n
        - t_t * t_t / t_n
        - ( p_t-t_t ) * ( p_t-t_t ) / ( p_n-t_n )
    END
"""

_TTEST_QUERY_3 = """
UPDATE {ds_out} SET numerator =
    CASE WHEN numerator > 0 THEN numerator ELSE 0 END
"""

_TTEST_QUERY_4 = """
UPDATE {ds_out} SET ttest_se =
    CASE WHEN p_n - t_n > 1 THEN SQRT( numerator * ( p_n - t_n ) / ( p_n - t_n - 1 ) ) / p_n
         ELSE 0
    END
"""

_TTEST_QUERY_5 = """
UPDATE {ds_out} SET ttest_value =
    CASE WHEN ttest_se > 0 THEN ( t_t/t_n - p_t/p_n ) / ttest_se
         WHEN p_n - t_n > 1 AND t_t/t_n > p_t/p_n THEN 10 * {critval}
         WHEN p_n - t_n > 1 AND t_t/t_n < p_t/p_n THEN -10 * {critval}
         ELSE 0
    END
"""

_TTEST_QUERY_6 = """
UPDATE {ds_out} SET {output_col} =
    CASE WHEN ttest_value < -{critval} THEN 1
         WHEN ttest_value > {critval} THEN 3
         ELSE 2
    END
"""

_TTEST_QUERY_7 = """
UPDATE {ds_out} SET {output_col}=2
WHERE CAST( 0.5 + {rrv} * p_t / p_n AS INT ) = CAST( 0.5 + {rrv} * t_t / t_n AS INT )
"""

_FINAL_OUTPUT_QUERY = """
INSERT INTO {ds_out}({columns})
SELECT {columns} FROM {ds_temp}
"""
__all__ = ['TTestLevel']

class TTestLevel( object ):
    def __init__( self, ds_in=None, ds_out=None, db_context=None,
                  input_col_name=None, output_col_name=None,
                  target_group_cols=[], parent_group_cols=[],
                  target_where_expression=None,
                  parent_where_expression=None,
                  critical_value=None, round_value=1.0 ):
        self.ds_in = ds_in
        self.ds_out = ds_out
        self.db_context = db_context
        self.input_col_name = input_col_name
        self.output_col_name = output_col_name
        self.target_group_cols = target_group_cols
        self.parent_group_cols = parent_group_cols
        self.target_where_expression = target_where_expression
        self.parent_where_expression=parent_where_expression
        self.critical_value = critical_value
        self.round_value = round_value
        
    def execute( self ):
        self.validate()
        self.confirm_input_nonmissing()
        target_sum = self.generate_summary_stats( self.target_where_expression, self.parent_group_cols, self.target_group_cols )
        self.confirm_groups_consistent( target_sum )
        self.confirm_parents_nonmissing( target_sum )
        parent_sum = self.generate_summary_stats( self.parent_where_expression, self.parent_group_cols )
        self.create_output( parent_sum, target_sum )
        parent_sum.drop()
        target_sum.drop()
        
        
    def validate(self):
        # Make sure that the input and output tables are in the form of
        # TableSpec objects
        self.ds_in = get_table_spec( self.ds_in, self.db_context )
        self.db_context = self.ds_in.db_context
        self.run_context = self.db_context.runContext

        # Make sure that ds_in exists
        # (SAS 88-96)
        if not table_exists( self.ds_in ):
            raise ValueError( 'Input table {} does not exist'.format( self.ds_in ) )
        self.ds_in.populate_from_connection()

        # Process output table name
        # (SAS 98-101)
        drop_table_if_exists( self.ds_out, self.db_context )
        self.ds_out = self.db_context.getTableSpec( self.ds_out )
        
        # Process output variable name
        # (SAS 103-111)
        self.output_col = FieldSpec( field_name=self.output_col_name,
                basic_type="TINYINT", nullable=True )
        self.ds_out.add( self.output_col )
        
        # Process critical value
        # (SAS 113-117)
        if self.critical_value is None:
            self.critical_value = DEFAULT_CRITICAL_VALUE
            self.run_context.warning( "Critical value has been set to default {}"
                    .format( DEFAULT_CRITICAL_VALUE ) )
        
        # Process target groups
        # (SAS 118-121; 135-140)
        if len( self.target_group_cols ) == 0:
            raise ValueError( "List of target group columns not supplied" )
        try:
            for i in range( len( self.target_group_cols ) ):
                self.target_group_cols[ i ] = \
                        self.ds_in[ self.target_group_cols[ i ] ]
        except KeyError:
            raise ValueError( "Target group column {} not found in table {}"
                    .format( self.target_group_cols[ i ], self.ds_in ) )

        # Process parent groups
        # (SAS 123-126; 141-146)
        if len( self.parent_group_cols ) == 0:
            raise ValueError( "List of parent group columns not supplied" )
        for i in range( len( self.parent_group_cols ) ):
            try:
                self.parent_group_cols[ i ] = \
                        self.ds_in[ self.parent_group_cols[ i ] ]
            except KeyError:
                raise ValueError( "Parent group column {} not found in table {}"
                        .format( self.parent_group_cols[ i ], self.ds_in ) )

        # Select target group columns that are not in parent group columns
        # No corresponding code in SAS. Deals with case where columns defining
        # the parent group and columns defining the target group overlap
        self.extra_target_group_cols = []
        for col in self.target_group_cols:
            if col not in self.parent_group_cols:
                self.extra_target_group_cols.append( col )

        # Process input variable
        # (SAS 128-134)
        try:
            self.input_col = self.ds_in[ self.input_col_name ]
        except KeyError:
            raise ValueError( "Input variable {} not found in table {}"
                    .format( self.input_col_name, self.ds_in ) )
            
        # Clean where clauses
        self.parent_where_expression = self.clean_where( self.parent_where_expression )
        self.target_where_expression = self.clean_where( self.target_where_expression )
        
        # A more useful number for rounding test
        self.reciprocal_round_value = 1.0/self.round_value
             

    def clean_where( self, expression ):
        """Clean off a "WHERE" if provided, and normalize None and empty values
        to "TRUE"
        """
        if expression is None:
            return '1=1'
        expression = expression.strip()
        parts = expression.split( None, 1 )
        if len( parts ) == 0 or parts[0] == '':
            return '1=1'
        if len( parts ) == 1:
            return expression
        if parts[0].upper() == '1=1':
            return parts[1]
        return expression
    
    def confirm_input_nonmissing(self):
        # (SAS 160-181)
        query = _MISSING_INVAR_QUERY.format(
                ds_in=self.ds_in,
                input_col=self.input_col,
                where=self.target_where_expression)
        ans = self.db_context.execute( query )
        n = ans[0][0]
        if n != 0:
            raise ValueError( "{} observations had missing input variable {} where {} is TRUE"
                    .format( n, self.input_col, self.target_where_expression ) )

    def generate_summary_stats( self, where_expression, *args ):
        # (SAS 188-192; 228-233)
        group_cols = Joiner( *args )
        summary_table = get_temp_table( self.db_context )
        query = _SUMMARY_QUERY.format( input_col=self.input_col,
                                       summary_table=summary_table,
                                       ds_in=self.ds_in,
                                       group_cols=group_cols,
                                       where=where_expression )
        self.db_context.executeNoResults( query )
        summary_table.populate_from_connection
        return summary_table
    
    def confirm_groups_consistent( self, summary_table ):
        # (SAS 198-207)
        query = _CONFLICTING_GROUPS_QUERY.format(
                target_summary_table=summary_table,
                all_group_cols = Joiner( self.parent_group_cols, self.extra_target_group_cols ),
                target_group_cols = Joiner( self.target_group_cols ) )
        ans = self.db_context.execute( query )
        n = ans[0][0]
        if n > 1:
            raise ValueError( "Some target groups contain more than one parent group" )

    def confirm_parents_nonmissing( self, summary_table ):
        # (SAS 209-220)
        query = _MISSING_PARENTS_QUERY.format(
                target_summary_table=summary_table,
                parent_group_cols = Joiner( self.parent_group_cols ),
                target_group_cols = Joiner( self.target_group_cols ) )
        ans = self.db_context.execute( query )
        n = ans[0][0]
        if n > 1:
            raise ValueError( "Some target groups contain more than one parent group" )

    def create_output( self, parent_summary, target_summary ):
        
        self.ds_out.add_all( self.parent_group_cols )
        self.ds_out.add_all( self.extra_target_group_cols )

        with get_temp_table(self.db_context) as ds_temp:
            ds_temp.add_all( self.ds_out )
            ds_temp.add( FieldSpec( field_name="numerator", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="ttest_se", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="ttest_value", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="p_css", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="t_css", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="p_t", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="t_t", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="p_n", basic_type="FLOAT" ) )
            ds_temp.add( FieldSpec( field_name="t_n", basic_type="FLOAT" ) )
            self.db_context.executeNoResults( ds_temp.definition )
            
            query = _TTEST_QUERY_1.format(
                    ds_out=ds_temp,
                    group_cols=Joiner( self.parent_group_cols, self.extra_target_group_cols ),
                    parent_summary=parent_summary,
                    target_summary=target_summary,
                    parent_group_cols=Joiner( self.parent_group_cols),
                    target_group_cols=Joiner( self.target_group_cols) )
            self.db_context.executeNoResults( query )
            
            query = _TTEST_QUERY_2.format( ds_out=ds_temp )
            self.db_context.executeNoResults( query )
    
            query = _TTEST_QUERY_3.format( ds_out=ds_temp )
            self.db_context.executeNoResults( query )
    
            query = _TTEST_QUERY_4.format( ds_out=ds_temp )
            self.db_context.executeNoResults( query )
    
            query = _TTEST_QUERY_5.format( ds_out=ds_temp,
                                           critval=self.critical_value )
            self.db_context.executeNoResults( query )
    
            query = _TTEST_QUERY_6.format( ds_out=ds_temp,
                                           critval=self.critical_value,
                                           output_col=self.output_col )
            self.db_context.executeNoResults( query )
            
            query = _TTEST_QUERY_7.format( ds_out=ds_temp,
                                           rrv=self.reciprocal_round_value,
                                           output_col=self.output_col )
            self.db_context.executeNoResults( query )
            
            self.db_context.executeNoResults( self.ds_out.definition )
            query = _FINAL_OUTPUT_QUERY.format( ds_out=self.ds_out,
                                                ds_temp=ds_temp,
                                                columns=Joiner( self.ds_out ) )
            self.db_context.executeNoResults( query )
