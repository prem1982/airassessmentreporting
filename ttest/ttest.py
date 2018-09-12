'''
Created on May 29, 2013

@author: temp_dmenes
'''

from airassessmentreporting.airutility import ( get_table_spec, SafeExcelReader, dump, table_exists, Joiner, get_temp_table,
                                                TableSpec, FieldSpec, drop_table_if_exists )
from airassessmentreporting.airutility.formatutilities import db_identifier_quote
from airassessmentreporting.ttestlevel import TTestLevel

__all__ = ['TTest']

MIN_CRITVAL = 1.28
MAX_CRITVAL = 3.89

_GET_INPUT_VARIABLES_QUERY = """
SELECT {vars}
INTO {ttest_input_table}
FROM {ds_in}
"""
#WHERE {where_t} AND {where_p}

_FILL_OUTPUT_TABLE_QUERY = """
INSERT INTO {output_table}( {target_id} )
SELECT DISTINCT {target_id} FROM {input_table}
WHERE {where_t}
"""

_ACCUMULATE_RESULTS_QUERY = """
UPDATE {output_table}
SET {output_table}.{output_var}={ttest_table}.{output_var}
FROM {output_table}
LEFT JOIN {ttest_table}
ON {output_table}.{target_id}={ttest_table}.{target_id}
"""

class TTest( object ):
    def __init__( self, ds_in=None, db_context=None,
                  agg_ds=None, agg_sheet='compttest', use_excel=True  ):
        self.ds_in = ds_in
        self.db_context = db_context
        self.agg_ds = agg_ds
        self.agg_sheet = agg_sheet
        self.use_excel= use_excel
        
    def execute( self ):
        
        self.run_context.debug( "Running ttests" )
        with get_temp_table( self.db_context ) as ttest_input_table, get_temp_table( self.db_context ) as ttest_output_table:
            ttest_level = TTestLevel( ds_in=ttest_input_table, ds_out=ttest_output_table, db_context=self.db_context,
                                      round_value=1 )
            
            for target_level in self.target_levels:
                drop_table_if_exists( target_level.output_table )
                self.db_context.executeNoResults( target_level.output_table.definition )
                where_t = target_level.get_where_expression()
                self.db_context.executeNoResults( _FILL_OUTPUT_TABLE_QUERY.format(
                        output_table=target_level.output_table,
                        input_table=self.ds_in,
                        target_id=target_level.output_table[ target_level.id ],
                        where_t=where_t,
                ))
                vars_target = set( target_level.where )
                vars_target.add( target_level.id )
                for parent_level in target_level.contents:
                    where_p = parent_level.get_where_expression()
                    vars_parent = set( parent_level.where )
                    vars_parent.add( parent_level.id )
                    vars_parent.update( vars_target )
                    for row in parent_level.contents:
                        vars_row = set( vars_parent )
                        vars_row.add( row.inputvar )
                        
                        self.run_context.debug( "Running ttest for variable {} comparing {} to {}".format(
                                row.inputvar, target_level.level, parent_level.level ) )
                        drop_table_if_exists( ttest_input_table )
                        query = _GET_INPUT_VARIABLES_QUERY.format( ds_in=self.ds_in, ttest_input_table=ttest_input_table,
                                vars=Joiner( vars_row ),
                                where_t=where_t, where_p=where_p )
                        self.db_context.executeNoResults( query )
                        
                        ttest_level.input_col_name=row.inputvar
                        ttest_level.output_col_name=row.outputvar
                        ttest_level.critical_value=row.critval
                        ttest_level.target_group_cols=[ target_level.id ]
                        ttest_level.parent_group_cols=[ parent_level.id ]
                        ttest_level.target_where_expression = where_t
                        ttest_level.parent_where_expression = where_p
                        
                        ttest_level.execute()
                        
                        query = _ACCUMULATE_RESULTS_QUERY.format( output_table=target_level.output_table,
                                ttest_table=ttest_output_table, output_var=row.outputvar, target_id=target_level.id )
                        self.db_context.executeNoResults( query )
                    
        
    def readAggData(self):
        
        # Validate the input file
        self.ds_in = get_table_spec( self.ds_in, self.db_context )
        self.ds_in.populate_from_connection()
        self.db_context = self.ds_in.db_context
        self.run_context = self.db_context.runContext
        self.run_context.debug( "processing control file" )
        if not table_exists( self.ds_in ):
            raise ValueError( "Input dataset {} does not exist".format( self.ds_in ) )
        
        # Read the control file
        # SAS 3-9
        if self.use_excel:
            reader = SafeExcelReader( self.run_context, self.agg_ds, self.agg_sheet )
            self.agg_data = [ row for row in reader.getRows() ]
        else:
            self.agg_ds = get_table_spec( self.agg_ds, self.db_context )
            self.agg_data = dump( self.agg_ds )
        
        # Validate the control file columns
        # SAS 10-28
        missing_vars = set()
        for var_name in [ 'outputvar', 'inputvar', 'targetlevel', 'targetid', 'wheret', 'wheret_value', 'parentlevel',
                          'parentid', 'wherep', 'wherep_value', 'critval' ]:
            if var_name not in self.agg_data[0]:
                missing_vars.add( var_name )
        if missing_vars:
            raise ValueError( "TTest control sheet lacks required columns: {:', '}".format( Joiner( missing_vars ) ) )
        
        # Validate existence of requested columns
        # SAS 29-86
        for row in self.agg_data:
            if row.wheret is None:
                row.wheret = []
            else:
                row.wheret = [ x.strip().lower() for x in row.wheret.strip().split( '*' )]
                
            if row.wherep is None:
                row.wherep = []
            else:
                row.wherep = [ x.strip().lower() for x in row.wherep.strip().split( '*' )]
                
            if row.wheret_value is None:
                row.wheret_value = []
            else:
                row.wheret_value = [ x.strip() for x in row.wheret_value.strip().split( ' ' )]
                
            if row.wherep_value is None:
                row.wherep_value = []
            else:
                row.wherep_value = [ x.strip() for x in row.wherep_value.strip().split( ' ' )]
                
            row.inputvar = row.inputvar.lower().strip()
            row.targetid = row.targetid.lower().strip()
            row.parentid = row.parentid.lower().strip()
            row.targetlevel = row.targetlevel.lower().strip()
            row.parentlevel = row.parentlevel.lower().strip()

            for var_name in ( row.wheret
                              + row.wherep
                              + [ row.inputvar, row.targetid, row.parentid ] ):
                if var_name != '' and var_name not in self.ds_in:
                    missing_vars.add( var_name )
        if missing_vars:
            raise ValueError( "TTest input data lacks required variables: {:', '}".format( Joiner( missing_vars ) ) )
        
        # Sort control data
        #SAS 87-90
        self.agg_data.sort( key=lambda row : ( row.targetlevel, row.parentlevel ) )
        
        # Check for consistency across "target" and "parent" variables.
        #SAS 91-222
        last_targetlevel = _NONE_LEVEL
        last_parentlevel = _NONE_LEVEL
        self.target_levels = []
        messages = []
        
        for row in self.agg_data:
            wheret = tuple( row.wheret )
            wheret_value = tuple ( row.wheret_value )
            if len( wheret ) != len( wheret_value ):
                messages.append( 'Number of wheret_value items must match number of wheret items ("{0}" vs "{1}")'.format(
                        row.wheret, row.wheret_value ) )
            if row.targetlevel != last_targetlevel.level:
                last_targetlevel = LevelData( row.targetlevel, row.targetid, wheret, wheret_value )
                self.target_levels.append( last_targetlevel )
                last_parentlevel = _NONE_LEVEL
                
                # Create an output table in which to accumulate the results
                table_name = 'ttest_' + row.targetlevel
                last_targetlevel.output_table = TableSpec( self.db_context, table_name )
                last_targetlevel.output_table.add( self.ds_in[ row.targetid ].clone() )
            else:
                last_targetlevel.check( row.targetid, wheret, messages )
            
            wherep = tuple( row.wherep )
            wherep_value = tuple ( row.wherep_value )
            if len( wherep ) != len( wherep_value ):
                messages.append( 'Number of wherep_value items must match number of wherep items ("{0}" vs "{1}")'.format(
                        row.wherep, row.wherep_value ) )
            if row.parentlevel != last_parentlevel.level:
                last_parentlevel = LevelData( row.parentlevel, row.parentid, wherep, wherep_value )
                last_targetlevel.contents.append( last_parentlevel )
            else:
                last_parentlevel.check( row.parentid, wherep, messages )
                
            last_parentlevel.contents.append( row )
            last_targetlevel.output_table.add( FieldSpec( row.outputvar, 'TINYINT' ) )
            
            try:
                row.critval = float( row.critval )
                if not MIN_CRITVAL <= row.critval <= MAX_CRITVAL:
                    messages.append( "Bad critical value {} is not between {} and {}".format(
                            row.critval, MIN_CRITVAL, MAX_CRITVAL ) )
            except ValueError:
                messages.append( "Critical value {} is not a float".format( row.critval ) )
                
            try:
                row.outputvar = db_identifier_quote( row.outputvar )
            except ValueError:
                messages.append( "Output variable name {} is not a valid database identifier".format( row.outputvar ) )
                
            try:
                row.targetlevel = db_identifier_quote( row.targetlevel )
            except ValueError:
                messages.append( "Target level name {} is not a valid database identifier".format( row.targetlevel ) )
                
        for message in messages:
            self.run_context.error( message )
        
        if messages:
            raise ValueError( "Invalid inputs to ttest macro. See log for details" )

class LevelData( object ):
    def __init__( self, level, ID, where, where_value ):
        self.level = level
        self.id = ID
        self.where = where
        self.where_value = where_value
        self.contents = []
        
    def check( self, ID, where, messages ):
        if self.where != where:
            messages.append( ( 'All WHERE variables for a level must match. Found {}, expecting {}'.format( where, self.where ) ) )
        if self.id != ID:
            messages.append( ( 'All ID values for a level must match. Found {}, expecting {}'.format( id, self.id ) ) )
            
    def get_where_expression(self):
        phrases = []
        for var, val in zip( self.where, self.where_value ):
            if var and val:
                phrases.append( '([{}]={})'.format( var, val ) )
        if phrases:
            return '(' + ' AND '.join( phrases ) + ')'
        else:
            return '(1=1)' 
        
        
_NONE_LEVEL = LevelData( None, None, None, None )

if __name__ == '__main__':
    from airassessmentreporting.testutility import SuiteContext, RunContext
#     RC = SuiteContext('unittest')
    print 'ttest started'
    RC = RunContext('sharedtest',)
    dbcontext = RC.getDBContext()
    ttester = TTest( 'student_aggregation' , dbcontext, 'C:\CVS projects\CSSC Score Reporting\OGT Summer 2012\Code\Development\Superdata\AggregationSheet.xls',"ttest",  True )
#    ttester = TTest( 'student_aggregation_sas' , dbcontext, 'C:\spec.xlsx',"ttest",  True )
    ttester.readAggData()
    ttester.execute()
    print 'ttest finished'
