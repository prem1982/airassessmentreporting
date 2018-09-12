'''Data structure carries the definition of a merge

@author Dan Menes

'''

import copy
import logging
from numbers import Number

from airassessmentreporting.airutility import ( FieldSpec, TableSpec,
                drop_table_if_exists, Joiner, get_temp_table, get_table_spec )

__all__ = [
           'MergeFieldSpec', 'MergeDef', 'DEFAULT_SIMILARITY_THRESHOLD', 'PRIORITY_LEFT',
           'PRIORITY_LEFT_NONMISSING', 'PRIORITY_LEFT_ONLY', 'PRIORITY_RIGHT',
           'PRIORITY_RIGHT_NONMISSING', 'PRIORITY_RIGHT_ONLY',
           'JOIN_TYPE_INNER', 'JOIN_TYPE_LEFT', 'JOIN_TYPE_RIGHT', 'JOIN_TYPE_FULL'
]

DEFAULT_SIMILARITY_THRESHOLD = 0.8
PRIORITY_LEFT = 'L'
PRIORITY_RIGHT = 'R'
PRIORITY_LEFT_NONMISSING = 'LN'
PRIORITY_RIGHT_NONMISSING = 'RN'
PRIORITY_LEFT_ONLY = 'LO'
PRIORITY_RIGHT_ONLY = 'RO'
JOIN_TYPE_LEFT = 'L'
JOIN_TYPE_RIGHT = 'R'
JOIN_TYPE_FULL = 'F'
JOIN_TYPE_INNER = 'I'

LOGGER = logging.getLogger('mergedef')

_FIND_DUPLICATES_QUERY = '''
INSERT INTO {dup_table:qualified}(
        {foreign_key:',\\n        ',itemfmt='{{:alias_or_name}}'},
        {required_key:',\\n        '},
        has_dups,
        has_missing)
    SELECT
        {foreign_key:',\\n',itemfmt='B.{{0:alias_or_name}} AS {{0:alias_or_name}}'},
        {required_key:',\\n',itemfmt='B.{{:{side}_with_as}}'},
        ( CASE A.n_dups WHEN 1 THEN 0 ELSE 1 END ) AS has_dups,
        B.has_missing
    FROM
        ( SELECT {required_key:',\\n',itemfmt='{{:{side} }}'}, COUNT(1) as n_dups
          FROM {input_table:qualified}
          GROUP BY {required_key:itemfmt='{{:{side} }}'}
        ) AS A
    RIGHT JOIN
        ( SELECT
            {foreign_key:',\\n',itemfmt='{{0:foreign}} AS {{0:alias_or_name}}'},
            {required_key:',\\n',itemfmt='{{:{side} }}'},
            ( CASE WHEN ({required_key:' OR ',itemfmt='({{:{side} }} IS NULL)'}) THEN 1 ELSE 0 END ) AS has_missing
          FROM {input_table:qualified}
        ) AS B
    ON {required_key:' AND ',item='K',itemfmt='A.{{K:{side} }}=B.{{K:{side} }}'}
'''
_ADD_REJECTS_QUERY="""
    INSERT INTO {reject_table:qualified}(
        {keys:',\\n',itemfmt='{{:alias_or_name}}'},
        merge_report)
    SELECT
        {keys:',\\n',itemfmt='{{:alias_or_name}}'},
        {reason_expression}
    FROM {from_table}
    WHERE {condition}
"""
_DELETE_ROWS_QUERY="""
    DELETE FROM {from_table} WHERE {condition}
"""

_DUPS_BOTH_QUERY="""
    SELECT {key:itemfmt='L.{{}}'}
    INTO {full_dup_table:qualified}
    FROM
        ( SELECT DISTINCT {key} FROM {left_dup_table} WHERE has_dups>0 ) AS L
    INNER JOIN
        ( SELECT DISTINCT {key} FROM {right_dup_table} WHERE has_dups>0 ) AS R
    ON ({key:' AND ',item='K',itemfmt='(L.{{K}}=R.{{K}})'})
"""

_MARK_DUPS_BOTH_QUERY="""
    UPDATE A
    SET A.has_dups_both=1
    FROM {dup_table:qualified} AS A
    INNER JOIN {full_dup_table:qualified} AS B
    ON ({key:' AND ',item='K',itemfmt='(A.{{K}}=B.{{K}})'})
"""

_FIRST_MERGE_QUERY="""
    INSERT INTO {merge_table:qualified}(
        {required_key:',\\n'},
        {foreign_key_l:',\\n'},
        {foreign_key_r:',\\n'},
        has_dups_l,
        has_dups_r,
        reject )
    SELECT
        {required_key:',\\n',item='K',itemfmt='COALESCE( L.{{K}}, R.{{K}} ) AS {{K}}'},
        {foreign_key_l:',\\n',itemfmt='L.{{}}'},
        {foreign_key_r:',\\n',itemfmt='R.{{}}'},
        L.has_dups AS has_dups_l,
        R.has_dups AS has_dups_r,
        0 as reject
    FROM {left_dup_table} AS L
    FULL JOIN {right_dup_table} AS R
    ON ({required_key:' AND ',item='K',itemfmt='L.{{K}}=R.{{K}}'})
"""

_ADD_MORE_KEYS_QUERY="""
    UPDATE A
    SET {fields:',\\n',item='X',itemfmt='A.{{X}}=B.{{X:{side}}}'}
    FROM {merge_table} AS A
    INNER JOIN {from_table} AS B
    ON ({required_key:' AND ',item='K',itemfmt='A.{{K}}=B.{{K:foreign}}'})
"""

_OPTIONAL_MISMATCH_QUERY="""
    UPDATE {merge_table}
    SET reject=1, merge_report={reason_expression}
    WHERE ({indices:' OR ',item='I',itemfmt='((loptkey_{{I}} IS NOT NULL) AND (roptkey_{{I}} IS NOT NULL) AND (loptkey_{{I}}<>roptkey_{{I}}))'})
"""

_OPTIONAL_MISMATCH_MESSAGE="""(
    'Optional key mismatch ("' +
    {indices:"+'; '+",itemfmt="COALESCE( CAST( loptkey_{{}} AS NVARCHAR(255) ), '{{{{NULL}}}}' )"} +
    '" vs "'+
    {indices:"+'; '+",itemfmt="COALESCE( CAST( roptkey_{{}} AS NVARCHAR(255) ), '{{{{NULL}}}}' )"} +
    '")')
"""

_FUZZY_MATCH_QUERY="""
    UPDATE {merge_table}
    SET {indices:'\\n,',item='I',itemfmt='similarity_{{I}}={schema}.NameSimilarity(lfuzzykey_{{I}}_1,lfuzzykey_{{I}}_2,rfuzzykey_{{I}}_1,rfuzzykey_{{I}}_2)'}
"""

_FUZZY_MATCH_REPORT_QUERY="""
    SELECT
        {key_fields:',\\n'},
        {indices:'\\n,',item='I',itemfmt='similarity_{{I}},\\nlfuzzykey_{{I}}_1,\\nlfuzzykey_{{I}}_2,\\nrfuzzykey_{{I}}_1,\\nrfuzzykey_{{I}}_2'}
    INTO {report_table:qualified}
    FROM {merge_table:qualified}
    WHERE reject=0
"""

_FUZZY_MISMATCH_QUERY="""
    UPDATE {merge_table}
    SET reject=1,
        merge_report=CASE WHEN merge_report IS NULL THEN ({reason_expression}) ELSE merge_report + '; ' + {reason_expression} END
    WHERE ({thresholds:' OR ',index='I',item='THRESHOLD',itemfmt='(similarity_{{I}}<={{THRESHOLD}})'})
"""

_FUZZY_MISMATCH_MESSAGE="""(
    'Fuzzy key mismatch ("' +
    {indices:"+'; '+",item='I',itemfmt="COALESCE( CAST( lfuzzykey_{{I}}_1 AS NVARCHAR(255) ), '{{{{NULL}}}}' )+' '+COALESCE( CAST( lfuzzykey_{{I}}_2 AS NVARCHAR(255) ), '{{{{NULL}}}}' )"} +
    '" vs "'+
    {indices:"+'; '+",item='I',itemfmt="COALESCE( CAST( rfuzzykey_{{I}}_1 AS NVARCHAR(255) ), '{{{{NULL}}}}' )+' '+COALESCE( CAST( rfuzzykey_{{I}}_2 AS NVARCHAR(255) ), '{{{{NULL}}}}' )"} +
    '")')
"""

_MELD_FIELDS_QUERY="""
    INSERT 
    INTO {out_table:qualified} (
        {key_fields:',\\n        '},
        {meld_fields:',\\n        '}
    )
    SELECT
        {key_fields:',\\n        ',item='K',itemfmt='{merge_table.alias}.{{K}}'},
        {meld_fields:',\\n        ',itemfmt='{{:meld}}'}
    FROM (
            {merge_table:with_as}
        LEFT JOIN
            {left_table:with_as}
        ON ({left_key:' AND ',item='K',itemfmt='{{K:qualified}}={{K:foreign_qualified}}'})
        )
    LEFT JOIN
        {right_table:with_as}
    ON ({right_key:' AND ',item='K',itemfmt='{{K:qualified}}={{K:foreign_qualified}}'})
"""

_RESTORE_REJECTS_QUERY="""
    INSERT INTO {out_table}(
        {foreign_key:',\\n        '},
        {fields:',\\n        '}
    )
    SELECT
        {foreign_key:',\\n        ',itemfmt='{{:foreign_qualified}}'},
        {fields:',\\n        ',itemfmt='{{:{side}_qualified}}'}
    FROM {input_table:with_as}
    LEFT JOIN {out_table} AS A
    ON ({foreign_key:' AND ',itemfmt='({{0:foreign_qualified}}=A.{{0}})'})
    WHERE ({foreign_key:' AND ',itemfmt='(A.{{0}} IS NULL)'})
"""

_REMAIN_TABLE_QUERY="""
    INSERT INTO {remain_table:qualified}(
        {key_fields:',\\n        '},
        merge_report,
        {data_fields:',\\n        '}
    )
    SELECT
        {key_fields:',\\n        ',item='K',itemfmt='CAST({reject_table.alias}.{{K}} AS {{K.data_type}})'},
        {reject_table.alias}.merge_report,
        {data_fields:',\\n        ',itemfmt='CAST({{0:{side}}} AS {{0.data_type}})'}
    FROM
        {reject_table:with_as}
    INNER JOIN
        {input_table:with_as}
    ON ({key_fields:' AND ',item='K',itemfmt='{{K:qualified}}={{K:foreign_qualified}}'})
"""

class MergeFieldSpec( FieldSpec ):
    
    '''
    
    .. todo::
    
        Handling of differing types in source fields could be much better
        
    .. todo::
    
        Logic might be cleaned up by creating a subclass of FieldSpec to represent a missing
        source, rather than repeatedly testing for None.
        
    '''
    def __init__( self, left_field, right_field, priority_field ):
        
        # These will be accessed by the setter when the superclass constructor
        # tries to set the default value
        self.left_field = None
        self.right_field = None
        super( MergeFieldSpec, self ).__init__( )
        self.left_field = copy.copy( left_field )
        self.right_field = copy.copy( right_field )
        self.priority_field = priority_field
        
        
        if ( left_field is not None and right_field is not None ):
            self._field_name = left_field.field_name
            self.basic_type = left_field.basic_type
            self.radix = left_field.radix
            self.scale = left_field.scale
            if ( left_field.basic_type == right_field.basic_type ) :
                self.data_length = max( left_field.data_length, right_field.data_length )
                self.precision = max( left_field.precision, right_field.precision )
            else:
                self.data_length = left_field.data_length
                self.precision = left_field.precision
        elif left_field is not None:
            self._field_name = left_field.field_name
            self.basic_type = left_field.basic_type
            self.data_length = left_field.data_length
            self.precision = left_field.precision
            self.radix = left_field.radix
            self.scale = left_field.scale
        elif right_field is not None:
            self._field_name = right_field.field_name
            self.basic_type = right_field.basic_type
            self.data_length = right_field.data_length
            self.precision = right_field.precision
            self.radix = right_field.radix
            self.scale = right_field.scale

        
        self.default_value = None
        self.nullable = True
        
    @FieldSpec.field_name.setter
    def field_name( self, value ):
        '''
        .. todo::
            
            Could move test into the setter for FieldSpec.alias
            
        '''
        # The method fset does too exist--ignore the "error."  Eclipse doesn't
        # understand the difference between how a property appears on an instance and
        # how it appears on the class
        FieldSpec.field_name.fset( self, value )
        clean_value = self.field_name
        if self.left_field is not None and self.left_field.field_name != clean_value:
            self.left_field.alias = clean_value
        if self.right_field is not None and self.right_field.field_name != clean_value:
            self.right_field.alias = clean_value
        
        
    def __format__( self, spec ):
        spec = spec.strip()
        if spec == "left":
            if self.left_field is None:
                return "NULL"
            else:
                return self.left_field.field_name

        elif spec == "right":
            if self.right_field is None:
                return "NULL"
            else:
                return self.right_field.field_name
        
        if spec == "left_qualified":
            if self.left_field is None:
                return "NULL"
            elif self.left_field.data_type != self.data_type:
                return "CAST({} AS {})".format( self.left_field.qualified_name, self.data_type )
            else:
                return self.left_field.qualified_name

        elif spec == "right_qualified":
            if self.right_field is None:
                return "NULL"
            elif self.right_field.data_type != self.data_type:
                return "CAST({} AS {})".format( self.right_field.qualified_name, self.data_type )
            else:
                return self.right_field.qualified_name
        
        elif spec == "left_with_as":
            if self.left_field is None:
                return "NULL AS " + self.field_name
            elif self.left_field.field_name == self.field_name:
                return self.field_name
            else:
                return self.left_field.field_name + " AS " + self.field_name

        elif spec == "right_with_as":
            if self.right_field is None:
                return "NULL AS " + self.field_name
            elif self.right_field.field_name == self.field_name:
                return self.field_name
            else:
                return self.right_field.field_name + " AS " + self.field_name
            
        elif spec == "meld":
            return self.meldExpression
        
        return super( MergeFieldSpec, self ).__format__( spec )
            
    @property
    def meldExpression(self):
        if self.priority_field in ( PRIORITY_LEFT, PRIORITY_LEFT_ONLY ):
            return '{x:left_qualified}'.format( x=self )

        elif self.priority_field in ( PRIORITY_RIGHT, PRIORITY_RIGHT_ONLY ):
            return '{x:right_qualified}'.format( x=self )
        
        elif self.priority_field == PRIORITY_LEFT_NONMISSING:
            #return 'COALESCE({x:left_qualified},{x:right_qualified})'.format( x=self )
            return '''CASE {x:left_qualified} 
                        WHEN '' THEN {x:right_qualified}
                        ELSE COALESCE({x:left_qualified},{x:right_qualified})
                      END
                   '''.format( x=self )
        
        elif self.priority_field == PRIORITY_RIGHT_NONMISSING:
            #return 'COALESCE({x:right_qualified},{x:left_qualified})'.format( x=self )
            return '''CASE {x:right_qualified} 
                        WHEN '' THEN {x:left_qualified}
                        ELSE COALESCE({x:right_qualified},{x:left_qualified})
                      END
                   '''.format( x=self )
            
        raise ValueError( "Illegal field priority code: {}".format( self.priority_field ) )

class MergeDef( TableSpec ):
    '''Data structure carries the definition of a merge
    
    .. todo::
        Key mismatches don't report mismatched values in reject table
        
    .. todo::
        Fuzzy key match is probably not implementing correct logic for missing keys
    '''
    
    def __init__( self, db_context ):
        global LOGGER
        LOGGER = db_context.runContext.get_logger( 'mergedef' )
        super( MergeDef, self ).__init__( db_context, None )
        self.left_input_table = None
        self.right_input_table = None
        self.required_merge_keys = []
        self.optional_merge_keys = []
        self.fuzzy_merge_keys = []
        self.similarity_thresholds = DEFAULT_SIMILARITY_THRESHOLD
        self.allow_dups_left = False
        self.allow_dups_right = False
        self.allow_dups_both = False
        self.fuzzy_report_table = None
        self.left_remain_table = None
        self.right_remain_table = None
        self.join_type = JOIN_TYPE_INNER
        
        
    def get_actual_tables(self, messages):
        succeed = True
        if self.left_input_table is None:
            messages.append( "Left input table not provided." )
            succeed = False
        else:
            self.left_input_table = get_table_spec( self.left_input_table, self.db_context )
            if len( self.left_input_table.primary_key ) == 0:
                messages.append( "Left input table {name} must have a primary key.".format( name=self.left_input_table.table_name ) )
        if self.right_input_table is None:
            messages.append( "Right input table not provided." )
            succeed = False
        else:
            self.right_input_table = get_table_spec( self.right_input_table, self.db_context )
            if len( self.right_input_table.primary_key ) == 0:
                messages.append( "Right input table {name} must have a primary key.".format( name=self.right_input_table.table_name ) )
        return succeed
            
    def validate(self):
        messages = []
        if len( self.required_merge_keys ) == 0:
            messages.append( "No required keys provided for merge" )
        if self.get_actual_tables( messages ):
            self._check_key_in_tables( self.required_merge_keys, "Required key", messages )
            self._check_key_in_tables( self.optional_merge_keys, "Optional key", messages )
            
            for item in self.fuzzy_merge_keys:
                if isinstance( item, MergeFieldSpec ):
                    item = ( item, )
                if not 1 <= len( item ) <= 2:
                    messages.append( "Fuzzy key must be a merge field specifier or a 1 or 2 item tuple" )
                self._check_key_in_tables( item, "Fuzzy key", messages )
                
            if isinstance( self.fuzzy_report_table, ( str, unicode ) ):
                self.fuzzy_report_table = self.db_context.getTableSpec( self.fuzzy_report_table )
            
            if isinstance( self.left_remain_table, ( str, unicode ) ):
                self.left_remain_table = self.db_context.getTableSpec( self.left_remain_table )
            
            if isinstance( self.right_remain_table, ( str, unicode ) ):
                self.right_remain_table = self.db_context.getTableSpec( self.right_remain_table )
                
            # Expand the similarity if necessary
            if isinstance( self.similarity_thresholds, Number ):
                self.similarity_thresholds = ( self.similarity_thresholds, ) * len( self.fuzzy_merge_keys )
                
            if len( self.similarity_thresholds ) != len( self.fuzzy_merge_keys ):
                messages.append( "Similarity threshold must be either a single number or a sequence of the same length as the fuzzy merge keys" )
            
        for message in messages:
            self.db_context.runContext.error( message )
        if len( messages ) > 0:
            raise ValueError( "MergeDef object improperly initialized" )
        
        
        

    def _check_key_in_tables( self, key_set, key_type, messages ):
        for key in key_set:
            if key.left_field is None:
                messages.append( "{key_type} key {key} not found in left table {table}".format(
                        key_type=key_type, key=key, table=self.left_input_table ) )
            if key.right_field is None:
                messages.append( "{key_type} key {key} not found in right table {table}".format(
                        key_type=key_type, key=key, table=self.righInputTable ) )

    def execute( self ):
        
        # Validate inputs
        self.validate()
        
        # Delete output tables if they exist
        drop_table_if_exists( self )
        if self.fuzzy_report_table:
            drop_table_if_exists( self.fuzzy_report_table )
        if self.left_remain_table:
            drop_table_if_exists( self.left_remain_table )
        if self.right_remain_table:
            drop_table_if_exists( self.right_remain_table )
        
        # Scan for illegal duplication of required keys in both tables
        left_dup_table, left_reject_table= self._process_required_key_dups_and_missing(
                self.db_context, self.left_input_table, 'left', self.allow_dups_left )
        right_dup_table, right_reject_table = self._process_required_key_dups_and_missing(
                self.db_context, self.right_input_table, 'right', self.allow_dups_right )
        
        
        # We will create an initial table that contains only the required keys,
        # optional keys, fuzzy keys, foreign keys and the duplicate detection columns
        merge_table, left_fields, right_fields = self._create_merge_table( )

        # If necessary, remove duplicates that appear in both tables
        if self.allow_dups_left and self.allow_dups_right and not self.allow_dups_both:
            with get_temp_table( self.db_context ) as full_dup_table:
            
                query = _DUPS_BOTH_QUERY.format( left_dup_table=left_dup_table,
                                                 right_dup_table=right_dup_table,
                                                 full_dup_table=full_dup_table,
                                                 key=Joiner( self.required_merge_keys ) )
                self.db_context.executeNoResults( query )
                query = _MARK_DUPS_BOTH_QUERY.format( dup_table=left_dup_table,
                                                      full_dup_table=full_dup_table,
                                                      key=Joiner( self.required_merge_keys ) )
                self.db_context.executeNoResults( query )
                self._move_rejects( left_dup_table, left_reject_table, 0,
                                   'has_dups_both<>0', "'Duplicate required key on both sides'" )
                query = _MARK_DUPS_BOTH_QUERY.format( dup_table=right_dup_table,
                                                      full_dup_table=full_dup_table,
                                                      key=Joiner( self.required_merge_keys ) )
                self.db_context.executeNoResults( query )
                self._move_rejects( right_dup_table, right_reject_table, 0,
                                   'has_dups_both<>0', "'Duplicate required key on both sides'" )
                
        
        # Perform the first merge (required key)
        query = _FIRST_MERGE_QUERY.format(
                merge_table=merge_table,
                required_key=Joiner( self.required_merge_keys ),
                foreign_key_l=Joiner( merge_table.foreign_keys[0] ),
                foreign_key_r=Joiner( merge_table.foreign_keys[1] ),
                left_dup_table=left_dup_table,
                right_dup_table=right_dup_table )
        self.db_context.executeNoResults( query )
        left_dup_table.drop()
        right_dup_table.drop()
        
        #Remove rejects after the first merge
        self._move_rejects( merge_table, left_reject_table, 0,
                           'has_dups_r IS NULL', "'No required key match'" )
        self._move_rejects( merge_table, right_reject_table, 1,
                           'has_dups_l IS NULL', "'No required key match'" )
        
        # Bring in optional and fuzzy keys
        if len( left_fields ) > 0:
            query = _ADD_MORE_KEYS_QUERY.format( fields=Joiner( left_fields ),
                    merge_table=merge_table, from_table=self.left_input_table, side='left',
                    required_key=Joiner( merge_table.foreign_keys[0] ) )
            self.db_context.executeNoResults( query )
        if len( right_fields ) > 0:
            query = _ADD_MORE_KEYS_QUERY.format( fields=Joiner( right_fields ),
                    merge_table=merge_table, from_table=self.right_input_table, side='right',
                    required_key=Joiner( merge_table.foreign_keys[1] ) )
            self.db_context.executeNoResults( query )
        
        key_fields=Joiner( self.required_merge_keys, merge_table.foreign_keys[0], merge_table.foreign_keys[ 1 ] )

        # Flag matches for rejection based on optional keys
        if len( self.optional_merge_keys ) > 0:
            indices=Joiner( range( 1, len( self.optional_merge_keys ) + 1 ) )
            reason_expression = _OPTIONAL_MISMATCH_MESSAGE.format( indices=indices )
            query = _OPTIONAL_MISMATCH_QUERY.format( merge_table=merge_table, indices=indices,
                    reason_expression=reason_expression )
            self.db_context.executeNoResults( query )
        
        # Flag matches for rejection based on fuzzy keys
        if len( self.fuzzy_merge_keys ) > 0:
            indices=Joiner( range( 1, len( self.fuzzy_merge_keys ) + 1 ) )
            query = _FUZZY_MATCH_QUERY.format( merge_table=merge_table, indices=indices, schema=self.db_context.schema )
            self.db_context.executeNoResults( query )
        
            # Create fuzzy report table
            if self.fuzzy_report_table is not None:
                query = _FUZZY_MATCH_REPORT_QUERY.format( key_fields=key_fields, indices=indices,
                                                          report_table=self.fuzzy_report_table, merge_table=merge_table )
                self.db_context.executeNoResults( query )
                
            # Drop fuzzy mismatches
            reason_expression = _FUZZY_MISMATCH_MESSAGE.format( indices=indices )
            query = _FUZZY_MISMATCH_QUERY.format( merge_table=merge_table,
                    thresholds=Joiner( self.similarity_thresholds ),
                    schema=self.db_context.schema,
                    reason_expression=reason_expression )
            self.db_context.executeNoResults( query )
            
        # Move keys rejected due to optional or fuzzy matches
        reason_expression="{fld:qualified}".format( fld=merge_table['merge_report'] )
        self._copy_rejects( merge_table, left_reject_table, 0,
                           '(reject<>0)',
                           reason_expression )
        self._copy_rejects( merge_table, right_reject_table, 1,
                           '(reject<>0)',
                           reason_expression )
        self._delete_rows( merge_table,
                             '(reject<>0)' )
        
        # Meld columns in main merge table, including data columns that did not participate in the merge
        self.left_input_table.alias="L"
        self.right_input_table.alias="R"
        merge_table.alias="A"
        key_field_names = [ x.field_name for x in key_fields ]
        original_fields = self[:]
        meld_fields = [ x for x in self if x.field_name not in key_field_names ]
        for key_field in key_fields:
            if key_field not in self:
                self.add( key_field.clone() )
        
        self.db_context.executeNoResults( self.definition );
        
        query = _MELD_FIELDS_QUERY.format(
                merge_table=merge_table,
                out_table=self,
                meld_fields=Joiner( meld_fields ),
                key_fields=key_fields,
                left_table=self.left_input_table,
                left_key=Joiner( merge_table.foreign_keys[0] ),
                right_table=self.right_input_table,
                right_key=Joiner(merge_table.foreign_keys[1]) )
        
        self.db_context.executeNoResults( query )
        
        # Add non-matched records for outer joins
        if self.join_type in ( JOIN_TYPE_LEFT, JOIN_TYPE_FULL ):
            query=_RESTORE_REJECTS_QUERY.format( out_table=self,
                                                 reject_table=left_reject_table,
                                                 input_table=self.left_input_table,
                                                 fields=Joiner( original_fields ),
                                                 foreign_key=Joiner( left_reject_table.foreign_keys[0] ),
                                                 side='left' )
            self.db_context.executeNoResults( query )
        
        if self.join_type in ( JOIN_TYPE_RIGHT, JOIN_TYPE_FULL ):
            query=_RESTORE_REJECTS_QUERY.format( out_table=self,
                                                 reject_table=right_reject_table,
                                                 input_table=self.right_input_table,
                                                 fields=Joiner( original_fields ),
                                                 foreign_key=Joiner( right_reject_table.foreign_keys[0] ),
                                                 side='right' )
            self.db_context.executeNoResults( query )
        
        # Bring into the remainder tables the data columns that did not participate in the merge
        if self.left_remain_table is not None and self.join_type in ( JOIN_TYPE_RIGHT, JOIN_TYPE_INNER ):
            # Create the table
            del self.left_remain_table[:]
            self.left_remain_table.add_all( left_reject_table.foreign_keys[0] )
            self.left_remain_table.add_all( meld_fields )
            self.left_remain_table.add( left_reject_table.merge_report.clone() )
            self.db_context.executeNoResults( self.left_remain_table.definition )
            
            left_reject_table.alias="A"
            query=_REMAIN_TABLE_QUERY.format(
                key_fields = Joiner( left_reject_table.foreign_keys[0] ),
                data_fields = Joiner( meld_fields ),
                remain_table = self.left_remain_table,
                reject_table = left_reject_table,
                input_table = self.left_input_table,
                side='left' )
            
            self.db_context.executeNoResults( query )
            
        if self.right_remain_table is not None and self.join_type in ( JOIN_TYPE_LEFT, JOIN_TYPE_INNER ):
            del self.right_remain_table[:]
            self.right_remain_table.add_all( right_reject_table.foreign_keys[0] )
            self.right_remain_table.add_all( meld_fields )
            self.right_remain_table.add( right_reject_table.merge_report.clone() )
            self.db_context.executeNoResults( self.right_remain_table.definition )

            right_reject_table.alias="A"
            query=_REMAIN_TABLE_QUERY.format(
                key_fields = Joiner( right_reject_table.foreign_keys[0] ),
                data_fields = Joiner( meld_fields ),
                remain_table = self.right_remain_table,
                reject_table = right_reject_table,
                input_table = self.right_input_table,
                side='right' )
            self.db_context.executeNoResults( query )
        
        left_reject_table.drop()
        right_reject_table.drop()
        merge_table.drop()
        
        
    def _process_required_key_dups_and_missing( self, db_context, input_table, side, allow_dups ):
        
        # Define duplicates table
        dup_table = get_temp_table( db_context )
        dup_table.create_foreign_key( input_table, True, 'fk_{}_'.format( side ) )
        dup_table.add_all( self.required_merge_keys )
        dup_table.add( FieldSpec( basic_type="TINYINT", field_name="has_dups" ) )
        dup_table.add( FieldSpec( basic_type="TINYINT", field_name="has_missing" ) )
        dup_table.add( FieldSpec( basic_type="TINYINT", field_name="has_dups_both" ) )
        db_context.executeNoResults( dup_table.definition )
        
        # Populate table
        query = _FIND_DUPLICATES_QUERY.format(
                dup_table=dup_table,
                input_table=input_table,
                foreign_key=Joiner( dup_table.foreign_keys[0] ),
                required_key=Joiner( self.required_merge_keys ),
                side=side )
        db_context.executeNoResults( query )
        
        # Define rejects table
        reject_table = get_temp_table( db_context )
        reject_table.create_foreign_key( input_table, True, 'fk_{}_'.format( side ) )
        reject_table.add_all( self.required_merge_keys )
        reject_table.add( FieldSpec( basic_type="NVARCHAR", data_length=4000, field_name="merge_report" ) )
        db_context.executeNoResults( reject_table.definition )
        
        # Move missing keys to rejects table
        self._move_rejects( dup_table, reject_table, 0, "has_missing > 0",
                           "'Missing required key on {}'".format( side ) )
        
        # If required, move duplicates to rejects table
        if not allow_dups:
            self._move_rejects( dup_table, reject_table, 0, "has_dups > 0",
                               "'Duplicate required key on {}'".format( side ) )
            
        
        return dup_table, reject_table

    def _create_merge_table(self):
        merge_table = get_temp_table( self.db_context )
        merge_table.add_all(self.required_merge_keys)
        i = 1
        left_fields = []
        right_fields = []
        for key in self.optional_merge_keys:
            lkey = MergeFieldSpec(key.left_field, None, PRIORITY_LEFT_ONLY)
            lkey.field_name = "LOptKey_" + str(i)
            merge_table.add(lkey)
            left_fields.append( lkey )
            rkey = MergeFieldSpec(None, key.right_field, PRIORITY_RIGHT_ONLY)
            rkey.field_name = "ROptKey_" + str(i)
            merge_table.add(rkey)
            right_fields.append( rkey )
            i += 1
        
        i = 1
        for keyset in self.fuzzy_merge_keys:
            if isinstance(keyset, MergeFieldSpec):
                keyset = ( keyset, None )
            if len(keyset) == 1:
                keyset = ( keyset[0], None )
            if len(keyset) != 2:
                raise ValueError("Fuzzy keys must be supplied singly or in pairs; received {}".format(len(keyset)))
            similarity_column = FieldSpec( field_name="Similarity_{}".format(i), basic_type="FLOAT" )
            merge_table.add( similarity_column )
            j = 1
            for key in keyset:
                if key is None:
                    lkey = MergeFieldSpec(None, None, PRIORITY_LEFT_ONLY)
                    lkey.field_name = "LFuzzyKey_{}_{}".format(i, j)
                    lkey.basic_type = "NVARCHAR"
                    lkey.data_length = 1
                    merge_table.add(lkey)
                    left_fields.append( lkey )
                    rkey = MergeFieldSpec(None, None, PRIORITY_RIGHT_ONLY)
                    rkey.field_name = "RFuzzyKey_{}_{}".format(i, j)
                    rkey.basic_type = "NVARCHAR"
                    rkey.data_length = 1
                    merge_table.add(rkey)
                    right_fields.append( rkey )
                else:
                    lkey = MergeFieldSpec(key.left_field, None, PRIORITY_LEFT_ONLY)
                    lkey.field_name = "LFuzzyKey_{}_{}".format(i, j)
                    merge_table.add(lkey)
                    left_fields.append( lkey )
                    rkey = MergeFieldSpec(None, key.right_field, PRIORITY_RIGHT_ONLY)
                    rkey.field_name = "RFuzzyKey_{}_{}".format(i, j)
                    merge_table.add(rkey)
                    right_fields.append( rkey )
                j += 1
            
            i += 1
        
        merge_table.create_foreign_key(self.left_input_table, True, 'fk_left_')
        merge_table.create_foreign_key(self.right_input_table, True, 'fk_right_')
        merge_table.add( FieldSpec( 'has_dups_l', 'TINYINT' ) )
        merge_table.add( FieldSpec( 'has_dups_r', 'TINYINT' ) )
        merge_table.add( FieldSpec( 'reject', 'TINYINT' ) )
        merge_table.add( FieldSpec( 'merge_report', 'NVARCHAR', 4000 ) )
        self.db_context.executeNoResults(merge_table.definition)
        return merge_table, left_fields, right_fields

    def _move_rejects( self, from_table, reject_table, key_index, condition, reason_expression ):
        self._copy_rejects( from_table, reject_table, key_index, condition, reason_expression )
        self._delete_rows( from_table, condition )
        
    def _copy_rejects( self, from_table, reject_table, key_index, condition, reason_expression ):
        query = _ADD_REJECTS_QUERY.format(
                from_table=from_table,
                reject_table=reject_table,
                keys=Joiner( from_table.foreign_keys[ key_index ], self.required_merge_keys ),
                reason_expression=reason_expression,
                condition=condition )
        self.db_context.executeNoResults( query )

    def _delete_rows( self, from_table, condition ):
        query = _DELETE_ROWS_QUERY.format( from_table=from_table, condition=condition )
        self.db_context.executeNoResults( query )
