'''
Created on May 22, 2013

@author: temp_dmenes
'''

import blist

from airassessmentreporting.airutility import ( get_table_spec, table_exists,
        drop_table_if_exists, Joiner, get_temp_table, FieldSpec )

__all__ = ['IDGenerator']

_TEACHER_ID_FORMAT_STRING = 'AIR{date}OATI{grade:02d}{i:07d}'
_CLASS_ID_FORMAT_STRING = 'AIR{date}OATT{subject}{grade:02d}{i:06d}'
_SELECT_DISTINCT = "SELECT DISTINCT {fields} FROM {table}"
_INSERT = "INSERT INTO {table}( {fields} ) VALUES( {fields:itemfmt='?'} )"
_COPY = "INSERT INTO {ds_out}( {fields} ) SELECT {fields} FROM {ds_in}"
_ADD_ID_AND_LABELS = """
UPDATE {ds_out}
SET {teacher_id_var}={teacher_table}.[teacher_id],
    {teacher_label_var}={teacher_table}.[teacher_label],
    {class_id_var}={class_table}.[class_id],
    {class_label_var}={class_table}.[class_label]
FROM (
    ( {ds_out}
        LEFT JOIN {teacher_table}
        ON (
            ({ds_out}.{grade_var}={teacher_table}.{grade_var})
            AND (ISNULL({ds_out}.{district_var},'')={teacher_table}.{district_var})
            AND (ISNULL({ds_out}.{school_var},'')={teacher_table}.{school_var})
            AND (ISNULL({ds_out}.{teacher_var},'')={teacher_table}.[teacher])
        )
    )
    LEFT JOIN {class_table}
    ON (
            ({ds_out}.{grade_var}={class_table}.{grade_var})
        AND (ISNULL({ds_out}.{district_var},'')={class_table}.{district_var})
        AND (ISNULL({ds_out}.{school_var},'')={class_table}.{school_var})
        AND (ISNULL({ds_out}.{teacher_var},'')={class_table}.[teacher])
        AND (ISNULL({ds_out}.{class_var},'')={class_table}.[class])
        AND (ISNULL({ds_out}.{section_var},'')={class_table}.[section])
        AND ({class_table}.[subject]='{subject_char}')
    )
)
"""

class IDGenerator( object ):
    def __init__( self, ds_in=None, ds_out=None, db_context=None, grade_var=None,
                  district_var=None, school_var=None, subject_char_lst=[],
                  teacher_var_lst=[], teacher_label_lst=[], teacher_id_lst=[],
                  class_var_lst=[], section_var_lst=[], class_label_lst=[],
                  class_id_lst=[], test_date='1006', err_var_name='errvar' ):
        '''
        Generate unique identifiers and labels for classes.
        
        A class is defined as a unique combination of teacher, subject and
        student cohort.
        '''
        
        self.ds_in = ds_in
        self.ds_out = ds_out
        self.db_context = db_context
        self.grade_var = grade_var
        self.district_var = district_var
        self.school_var = school_var
        self.subject_char_lst = subject_char_lst
        self.teacher_var_lst = teacher_var_lst
        self.teacher_label_lst = teacher_label_lst
        self.teacher_id_lst = teacher_id_lst
        self.class_var_lst = class_var_lst
        self.section_var_lst = section_var_lst
        self.class_label_lst = class_label_lst
        self.class_id_lst = class_id_lst
        self.test_date = test_date
        self.err_var_name = err_var_name
        
    def validate(self):
        # Make sure that the input and output tables are in the form of
        # TableSpec objects
        self.ds_in = get_table_spec( self.ds_in, self.db_context )
        self.db_context = self.ds_in.db_context
        self.run_context = self.db_context.runContext
        
        self.logger = self.run_context.get_logger( "IDGenerator" )
        self.logger.debug( "Validating IDGenerator parameters" )

        # Make sure that ds_in exists
        # (SAS 30)
        if not table_exists( self.ds_in ):
            raise ValueError( 'Input table {} does not exist'.format( self.ds_in ) )
        self.ds_in.populate_from_connection()
        
        # Remove the output dataset if it exists
        drop_table_if_exists( self.ds_out )
        self.ds_out = self.db_context.getTableSpec( self.ds_out )
        
        # Confirm existence of singleton variables
        # (SAS 32-49)
        if self.grade_var not in self.ds_in:
            raise ValueError( "Grade variable {} not found in dataset {}"
                    .format( self.grade_var, self.ds_in ) )
        if self.district_var not in self.ds_in:
            raise ValueError( "District variable {} not found in dataset {}"
                    .format( self.grade_var, self.ds_in ) )
        if self.school_var not in self.ds_in:
            raise ValueError( "School variable {} not found in dataset {}"
                    .format( self.grade_var, self.ds_in ) )

        # Confirm that variable list lengths conform
        # (SAS 50-99)
        n = len( self.teacher_var_lst )
        if n == 0:
            raise ValueError( "No teacher variables specified" )
        if len( self.class_var_lst ) != n:
            raise ValueError( "Number of class variables {} must match number of teacher variables {}"
                              .format( len( self.class_var_lst, n ) ) )
        if len( self.section_var_lst ) != n:
            raise ValueError( "Number of section variables {} must match number of teacher variables {}"
                              .format( len( self.section_var_lst, n ) ) )
        if len( self.subject_char_lst ) != n:
            raise ValueError( "Number of subject characters {} must match number of teacher variables {}"
                              .format( len( self.subject_char_lst, n ) ) )
        if len( self.class_label_lst ) != n:
            raise ValueError( "Number of class label variables {} must match number of teacher variables {}"
                              .format( len( self.class_label_lst, n ) ) )
        if len( self.class_id_lst ) != n:
            raise ValueError( "Number of class id variables {} must match number of teacher variables {}"
                              .format( len( self.class_id_lst, n ) ) )
        if len( self.teacher_label_lst ) != n:
            raise ValueError( "Number of teacher label variables {} must match number of teacher variables {}"
                              .format( len( self.class_var_lst, n ) ) )
        if len( self.teacher_id_lst ) != n:
            raise ValueError( "Number of class variables {} must match number of teacher variables {}"
                              .format( len( self.tacher_id_lst, n ) ) )
        
        # Confirm that teacher, class and section variables exist in dataset
        # (SAS 72-88)
        for i in range( n ):
            if self.teacher_var_lst[ i ] not in self.ds_in:
                raise ValueError( "Teacher variable {} not found in input dataset"
                                  .format( self.teacher_var_lst[i] ) )
            if self.class_var_lst[ i ] not in self.ds_in:
                raise ValueError( "Class variable {} not found in input dataset"
                                  .format( self.class_var_lst[i] ) )
            if self.section_var_lst[ i ] not in self.ds_in:
                raise ValueError( "Section variable {} not found in input dataset"
                                  .format( self.section_var_lst[i] ) )
    def execute( self ):
        self.validate()
        
        # First cleaning pass, uniquify, and sort
        # SAS 102-121, 142-144, 174-176
        query = _SELECT_DISTINCT.format( table = self.ds_in,
                fields = Joiner( ( self.grade_var, self.district_var, self.school_var ),
                                 self.teacher_var_lst, self.class_var_lst, self.section_var_lst ) )
        teacher_keys = blist.sortedset()
        class_keys = blist.sortedset()
        n = len( self.subject_char_lst )
        for row in self.db_context.execute( query ):
            grade = int( row[0] )
            district = str( row[1] )
            school = str( row[2] )

            i = 0
            for subject_char in self.subject_char_lst:
                teacher = _basic_clean( row[ 3 + i ] )
                teacher_keys.add( ( grade, district, school, teacher ) )
                cls = _basic_clean( row[ 3 + n + i ] )
                section = _basic_clean( row[ 3 + n + n + i ] )
                class_keys.add( ( grade, district, school, teacher, cls, section, subject_char  ) )
                i += 1
        
        # Create teacher labels and IDs
        teacher_items = {}
        i = 1
        teacher_label_width = 1
        for teacher_key in teacher_keys:
            # SAS 123-127; 146-173; 
            grade, district, school, teacher = teacher_key
            if teacher:
                teacher_label = teacher
                teacher_id = _TEACHER_ID_FORMAT_STRING.format( date=self.test_date, grade=grade, i=i )
                i += 1
            else:
                teacher_label = 'NO NAME'
                teacher_id = None
            teacher_items[ teacher_key ] = _item( teacher_label, teacher_id )
            teacher_label_width = max( teacher_label_width, len( teacher_label ) )
            
        # Send teacher data back to SQL Server
        with get_temp_table( self.db_context ) as teacher_table, get_temp_table( self.db_context ) as class_table:
            teacher_table.add_all( ( self.ds_in[ self.grade_var ],
                                     self.ds_in[ self.district_var ],
                                     self.ds_in[ self.school_var ] ) )
            
            width = reduce( max, [ self.ds_in[ f ].data_length for f in self.teacher_var_lst ] )
            teacher_field = FieldSpec( 'teacher', 'NVARCHAR', width )
            teacher_table.add( teacher_field )
            
            teacher_label_field = FieldSpec( 'teacher_label', 'NVARCHAR', teacher_label_width )
            teacher_table.add( teacher_label_field )
            
            teacher_id_field = FieldSpec( 'teacher_id', 'NVARCHAR', 20 )
            teacher_table.add( teacher_id_field )
            
            teacher_table.primary_key.append( teacher_table[ self.grade_var ] )
            teacher_table.primary_key.append( teacher_table[ self.district_var ] )
            teacher_table.primary_key.append( teacher_table[ self.school_var ] )
            teacher_table.primary_key.append( teacher_field )
            self.db_context.executeNoResults( teacher_table.definition )
            
            query = _INSERT.format( table=teacher_table, fields=Joiner( teacher_table ) )
            for teacher_key in teacher_keys:
                # SAS 123-127; 146-173; 
                grade, district, school, teacher = teacher_key
                if grade is None or district is None or school is None or teacher is None:
                    continue
                item = teacher_items[ teacher_key ]
                params = grade, district, school, teacher, item.label, item.id
                self.db_context.executeNoResults( query, params )
            del teacher_items
            del teacher_keys
            
            # Create class labels and IDs
            class_items = {}
            i = 1
            class_label_width = 1
            for class_key in class_keys:
                # SAS 128-140; 174-216
                ( grade, district, school, teacher, cls, section, subject_char ) = class_key
                parts = []
                has_parts = False
                if teacher:
                    has_parts = True
                    parts.append( teacher )
                else:
                    parts.append( 'NO NAME' )
                if cls:
                    has_parts = True
                    parts.append( cls )
                if section:
                    has_parts = True
                    parts.append( section )
                if has_parts:
                    class_label = ' - '.join( parts )
                    class_id = _CLASS_ID_FORMAT_STRING.format( date=self.test_date, subject=subject_char, grade=grade, i=i )
                    i += 1
                else:
                    class_label = 'CLASS ASSIGNMENT UNKNOWN'
                    class_id = None
                class_items[ class_key ] = _item( class_label, class_id )
                class_label_width = max( class_label_width, len( class_label ))
                    
            # Send class data back to SQL Server
            class_table.add_all( ( self.ds_in[ self.grade_var ],
                                   self.ds_in[ self.district_var ],
                                   self.ds_in[ self.school_var ] ) )
            
            class_table.add( teacher_field.clone() )
            
            width = reduce( max, [ self.ds_in[ f ].data_length for f in self.class_var_lst ] )
            class_field = FieldSpec( 'class', 'NVARCHAR', width )
            class_table.add( class_field )
            
            width = reduce( max, [ self.ds_in[ f ].data_length for f in self.section_var_lst ] )
            section_field = FieldSpec( 'section', 'NVARCHAR', width )
            class_table.add( section_field )
            
            subject_field = FieldSpec( 'subject', 'NVARCHAR', 1 )
            class_table.add( subject_field )
            
            class_label_field = FieldSpec( 'class_label', 'NVARCHAR', class_label_width )
            class_table.add( class_label_field )
    
            class_id_field = FieldSpec( 'class_id', 'NVARCHAR', 20 )
            class_table.add( class_id_field )
            
            class_table.primary_key.append( class_table[ self.grade_var ] )
            class_table.primary_key.append( class_table[ self.district_var ] )
            class_table.primary_key.append( class_table[ self.school_var ] )
            class_table.primary_key.append( class_table[ 'teacher' ] )
            class_table.primary_key.append( class_field )
            class_table.primary_key.append( section_field )
            class_table.primary_key.append( subject_field )
            self.db_context.executeNoResults( class_table.definition )
            
            query = _INSERT.format( table=class_table, fields=Joiner( class_table ) )
            for class_key in class_keys:
                grade, district, school, teacher, cls, section, subject_char = class_key
                if ( grade is None or district is None or school is None or teacher is None
                     or cls is None or section is None or subject_char is None ):
                    continue
                item = class_items[ class_key ]
                params = grade, district, school, teacher, cls, section, subject_char, item.label, item.id
                self.db_context.executeNoResults( query, params )
            del class_items
            del class_keys
            
            # Create output table
            self.ds_out.add_all( self.ds_in )
            for var_name in self.teacher_label_lst:
                self.ds_out.add( FieldSpec( var_name, 'NVARCHAR', teacher_label_width ) )
            for var_name in self.teacher_id_lst:
                self.ds_out.add( FieldSpec( var_name, 'NVARCHAR', 20 ) )
            for var_name in self.class_label_lst:
                self.ds_out.add( FieldSpec( var_name, 'NVARCHAR', class_label_width ) )
            for var_name in self.class_id_lst:
                self.ds_out.add( FieldSpec( var_name, 'NVARCHAR', 20 ) )
            self.db_context.executeNoResults( self.ds_out.definition )
            
            # Copy data from ds_in to ds_out
            query = _COPY.format( ds_out=self.ds_out, ds_in=self.ds_in, fields=Joiner( self.ds_in ) )
            self.db_context.executeNoResults( query )
            
            # Insert the label and ID fields for each subject
            for subject_char, teacher_var, teacher_label_var, teacher_id_var, class_var, section_var, class_label_var, class_id_var \
                  in zip( self.subject_char_lst,
                      self.teacher_var_lst,
                      self.teacher_label_lst,
                      self.teacher_id_lst,
                      self.class_var_lst,
                      self.section_var_lst,
                      self.class_label_lst,
                      self.class_id_lst ):
                
                query = _ADD_ID_AND_LABELS.format( ds_out=self.ds_out,
                                                   teacher_table=teacher_table,
                                                   class_table=class_table,
                                                   grade_var=self.grade_var,
                                                   district_var=self.district_var,
                                                   school_var=self.school_var,
                                                   subject_char=subject_char,
                                                   teacher_var=teacher_var,
                                                   teacher_label_var=teacher_label_var,
                                                   teacher_id_var=teacher_id_var,
                                                   class_var=class_var,
                                                   section_var=section_var,
                                                   class_label_var=class_label_var,
                                                   class_id_var=class_id_var )
                self.db_context.executeNoResults( query )
            
                
def _basic_clean( s ):
    return '' if s is None else ' '.join( str(s).replace( '- -', '-').split() )

class _item( object ):
    __slots__ = [ 'label', 'id' ]
    def __init__( self, label, ID ):
        self.label = label
        self.id = ID

        