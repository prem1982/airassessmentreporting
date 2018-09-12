'''
Created on Apr 29, 2013

@author: temp_dmenes
'''
import unittest
import os.path

from airassessmentreporting.airutility import (
        clear_all, SafeExcelReader, get_table_names, n_obs )
from airassessmentreporting.testutility import SuiteContext

_XLS_FILE = r'merge_samples.xls'
_CSV_FILE = r'means_test/studentg3_n.txt'

_CSV_COLUMNS=[ x.strip() for x in """
ethnicity,migrant,schtype,ucrx_sub_name,ucrxgen,ufxx_TEST_TYPE,
Serial_number,batch_number,bcrxid_attend,bcrxnm_attend,dcrxid_attend,
dcrxnm_attend,ucrx_preid,ucrx_section_num,ucxx_data_acquisition,
ucxx_room_number,ucxx_student_UIN,ucxx_teacher_UIN,__OBS_,uprx_score_item_1,
uprx_score_item_10,uprx_score_item_11,uprx_score_item_12,uprx_score_item_13,
uprx_score_item_14,uprx_score_item_15,uprx_score_item_16,uprx_score_item_17,
uprx_score_item_18,uprx_score_item_19,uprx_score_item_2,uprx_score_item_20,
uprx_score_item_21,uprx_score_item_22,uprx_score_item_23,uprx_score_item_24,
uprx_score_item_25,uprx_score_item_26,uprx_score_item_27,uprx_score_item_28,
uprx_score_item_29,uprx_score_item_3,uprx_score_item_30,uprx_score_item_31,
uprx_score_item_32,uprx_score_item_33,uprx_score_item_34,uprx_score_item_35,
uprx_score_item_36,uprx_score_item_4,uprx_score_item_5,uprx_score_item_6,
uprx_score_item_7,uprx_score_item_8,uprx_score_item_9,ucxx_ADMIN_DATE_YR,
Grade,Grade_preid,ucxx_ADMIN_DATE_MO,ucxx_rec_change_postreport,
ucxx_rec_change_preid,ucxx_rec_change_prereport,ucxx_spec_test_ver,
ufrx_attempt,ufxx_early_return,uprx504,uprx_DNS,uprx_INV,uprx_OTHER,
uprx_READ_ALOUD,uprxiep,uprxlep,uprx_tog,obs_num,original_dcrxid,
original_dcrxnm,uprxraw,uprxscal,uprxlev,upraraw,upralev,upriraw,uprilev,
uprlraw,uprllev,uprrraw,uprrlev,uprascal,upralab,uprase,upriscal,uprilab,
uprise,uprlscal,uprllab,uprlse,uprrscal,uprrlab,uprrse,uprxlab,uprxse,
dcrxid,bcrxid,dcrxnm,bcrxnm,dcxx_county,myid,ufxx_migrant,gender,
school_irn,district_irn,student_gender,Public_flag,private_flag,scrxid,
state_irn,ethnicity_code,ufxx_accel_test,TOG_flag,conflict_accel_flag,upxxlep,
hist_score,upwx_score_item_17,hack_flag,upRx_ACCOMODATION,dummy_record_flag,
stateinclusionflag,Rteacher_ID,Rclass_id,icRxnm,Rlabel,Blank_class_flag_R,
MCitemsumR,OEitemsumR,InclusionFlagR,upRAdum1,upRLdum1,upRIdum1,upRRdum1,
upRAdum2,upRLdum2,upRIdum2,upRRdum2,upRAdum3,upRLdum3,upRIdum3,upRRdum3,
upRAdum4,upRLdum4,upRIdum4,upRRdum4,i,upRxdum1,upRxdum2,upRxdum3,upRxdum4,
upRxdum5,k,profOrHigherR,advaccR
""".split(',')]

class Test(unittest.TestCase):
    
    def setUp(self):
        self.runContext = SuiteContext('unittest')
        self.db_context = self.runContext.getDBContext( tag='unittest' )
        clear_all( self.db_context )
        self.reader = SafeExcelReader( self.runContext )
        self.reader.db_context = self.db_context
        self.testDataDir = self.runContext.tests_safe_dir
        

    def testXLS(self):
        self.reader.filename = os.path.join( self.testDataDir, _XLS_FILE )
        self.reader.sheetName = "Data1"
        self.reader.outputTable = "Data1"
        self.reader.createTable()
        
        table_spec = self.db_context.getTableSpec( 'Data1' )
        primary_key = table_spec.primary_key
        self.assertEquals( len( primary_key ), 1 )
        self.assertEquals( primary_key[0].field_name, '[import_order]' )

    def testConstructor(self):
        reader = SafeExcelReader( self.runContext, filename=os.path.join( self.testDataDir, _XLS_FILE ), sheet_name='Data1',
                                  db_context=self.runContext.getDBContext(),
                                  output_table='Temp1', get_names=True, delimiter=',', import_order='import_order' )
        reader.createTable()
        
        for name in get_table_names( self.db_context ) :
            self.assertEqual( '[temp1]', name, "Found name '{name}' instead of '[temp1]'".format(name=name) )
            
    def testIntoPython(self):
        self.reader.filename = os.path.join( self.testDataDir, _XLS_FILE )
        self.reader.sheetName = "Data1"
        rows = [ row for row in self.reader.getRows() ]
        self.assertEqual(300, len( rows ), 'Expected 300 rows, found %d' % len( rows ) )
        
    def testCSVIntoDB(self):
        self.reader.filename = os.path.join( self.testDataDir, _CSV_FILE )
        self.reader.outputTable = "CSV1"
        self.reader.scan_all = True
        self.reader.getNames = True
        self.reader.delimiter = "|"
        self.reader.skip = 0
        self.reader.range = (0,0,100,1024)
        self.reader.createTable()
        table_spec = self.db_context.getTableSpec( 'CSV1' )
        for col_name in _CSV_COLUMNS:
            self.assertTrue( col_name in table_spec, "Missing column {}".format(col_name) )
                
        self.assertEquals( n_obs( table_spec ), 100, "Wrong number of rows in imported data" )
        
    def test_xlcol(self):
        letters = map(lambda x:self.reader._xlcol(x), [0,3,25])
        self.assertTrue(letters == ['A','D','Z'], 'xlcol - translating numbers to letters - failed')
        
    def test_xlcolnumber(self):
        numbers = map(lambda x:self.reader._xlcolnumber(x), ['A','D','Z'])
        self.assertTrue(numbers == [0,3,25], 'xlcolnumber - translating letters to numbers - failed')