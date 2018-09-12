from airassessmentreporting.datacheck.idcheck import idcheck, idCheckSheet
from airassessmentreporting.datacheck.dataset import Dataset 
import unittest


#Tests
    #non-missing: 
        # a field is missing several, compare rows
        # a field is missing none - passes
    #unique
        # a field has several duplicate keys, compare rows
        # a field is correctly unique - passes
    #uniquelabel
        # an id, label pair is correctly populated with duplicate and unique ids, some duplicate labels are blank, check to see if they're populated correctly  - passes
        # an id, label pair has a duplicate id with unique labels - fails
        # an id with several labels is correctly populated, some blank entries should be populated - passes
        # an id with several labels has a duplicate id with unique labels - fails


class TestMeans(unittest.TestCase):
    
    def setUp(self):
        input_csv = 'testdata.csv'
        out_csv = "id_out.csv"
        input_ds = Dataset(name=input_csv,open_mode='rb')
        output_ds = Dataset(name=out_csv,open_mode='wb')
        self.reader = input_ds.dict_reader()
        self.writer = output_ds.dict_writer(["bad_variable", "bad_value", "bad_reason", "__obs", "bad_label"])
        
        query = """
            CREATE TABLE {table} (
                id INT,
                nmiss NVARCHAR(128),
                nmiss_fail NVARCHAR(128),
                unique0 NVARCHAR(128),
                unique_fail NVARCHAR(128),
                id0 NVARCHAR(128),
                label0 NVARCHAR(128),
                id1 NVARCHAR(128),
                label1 NVARCHAR(128),
                label2 NVARCHAR(128)
            )
            INSERT INTO {table} VALUES ('1', '1', '1', '1', '1', '1', 'a', '1', 'c', 'a') 
            INSERT INTO {table} VALUES ('2', '2', '2', '2', '2', '1', 'a', '1', 'c', 'b') 
            INSERT INTO {table} VALUES ('3', '3', '3', '3', '3', '2', 'c', '2', 'd', 'b') 
            INSERT INTO {table} VALUES ('4', '4', NULL,'4', '3', '2', 'c', '2', 'd', 'b') 
            INSERT INTO {table} VALUES ('5', '5', '',  '5', '5', '3', 'e', '3', 'h', 'p') 
            INSERT INTO {table} VALUES ('6', '6', '6', '6', '6', '3', 'e', '3', 'h', 'p') 
            INSERT INTO {table} VALUES ('7', '7', '7', '7', '7', '3', 'e', '3', 'i', 'p') 
        """.format(table='idTestTable')
        print query
        
        #checkFile = "C:\\development\\air_assessment_reporting\\lib\\airassessmentreporting\\datacheck\\test\\OGT_ID_Sheet.xls"
        #inTable = "idTestTable"

        idchecksheet = Dataset(name='idchecksheet.csv',open_mode='rb')
        self.checksheet_reader = idchecksheet.dict_reader()

        self.nmiss = ['lithocode'];
        self.nmiss_fail = ['nonmissing_fail'];
        self.unique = ['lithocode'];
        self.unique_fail = ['unique_fail'];
        self.uniqueLabel = {'bcrxid_attend':['dcrxnm_home']};
        self.uniqueLabel_fail = {'unique_ids':['labels1', 'labels2_fail']};

    def test_all(self):
        errs = idCheckSheet(self.checksheet_reader, self.reader, self.writer)
        #print "This is errs-1:\n" + errs
        rows = ["8", "135", "159", "284", "289", "361", "393", "447", "448", "449", "450", "990", "992", "1000"] 
        for r in rows:
            self.assertTrue(r in errs)
        
    def test_nmiss(self):
        errs = idcheck(self.reader, self.nmiss, [], {}, self.writer)
        #print "This is errs0:\n" + errs
        self.assertTrue(errs == "")

    def test_nmiss_fail(self):
        errs = idcheck(self.reader, self.nmiss_fail, [], {}, self.writer)
        #print "This is errs1\n" + errs
        self.assertTrue("992" in errs and "393" in errs)

    def test_unique(self):
        errs = idcheck(self.reader, [], self.unique, {}, self.writer)
        #print "This is errs2\n" + errs
        self.assertTrue(errs == "")

    def test_unique_fail(self):
        errs = idcheck(self.reader, [], self.unique_fail, {}, self.writer)
        #print "This is errs3\n" + errs
        rows = ["135", "284", "361", "447", "448", "449", "450", "1000"] 
        for r in rows:
            self.assertTrue(r in errs)

    def test_uniqueLabel(self):
        errs = idcheck(self.reader, [], [], self.uniqueLabel, self.writer)
        #print "This is errs4\n" + errs
        self.assertTrue(errs == "", errs)

    def test_uniqueLabel_fail(self):
        errs = idcheck(self.reader, [], [], self.uniqueLabel_fail, self.writer)
        #print "This is errs5\n" + errs
        rows = ["8", "990", "289", "159"] 
        for r in rows:
            self.assertTrue(r in errs)

if __name__ == "__main__":
    unittest.main()

