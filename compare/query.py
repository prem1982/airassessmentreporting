'''
Created on Mar 29, 2013

@author: temp_plakshmanan
'''
from airassessmentreporting.airutility import RunContext
class Query(object):
    '''
    classdocs
    '''
    def __init__(self,run_context,tablenm1='',tablenm2='',Keyfields=[],excludefields=[],db_tag=None):
        '''
        Constructor: This method builds the SQL for a given table, keylists and and excludelists
        '''
        self.runContext = run_context
        self.dbTag   = db_tag
        print 'query self.db=', self.db
        self.conn = self.runContext.getDBContext(tag=self.dbTag)
        self.tablenm1 = tablenm1
        self.tablenm2 = tablenm2
        self.Keyfields = Keyfields
        self.excludefields = excludefields
    
    def process(self):
        """ This method will build the SELECT query that will be executed to get the result"""
        
        #query = "Select name from sys.columns where object_id = OBJECT_ID(?) order by column_id"
        query = """SELECT A.NAME FROM SYS.COLUMNS A,SYS.COLUMNS B WHERE A.NAME = B.NAME AND a.object_id = OBJECT_ID(?) AND b.object_id = OBJECT_ID(?)"""
        result  = self.conn.execute(query, (self.tablenm1,self.tablenm2))
        
        print 'query result=', result
        if result:          
            select_collist = ','.join(elem[0] for elem in result)
            match_collist  = [str(each[0]).upper() for each in result]
        print 'match_collist=', match_collist
        if self.Keyfields:  key_collist = ','.join(elem for elem in self.Keyfields)
        print 'QUERY result=', result
        
        table1_qry = "Select {select_collist} from {table_nm} order by {key_collist}".format(select_collist=select_collist,table_nm=self.tablenm1,key_collist=key_collist)
        table2_qry = "Select {select_collist} from {table_nm} order by {key_collist}".format(select_collist=select_collist,table_nm=self.tablenm2,key_collist=key_collist)
      
        return table1_qry, table2_qry, match_collist

if __name__ == '__main__':
    x = Query(RunContext("unittest"),'dbo.compare1','dbo.compare2',['ssid'] ,[],db='Share')
    x.process()