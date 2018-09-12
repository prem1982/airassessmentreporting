'''
Created on Sep 30, 2013

@author: temp_plakshmanan
'''

class DeleteTemp( object ):
    def __init__(self, dbcontext):
        print 'initialized'
        self.dbcontext = dbcontext
        query = "select name from sys.tables where name like 'temp_%' "
        result = self.dbcontext.execute ( query )
        result = [each[0].encode('ascii') for each in result]
        print result
        for each in result:
            query = "DROP TABLE {tablename}".format(tablename=each)
            print 'query =', query
            result = self.dbcontext.executeNoResults( query )
        

if __name__ == '__main__':
    from airassessmentreporting.testutility import SuiteContext, RunContext
#     RC = SuiteContext('unittest')
    RC = RunContext('sharedtest',)
    dbcontext = RC.getDBContext()
#     ttester = TTest( 'student_aggregation' , dbcontext, 'C:\CVS projects\CSSC Score Reporting\OGT Summer 2012\Code\Development\Superdata\AggregationSheet.xls',"ttest",  True )
    deletetemp = DeleteTemp(  dbcontext )
