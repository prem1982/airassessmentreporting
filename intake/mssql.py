'''
Created on Jun 24, 2013

@author: temp_plakshmanan
'''

import pymssql


import pymssql
conn = pymssql.connect(host=r'DC1LAKSHMANANP\SQLEXPRESS', user=r'AIR\temp_plakshmanan', password=r'Ndtv2160', database=r'CSSC')  # @UndefinedVariable
print conn
cur = conn.cursor()
cur1 = conn.cursor()
cur.execute(r'SELECT * FROM Employee1')

cur1.execute(r'SELECT * FROM Employee1')
print 'Cursor- starting=',cur.fetchall()
for item in cur.fetchall():
    print 'item=', item

cur.close()
    
print 'Cursor-1 starting=',cur1.fetchall()        
for item1 in cur1.fetchall():
        print 'item1=', item1
    

cur.close()
conn.close()



