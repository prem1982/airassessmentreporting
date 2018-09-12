'''
Created on Jun 18, 2013

@author: temp_plakshmanan
'''
i = 1
f0 = open('C:\SAS\OGT\Input\original-record1.txt','w')
# f1 = open('C:\SAS\OGT\Input\original-part1.txt','w')
# f2 = open('C:\SAS\OGT\Input\original-part2.txt','w')
# f3 = open('C:\SAS\OGT\Input\original-part3.txt','w')
# f4 = open('C:\SAS\OGT\Input\original-part4.txt','w')
 
for each in open('H:\\share\\Ohio Graduation Tests\\Technical\\2012 October\\ScoreReports\\TextFileFromDRC\\536215_2012OhioOGTFall_Regular.txt','r'):
#     print each
    if i == 9165:
        print 'len(each)=', len(each)
        f0.write(each)
#     if i >= 1 and i <= 10000:
#         f1.write(each)
#     elif i > 50000 and i <= 100000:
#         f2.write(each)
#     elif i > 100000 and i <= 150000:
#         f3.write(each)
#     else:
#         f4.write(each)
    i += 1
 
print 'DOne'

# class Fileprocessor( object ):
#     def __init__(self, filename):
#         self.filename = filename
#     
#     def process(self):
#         for line in open(self.filename):
#             print 'lower_case_line=', line
#             upper_case_line = self._convert_to_upper_case(line)
#             print 'upper_case_line=', upper_case_line
# 
#     def _convert_to_upper_case(self, line):
#         return line.upper()
# 
# if __name__ == '__main__':
#     fp = Fileprocessor(filename="C:/input-1.txt")
#     fp.process()