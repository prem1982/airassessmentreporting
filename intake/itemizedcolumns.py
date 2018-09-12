'''
Created on May 28, 2013

@author: temp_plakshmanan
'''
import re
import pprint

class ItemizedColumns(object):
    """ Based on a given pattern this object will identify the list of columns to be in the items table"""
    
    def __init__(self,pattern='',columns=[]):
        """This is the initialization method and it takes the following input
        1. Matching Pattern
        2. List of Columns to identify the matching patterns"""
        self.pattern = pattern
        self.columns = columns
        self.item_variables = []
        self.likely_item_variables = []
        self.variable_names = {}
#         print 'self.pattern=', self.pattern
#         print 'self.columns=', self.columns
    
    def process(self):
        """This is the main process that controls the flow of this module"""
        #The below FOR LOOP,scans through the list of column for a given pattern and determines the maximum occurence for the pattern
        for i in range(500):
            if i <> 0:
                result = self.repeatating_columns(i)
                if result:
                    self.likely_item_variables = result
                    self.max_count = i
                else:
                    break
        
        #The below for loop,checks if a column has all the elements starting from maximum occurence until 1. 
        for each in self.likely_item_variables:
            
            for i in reversed(range(self.max_count)):
                #You can check if the 0'th occurence, so this if stmt will start checking from 1st occurence
                if i <> 0:
                    replacestring = each.replace(str(self.max_count),str(i))
                    if replacestring in self.columns:
                        stringfound = True
                    else:
                        stringnotfound = False
                        break
            #If all the occurences are found, then create a lists with all item variable -For example:variable_1,variable_2....variable_44    
            if stringfound:
                all_columns = []
                all_columns.append(each)
                for i in reversed(range(self.max_count)):
                    if i <> 0:
                        replacestring = each.replace(str(self.max_count),str(i))
                        all_columns.append(replacestring)

            self.item_variables.append(all_columns)
            
            for each in range(len(self.item_variables)):
                eachvar = self.item_variables[each][0]
                match = re.search(self.pattern.format(icnt=self.max_count).strip(),eachvar,re.IGNORECASE)
                strippos_start = match.end() - len(str(self.max_count))
                strippos_end = strippos_start + len(str(self.max_count)) 
                #We need to strip the numeric values from a column_name , the assumption is that all the MC items will have a pre-fix '_' before the occurence of the field
                name = eachvar[0:strippos_start - 1] + eachvar[strippos_end:]
                self.variable_names[eachvar] = name
#             print self.variable_names
        
        return self.item_variables, self.variable_names
        
    def repeatating_columns(self,i):
        """ This method fetches all the columns for a given matching pattern"""
        matchlist = []
        for each in self.columns:
            pattern=self.pattern.format(icnt=i)
            match = re.search(pattern.strip(),each,re.IGNORECASE)
            if match:
                matchlist.append(match.group())
                
        return matchlist
        
        
if __name__ == '__main__':
    f = open("c:/regex.txt")
    variablelist=[]
    for each in f.readlines():
        variablelist.append(each.rstrip('\n'))
    print 'variablelist=',variablelist
    ic = ItemizedColumns('[u][pcf][m]x_.*{icnt}.*',columns=variablelist)
    ic.process()