from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote

import wx
import layoutcheck
import wx.grid as gridlib

import wx

class ScrollbarFrame(wx.Frame):
    def __init__(self, title, runcontext, dbcontext, layoutfile):
            wx.Frame.__init__(self, None, wx.ID_ANY,"Grid with Popup Menu")
            self.runcontext = runcontext
            self.dbcontext = dbcontext
            self.layout = layoutfile
            self.layout_file = {'ID':[1,5,5],'NAME':[6,12],'STATE':[13,20]}
            print self.layout_file
            self.OnInit()
        
    def OnInit(self):
        LC = layoutcheck.LayoutCheck(runcontext=self.runcontext, dbcontext=self.dbcontext,
                                                layoutfile=self.layout)
  
  
        self.layoutdict, self.maxmindict, self.recodingsdict = LC.process()
        print self.layoutdict
        panel = wx.Panel(self, wx.ID_ANY)
        
        vbox = wx.BoxSizer(wx.VERTICAL)
        hbox1 = wx.BoxSizer(wx.HORIZONTAL)
        hbox2 = wx.BoxSizer(wx.HORIZONTAL)
        
        menuBar = wx.MenuBar()
 
        fileMenu = wx.Menu()
        fileMenu.Append(wx.ID_NEW, '&New')
        fileMenu.Append(wx.ID_OPEN, '&Open')
        fileMenu.Append(wx.ID_SAVE, '&Save')
        fileMenu.Append(wx.ID_FIND, '&Find')
         
         
        menuBar.Append(fileMenu, '&File')
         
        self.SetMenuBar(menuBar)
        self.Bind(wx.EVT_MENU, self.OnButtonClicked,id=wx.ID_FIND)
        self.grid = gridlib.Grid(panel)
        self.grid.CreateGrid(len(self.layoutdict) ,1000)
        print 'len(self.layoutdict)=',len(self.layoutdict)
#         self.grid.CreateGrid(10,10)
        for eachline in open('C:\SAS\OGT\Input\input-1.txt'):
            i = 0
            for eachcol in self.layoutdict:
                print eachcol
                self.grid.SetCellValue(i, 0 ,eachcol)
                i += 1
                print 'i=',i
                
#         self.grid.SetCellValue(0,0,'Hi')
#         self.grid.SetCellValue(1,0,'Hiii')
            
        print self.grid.NumberCols
        

        vbox.Add(self.grid, 1, wx.EXPAND,5)
        panel.SetSizer(vbox)
    
    def OnButtonClicked(self, e):
        print 'Button clicked'    
if __name__ == '__main__':
    RC = RunContext('unittest')
    dbcontext = RC.getDBContext()
        
    app = wx.PySimpleApp()
    frame = ScrollbarFrame(title='LayoutMapper',
            runcontext = RC, dbcontext = dbcontext, 
            layoutfile = 'C:\CVS Projects\CSSC Score Reporting\OGT Spring 2012\Input Layout\OGT_SP12_Op_DataLayout_IntakeLayout.xls')
    frame.Show()
    app.MainLoop()



