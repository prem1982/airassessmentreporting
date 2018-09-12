from airassessmentreporting.airutility import RunContext, TableSpec, FieldSpec 
from airassessmentreporting.airutility.dbutilities import table_exists,drop_table_if_exists,get_column_names,get_table_spec
from airassessmentreporting.airutility.formatutilities import Joiner,db_identifier_unquote
import layoutcheck

import wx
import wx.grid as gridlib

class ScrollbarFrame(wx.Frame):
    def __init__(self):
            wx.Frame.__init__(self, None, wx.ID_ANY,"Grid with Popup Menu")
            self.layout_file = {'ID':[1,5,5],'NAME':[6,12],'STATE':[13,20]}
            print self.layout_file
            self.OnInit()
        
    def OnInit(self):
        panel = wx.Panel(self, wx.ID_ANY)
        
        vbox = wx.BoxSizer(wx.VERTICAL)
        hbox1 = wx.BoxSizer(wx.HORIZONTAL)
        hbox2 = wx.BoxSizer(wx.HORIZONTAL)
        
        menuBar = wx.MenuBar()

        fileMenu = wx.Menu()
        fileMenu.Append(wx.ID_NEW, '&New')
        fileMenu.Append(wx.ID_OPEN, '&Open')
        fileMenu.Append(wx.ID_SAVE, '&Save')
        
        menuBar.Append(fileMenu, '&File')
        
        menu = self.SetMenuBar(menuBar)
        
        self.Bind(wx.EVT_MENU, self.OnButtonClicked)
        
        self.grid = gridlib.Grid(panel)
        self.grid.CreateGrid(10 ,10)
            
        self.grid.SetCellValue(0,0,'(0,0)')
        self.grid.SetCellValue(0,0,'(0,1)')
        self.grid.SetCellValue(1,0,'(1,0)')
        self.grid.SetCellValue(1,0,'(1,1)')
            
        vbox.Add(self.grid, 1, wx.EXPAND,5)
        panel.SetSizer(vbox)
    
    def OnButtonClicked(self,e ):
        print 'cliecked', e
        panel2 = wx.Panel(self,wx.ID_ANY)
        panel2.Show()
        panel2.Raise()
        panel2.Iconize(False)
        
if __name__ == '__main__':
        
    app = wx.PySimpleApp()
    frame = ScrollbarFrame()
    frame.Show()
    app.MainLoop()



