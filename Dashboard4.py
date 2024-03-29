#! /usr/bin/env python
#! /usr/bin/arch -x86_64 /usr/bin/env python

from logging import error
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
import datetime as dt
from plotly.subplots import make_subplots
from dash.dependencies import Input, Output, State
from pprint import pprint
import waitress
import json
import re
import argparse
import os
import zlib
import math
import textwrap
from ordered_set import OrderedSet
import natsort 
from zipfile import ZipFile
from bs4 import BeautifulSoup  # you also need to install "lxml" for the XML parser
from tabulate import tabulate
from collections import OrderedDict
import socket
from contextlib import closing
import sqlite3

from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype

def find_free_port():

	DebugMsg2("Inside def find_free_port():")

	DebugMsg2("Inside def find_free_port():")
	with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
		s.bind(('', 0))
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		return s.getsockname()[1]
	
	
print("############################################")
print("############################################")
print("############################################")
print("############################################")
debug=False

def DebugMsg(msg1,msg2=None,printmsg=True):
	if debug or printmsg:
		print(dt.datetime.now().strftime("%c"),end=" " )
		print(msg1,end=" " )
		if msg2 is not None:
			print(msg2)
		print("",flush=True)

def DebugMsg2(msg1,msg2=None,printmsg=True):
	DebugMsg(msg1,msg2,printmsg)

def DebugMsg3(msg1,msg2=None,printmsg=True):
	DebugMsg(msg1,msg2,printmsg)

def Info(msg1,msg2=None,printmsg=True):
	DebugMsg(msg1,msg2,printmsg)

def get_xlsx_sheet_names(xlsx_file,return_As_dropdown_options=False):

	DebugMsg2("Inside def get_xlsx_sheet_names(xlsx_file,return_As_dropdown_options=False):")
	with ZipFile(xlsx_file) as zipped_file:
		summary = zipped_file.open(r'xl/workbook.xml').read()
	soup = BeautifulSoup(summary, "html.parser")
	sheets = [sheet.get("name") for sheet in soup.find_all("sheet")]
	if return_As_dropdown_options:
		doptions=[]
		for sheet in sheets:
			doptions.append({"label": sheet, "value": sheet})
		return doptions
	else:
		return sheets

def get_sqlite_table_names(sqlite_db_path,return_As_dropdown_options=False):
	tables=[]
	try:
		# Making a connection between sqlite3
		# database and Python Program
		sqliteConnection = sqlite3.connect(sqlite_db_path)
		# Getting all tables from sqlite_master
		sql_query = """SELECT name FROM sqlite_master
		WHERE type='table';"""

		# Creating cursor object using connection object
		cursor = sqliteConnection.cursor()
		
		# executing our sql query
		cursor.execute(sql_query)
		
		# printing all tables list
		tables=cursor.fetchall()
	
	except sqlite3.Error as error:
		print("Failed to execute the above query", error)
		
	finally:
	
		# Inside Finally Block, If connection is
		# open, we need to close it
		if sqliteConnection:
			
			# using close() method, we will close
			# the connection
			sqliteConnection.close()
			
			# After closing connection object, we
			# will print "the sqlite connection is
			# closed"
			print("the sqlite connection is closed")

	if return_As_dropdown_options:
		doptions=[]
		for table in tables:
			doptions.append({"label": table[0], "value": table[0]})
		return doptions
	return tables

def sqlite_table_to_df(sqlite_db_path, table_name):
	sqliteConnection = sqlite3.connect(sqlite_db_path)
	cnx = sqlite3.connect("/arm/projectscratch/pipd/umc/l22ulpull/mem/sram_sp_hde_hvtullp_mvt/workspace/da/da_pdk_70_new/data/eos/da/database/eosDb")
	df = pd.read_sql_query("SELECT * FROM %s " % table_name, cnx)
	sqliteConnection.close()
	return df


def read_rdb_in_df(rdb_file):
    print ("Reading "  +rdb_file)
    keep_lines=list()
    lineno=0
    valid_lineno=0
    head=[]
    with open(rdb_file) as f:
        for line in f:
            lineno=lineno+1
            if re.search("^\s*#",line) or re.search("^\s*$",line):
                pass
            else:
                line = line.strip('\n')
                valid_lineno=valid_lineno+1
                if valid_lineno==1:
                    head=(["__LINENO"] + line.split("\t"))
                elif valid_lineno ==2:
                    pass
                else:
                    keep_lines.append([lineno] + line.split("\t"))
    df=pd.DataFrame(keep_lines,columns=head)
    df.index=df['__LINENO']
    df=df.drop(['__LINENO'],axis=1)
    return df

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options



class Dashboard:
	def __init__(self,  datafile,isxlsx=False,sheetname=None,skiprows=0,replace_with_nan=None, DashboardMode=False):
		DebugMsg2("Inside def __init__(self,  datafile,isxlsx=False,sheetname=None,skiprows=0,replace_with_nan=None, DashboardMode=False):")
		self.RecentFilesListPath="./recent"
		self.DashboardMode=DashboardMode
		self.ComparisonFunctionalityPlaceholder()
		df_index=self.default_df_index
		if datafile is not None:
			self.setDataFile(datafile,isxlsx,sheetname,skiprows,replace_with_nan,df_index)
		self.createDashboard(df_index,self.DashboardMode)
		self.app = dash.Dash(external_scripts=["./dashboard.css"])
#        self.app = dash.Dash()
		self.app.layout = html.Div(self.layout())

	def reset_df_index(self,idx):

		DebugMsg2("Inside def reset_df_index(self,idx):")
		self.df[idx]=None
		self.filtered_df[idx]=None
		self.plot_df[idx]=None
		self.DataFile[idx]=None

	def ComparisonFunctionalityPlaceholder(self):

		DebugMsg2("Inside def ComparisonFunctionalityPlaceholder(self):")
		self.df_indexes=["1","2"]
		self.current_df_index="1"
		self.default_df_index="1"

		self.df=dict()
		self.filtered_df=dict()
		self.plot_df=dict()
		self.DataFile=dict()
		for idx in self.df_indexes:
			self.df[idx]=None
			self.filtered_df[idx]=None
			self.plot_df[idx]=None
			self.DataFile[idx]=None

	def createDashboard(self, df_index, DashboardMode=False):

		DebugMsg2("Inside def createDashboard(self, df_index, DashboardMode=False):")
		self.init_constants()
#        self.df=None
		self.reset=False
		self.newXAxisColName = "#"
		self.DatatoDownload = None
		self.ControlMode=not DashboardMode


		self.GlobalParams={}
		self.GlobalParams['available_legends']=OrderedSet()
		self.GlobalParams['SecAxisTitles']=OrderedSet()
		self.GlobalParams['PrimaryAxisTitles']=OrderedSet()
		self.GlobalParams['LegendTitle']="Legend"
		self.GlobalParams['Datatable_columns']=[]
		self.GlobalParams['ColumnsUpdated']=False
		self.GlobalParams['NewColumns']=[]

		self.GlobalParams['TableCurrentPage']=1
		self.GlobalParams['TablePageSize']=20
		self.GlobalParams['TableDfIndex']=self.default_df_index

		tmp=None
		if self.DataFile[df_index] is not None :
			tmp=self.loadMetadata(df_index,"LastGraph")
		if tmp is not None:
			self.GraphParams = tmp
			self.update_aggregate()
		else:
			self.initialize_GraphParams()

		self.update_aggregate()
		self.groups = [[json.dumps(self.GraphParams)]]

		self.DF_read_copy = dict()

		self.readFileInitDash(df_index)
		self.updateGraphList(df_index)

		self.filtered_df[df_index] = self.df[df_index].copy()
		self.plot_df[df_index]=self.filtered_df[df_index]
		self.table_df=self.filtered_df[df_index]
		self.initialize_figs()
		#self.update_graph()

	def setDataFile(self,datafile,fileFormat,fileLoadOptions,skiprows,replace_with_nan,df_index):
		DebugMsg2("Inside def setDataFile(self,datafile,isxlsx,sheetname,skiprows,replace_with_nan,df_index):")
		DebugMsg2("fileFormat=",fileFormat)
		if datafile is not None:
			datafile1=os.path.abspath(datafile)
			self.DataFile[df_index] = {'Path': datafile1,
							'FileFormat':fileFormat,
							'FileLoadOptions': fileLoadOptions,
							'SkipRows': skiprows,
							'ReplaceWithNan' : replace_with_nan, 
							'LastModified' : 0 ,
							'MetadataFile' : datafile + ".dashjsondata" , 
							}
			self.update_df(self.DataFile[df_index],df_index)
			self.updateRecentFiles(df_index)
		else:
			self.DataFile[df_index]=None
			self.reset_df_index(df_index)
			self.updateRecentFiles(df_index)


	def initialize_GraphParams(self):


		DebugMsg2("Inside def initialize_GraphParams(self):")
		self.GraphParams["GraphId"] = ""
		self.GraphParams["Name"] = ""
		self.GraphParams["Xaxis"] = []
		self.GraphParams["GraphType"] = "Scatter"
		self.GraphParams["Primary_Yaxis"] = []
		self.GraphParams["Primary_Legends"] = []
		self.GraphParams["Aggregate_Func"] = []
		self.GraphParams["Secondary_Legends"] = []
		self.GraphParams["Aggregate"] = []
		self.GraphParams["Scatter_Labels"] = []
		self.GraphParams["SortBy"] = []
		self.GraphParams["Filters"] = ""
		self.GraphParams["FilterAgregatedData"] = ""
		self.GraphParams["PreviousOperations"] = []
		self.GraphParams["ShowPreAggregatedData"] = ['Yes']
		self.GraphParams["PlotIndexes"] = [self.current_df_index]
	
	def loadMetadata(self,df_index,header=None):
	
		DebugMsg2("Inside def loadMetadata(self,df_index,header=None):")
		jsondata=None
		name= self.getDataFileName(self.DataFile[df_index]) 
		if self.DataFile[df_index] is not None and os.path.exists(self.DataFile[df_index]['MetadataFile']):
			with open(self.DataFile[df_index]['MetadataFile']) as json_file:
				jsondata=json.load(json_file)  
				if header is None:
					return jsondata ## just to update data
				if name not in jsondata:
					jsondata=None
				else:
					jsondata=jsondata[name]
		if jsondata is not None and header is not None: 
			 
			if header in jsondata:
				jsondata=jsondata[header]
			else:
				jsondata=None
		DebugMsg2("Done def loadMetadata(self,df_index,header=None):")
		return jsondata

	def updateMetadata(self,header,data,df_index):

		DebugMsg2("Inside def updateMetadata(self,header,data,df_index):")
		jsondata=self.loadMetadata(df_index)
		name= self.getDataFileName(self.DataFile[df_index]) 
		if jsondata is None:
			jsondata=dict()
		if name not in jsondata:
			jsondata[name]=dict()
			
		jsondata[name][header]=data
		with open(self.DataFile[df_index]['MetadataFile'], "w") as outfile:
			json.dump(jsondata,outfile)
		


	def updateGraphList(self,df_index):
		


		DebugMsg2("Inside def updateGraphList(self,df_index):")
		if self.DataFile[df_index] is not None: 
			self.SavedGraphList= self.getGraphList(df_index,'SavedGraphs')
			self.HistoricalGraphList= self.getGraphList(df_index,'HistoricalGraphs')
		else:
			self.SavedGraphList= dict()
			self.HistoricalGraphList= dict()

	def getGraphList(self,df_index,type):

		DebugMsg2("Inside def getGraphList(self,df_index,type):")           
		# type can be SavedGraphs/HistoricalGraphs
		x=self.loadMetadata(df_index,type)
		if x is None:
			return dict()
		else:
			return x


	def set_Graphid(self):


		DebugMsg2("Inside def set_Graphid(self):")           
		x=self.GraphParams.copy()
		x['GraphId']=""
		x['Name']=""
		self.GraphParams['GraphId']=zlib.adler32(bytes(json.dumps(x),'UTF-8'))
		return id

	def update_dtypes(self,df1):

		DebugMsg2("Inside def update_dtypes(self,df1):")           
		for col in self.dtypes:
			if col in df1.columns:
				if self.dtypes[col] == 'datetime':
					df1[col]=pd.to_datetime(df1[col])
				else:
					df1[col]=df1[col].astype(self.dtypes[col])
		return df1

	def get_dypes(self,cols):

		DebugMsg2("Inside def get_dypes(self,cols):")
		if cols is None:
			dtypes=self.df[self.default_df_index].dtypes.to_frame('dtypes')['dtypes'].astype(str).to_dict()
		else:
			dtypes=self.df[self.default_df_index][cols].dtypes.to_frame('dtypes')['dtypes'].astype(str).to_dict()
		return dtypes


	def update_dtype(self,cols,dtype,custom_datetime_fmt):


		DebugMsg2("Inside def update_dtype(self,cols,dtype,custom_datetime_fmt):")
		update_done=False
		for col in cols:
			for idx in self.df_indexes:
				if self.df[idx] is not None:
					if dtype == 'datetime_custom_format':
						self.df[idx][col]=pd.to_datetime(self.df[idx][col],format=custom_datetime_fmt,errors='coerce')
					else:
						self.df[idx][col]=self.df[idx][col].astype(self.AvailableDataTypes[dtype])
					update_done=True
		if update_done:
			dtypes=self.df[self.default_df_index].dtypes.to_frame('dtypes').reset_index().set_index('index')['dtypes'].astype(str).to_dict()
			self.updateMetadata("ColumnsDataTypes",dtypes,self.default_df_index)



	def init_constants(self):



		DebugMsg2("Inside def init_constants(self):")
		self.dtypes_old= {
			'MasterJobId' : str ,
			'jobid' : str ,
			'jobindex' : float ,
			'token' : str ,
			'cluster' : str ,
			'mem_bucketed' : float ,
			'step' : str ,
			'submit_time' : 'datetime' ,
			'mem_before_bucket' : str ,
			'lineno' : float ,
			'mem_selection_reason' : str ,
			'status' : str ,
			'completion_time' : 'datetime' ,
			'EosFlowVersion' : str ,
			'PegasusVersion' : str ,
			'Sandboxpath' : str ,
			'RepeatabilityMode' : bool ,
			'MasterStartTime' : 'datetime' ,
			'LastRecordedTime' : 'datetime' ,
			'status_bjobs' : str ,
			'start_time' : 'datetime',
			'CR_ProjectID' : str ,
			'CR_TaskID' : str ,
			'CR_JobId' : str ,
			'CPU_Architecture' : str ,
			'Grafana_Tag' : str ,
			'Project_Task_Tag' : str ,
			'CRRunningStartTime' : 'datetime',
			'new_status' : str ,
			'PATH' : str ,
			'CORNERVT' : str ,
			'PACKAGE' : str ,
			'INSTANCE' : str ,
			'ARC' : str ,
			'VT' : str ,
			'CORNER' : str ,
			'EXTRACTION' : str ,
			'SIM_CFG' : str ,
			'TOKEN_esti' : str ,
			'MEM_REQ_SIZE_esti' : float,
			'MAX_MEM_esti' : float,
			'PATH_esti' : str ,
			'delta_SIM_vs_esti' : float,
			'%age_SIM_vs_ESTI' : float,
			'eosFlow' : str ,
			'userid' : str ,
			'AetherShutdown' : bool,
			'DatabaseLocked' : bool,
			'MasterStatus' : str ,
			'MEM_REQ_TYPE' : str ,
			'MEM_REQ_SIZE' : float,
			'CPU_CNT' : float,
			'CPU_TIME' : float,
			'MEM_USAGE' : float,
			'HOST_ID' : str ,
			'SUBMIT_TIME' : 'datetime',
			'START_TIME' : 'datetime',
			'END_TIME' : 'datetime',
			'RESUBMIT_COUNT' : float ,
			'MAX_CPU' : float ,
			'MAX_MEM' : float,
			'EXIT_INFO' : float,
			'STATUS' : str ,
			'RunTime' : float,
			'TurnAroundTime' : float,
			'RunTimeBin(Hrs)' : str
		}


		self.GraphParams = dict()
		self.operators = [
			["ge ", ">="],
			["le ", "<="],
			["lt ", "<"],
			["gt ", ">"],
			["ne ", "!="],
			["eq ", "="],
			["contains "],
			["not_contains "],
			["isin "],
			["not_in "],
			["datestartswith "],
		]
		
		self.GraphTypeMap = {
			"Bar": go.Bar,
			"BarH": go.Bar,
			"BarStacked": go.Bar,
			"BarStackedH": go.Bar,
			"Line": go.Scattergl,
			"Area": go.Scatter,
			"Scatter": go.Scattergl,
			"Pie": go.Pie,
			"Histogram": go.Histogram,
		}
		self.GraphModeMap = {
			"Bar": "",
			"BarH": "",
			"BarStacked": "",
			"BarStackedH": "",
			"Pie": "",
			"Histogram": "",
			"Line": "lines",
			"Area": "lines",
			"Scatter": "markers",
		}
		self.aggregateFuncs = [
							'mean',
							'sum',
							'count' ,
							'std' ,
							'var',
							'sem',
							'first',
							'last',
							'min',
							'max'
								]

		self.NumericaggregateFuncs = [
							'mean',
							'sum',
							'std' ,
							'var',
							'sem',
								]
		self.GraphParamsOrder2 = [
			"Xaxis",
			"GraphType",
			"Primary_Yaxis",
			"Primary_Legends",
			"Aggregate_Func"
		]

		self.AvailableDataTypes= {
			'string':str, 
			'int' : int, 
			'float': float,
			'datetime' : 'datetime64[ns]', 
			'datetime_custom_format' : 'datetime64[ns]', 
			'boolean': bool
		}
		self.separatorMap={
			 "<tab>": "\t",
			 "<space>" : " ",
			 ",<comma>": ",",
			 ";<semi-colon>": ";",
			 ":<colon>": ":",
			 "#<hash>": "#",
			 "rdb": "rdb",
		}


		self.GraphParamsOrder = self.GraphParamsOrder2 + [ "Secondary_Legends"]

	def read_file_in_df(self,  FileInfo):
		DebugMsg2("Inside def read_file_in_df(self,  FileInfo):")
		DebugMsg2("FileInfo['FileFormat']",FileInfo['FileFormat'])
		mtime = os.path.getmtime(FileInfo['Path'])
		if mtime > FileInfo['LastModified']:
			Info("Reading file " + str(FileInfo['Path']) + " skiprows=" + str(FileInfo['SkipRows'])  )
			FileInfo['LastModified'] = mtime
			dtypes=self.loadMetadata(self.default_df_index,'ColumnsDataTypes')
			dates_col=[]
			if dtypes is not None:
				for col in dtypes:
					if dtypes[col]=='datetime64[ns]':
						Info("Updating Dtypes %s" % col )
						dtypes[col]='object'
						dates_col.append(col)
			if FileInfo['FileFormat']=="xlsx":
				if FileInfo['FileLoadOptions']==None:
					raise ValueError("SheetName is not defined")
				pickle_filename=  FileInfo['Path'] + "." + FileInfo['FileLoadOptions'] + ".pickle.gz"  
				df=None
				if os.path.exists(pickle_filename) and mtime < os.path.getmtime(pickle_filename):  
					DebugMsg2("pd.read_pickle")
					df=pd.read_pickle(pickle_filename)
				else:
					DebugMsg2("pd.read_excel")
					df=pd.read_excel(FileInfo['Path'],sheet_name=FileInfo['FileLoadOptions'],skiprows=FileInfo['SkipRows'],dtype=dtypes)
					DebugMsg2("Saving Pickle")
					df.to_pickle(pickle_filename)
					DebugMsg2("Saving Pickle Done")

				df.columns = df.columns.astype(str)

				#DebugMsg3("DF head=", df.head())
			elif FileInfo['FileFormat']=="sqldb":
				if FileInfo['FileLoadOptions']==None:
					raise ValueError("TableName is not defined")
				df=None
				DebugMsg2("pd.read_sql_query")

				df=sqlite_table_to_df(FileInfo['Path'], table_name=FileInfo['FileLoadOptions'])

				df.columns = df.columns.astype(str)

				#DebugMsg3("DF head=", df.head())
			else:
				DebugMsg3("Reading File1723")
				sep= FileInfo['FileLoadOptions']
				if FileInfo['FileLoadOptions']==None:
					raise ValueError("Separator is not defined")
					

				pickle_filename=  FileInfo['Path'] + ".pickle.gz"  
				if os.path.exists(pickle_filename) and mtime < os.path.getmtime(pickle_filename):  
					DebugMsg2("pd.read_pickle")
					df=pd.read_pickle(pickle_filename)
				else:
					DebugMsg2("pd.read_csv")
					if sep == "rdb" :
						df=read_rdb_in_df(FileInfo['Path'])
					else:
						df=pd.read_csv(FileInfo['Path'], sep=self.separatorMap[sep],skiprows=FileInfo['SkipRows'],dtype=dtypes,parse_dates=dates_col)
					df.to_pickle(pickle_filename)
				df.columns = df.columns.astype(str)

			col_ren={}
			for col in df.columns:
				col_ren[col]=re.sub("\s","__",col)
			df=df.rename(columns=col_ren)

			replace_dict=dict()
			if FileInfo['ReplaceWithNan'] is not None:
				for nan_value in FileInfo['ReplaceWithNan'].split(","):
					replace_dict[nan_value]=np.nan
				df = df.replace(replace_dict)
			DebugMsg2("Converting Dtypes")
			df = df.convert_dtypes(convert_integer=False,convert_floating=False,convert_string=False)
			DebugMsg2("Converting Dtypes Done")
			df = df.replace({pd.NA: np.nan})
			DebugMsg2("Replacing Nan")
#            self.DF_read_copy[FileInfo['Path']] = self.update_dtypes(df)
			self.DF_read_copy[FileInfo['Path']] = df
			
		else:
			Info("File not changed")
		return self.DF_read_copy[FileInfo['Path']].copy()
		
	
	def getDataFileName(self,datafile):
		
	
		DebugMsg2("Inside 2 def getDataFileName(self,datafile):")
		name= (datafile['Path']  + "#" 
				+ str(datafile['FileFormat'])  + "#" 
				+ str(datafile['FileLoadOptions'])  + "#" 
				+ str(datafile['SkipRows'])  + "#" 
				+ str(datafile['ReplaceWithNan'])  + "#" 
			  )
		DebugMsg2("Done def getDataFileName(self,datafile):")
		return name

	def update_df(self,Datafile,df_index):

		DebugMsg2("Inside def update_df(self,Datafile,df_index):")
		self.df[df_index] = self.read_file_in_df(Datafile)
		self.filtered_df[df_index] = self.df[df_index].copy()
		self.plot_df[df_index]=self.filtered_df[df_index]
		self.table_df=self.filtered_df[df_index]
	
	def loadLastLoadedFiles(self):
	
		DebugMsg2("Inside def loadLastLoadedFiles(self):")
		filelist=dict()
		if os.path.exists(self.RecentFilesListPath):
			with open(self.RecentFilesListPath) as json_file:
				filelist=json.load(json_file)  
			if "LastLoadedFile" in filelist:
				for df_index in filelist["LastLoadedFile"]:
					Info(df_index)
					name=filelist["LastLoadedFile"][df_index]
					self.DataFile[df_index]=filelist["recent"][name]
					if os.path.exists(self.DataFile[df_index]['Path']):
						self.update_df(self.DataFile[df_index],df_index)
					else:
						if 'path' in self.DataFile[df_index]:
							Info(self.DataFile[df_index]['path'] + " not exists")
						else:
							Info("Path not exists in  self.DataFile[" + str(df_index) + "]")

	
	def updateRecentFiles(self,df_index):

	
		DebugMsg2("Inside def updateRecentFiles(self,df_index):")
		filelist=dict()
		if os.path.exists(self.RecentFilesListPath):
			with open(self.RecentFilesListPath) as json_file:
				filelist=json.load(json_file)  

		if "recent" not in filelist:
			filelist["recent"]=dict()

		if "LastLoadedFile" not in filelist:
			filelist["LastLoadedFile"]=dict()
		
		if self.DataFile[df_index] is not None:
			name= self.getDataFileName(self.DataFile[df_index]) 
			filelist["LastLoadedFile"][df_index]=name
			filelist["recent"][name]=self.DataFile[df_index].copy()
			filelist["recent"][name]['LastModified'] = 0
		else:
			del(filelist["LastLoadedFile"][df_index])

		with open(self.RecentFilesListPath, "w") as outfile:
			json.dump(filelist,outfile)

	def readFileInitDash(self,df_index):

		DebugMsg2("Inside def readFileInitDash(self,df_index):")
		if self.df[df_index] is None:
			if  self.DataFile[df_index] is not None:
				self.df[df_index] = self.read_file_in_df(self.DataFile[df_index])
			else:
				self.df[df_index]=pd.DataFrame()
				
		self.figs = dict()


	def get_groupid(self, group):


		DebugMsg2("Inside def get_groupid(self, group):")
		return "TopLevelID"
		# return "-".join(group)
	
	def hasDuplicates(self,df):
	
		DebugMsg2("Inside def hasDuplicates(self,df):")
		s=set()
		i=0
		for x in df.index:
			i+=1
			s.add(str(list(df.loc[x])))
			if len(s) < i:
				return True
		return False

	def extract_data(self, df , keep_cols=[]):

		DebugMsg2("Inside def extract_data(self, df , keep_cols=[]):")
		if len(self.GraphParams["Xaxis"]) ==0 or  ( '#index' in self.GraphParams["Xaxis"]):
			df['#index']=df.index.copy()
			self.GraphParams["Xaxis"]=['#index']
		DebugMsg("extract_data",self.GraphParams['Xaxis'])
		DebugMsg("extract_data Primary legends " ,self.GraphParams['Primary_Legends'])
		filters_tmp_p = list(OrderedDict.fromkeys(self.GraphParams["Xaxis"] + self.GraphParams["Primary_Legends"]))
		filters_tmp_p2=list(OrderedDict.fromkeys(filters_tmp_p + keep_cols))

		DebugMsg("extract_data df columns",df.shape)
		DebugMsg("extract_data filters_tmp_p2",filters_tmp_p2)
		DebugMsg("extract_data filters_tmp_p",filters_tmp_p)
		DebugMsg("extract_data keep_cols",keep_cols)
		DebugMsg("extract_data Primary_Yaxis",self.GraphParams["Primary_Yaxis"])
		DebugMsg("extract_data Scatter_Labels",self.GraphParams["Scatter_Labels"])
		DebugMsg("extract_data Aggrega",self.GraphParams["Aggregate_Func"])
		df1 = None
		if len(self.GraphParams["Primary_Yaxis"]) > 0:
			df_p = None
			reqd_cols= list(OrderedDict.fromkeys(filters_tmp_p2 + self.GraphParams["Primary_Yaxis"] + self.GraphParams["Scatter_Labels"]))  ## make list unique preserving order
			if self.isAggregated():
#                for col in self.GraphParams["Primary_Legends"]:
#                        df[col] = df[col].astype(str).replace("nan", "#blank")
				for col in (keep_cols + self.GraphParams["Scatter_Labels"] + self.GraphParams["Primary_Yaxis"]):
					if col not in filters_tmp_p: 
						if self.GraphParams['Aggregate_Func'] in self.NumericaggregateFuncs:
							df[col]=pd.to_numeric(df[col],errors='coerce')

				DebugMsg("extract_data df.shape",df.shape)
				df_p = (
					df[ reqd_cols].groupby(filters_tmp_p)
					.agg(self.GraphParams['Aggregate_Func'])
				)
				DebugMsg("extract_data df_p.shape 2 ",df_p.shape)
				DebugMsg("extract_data df ",df_p.head())

				df_p=df_p.reset_index()
				df_p=df_p[reqd_cols]
			else:
				if self.GraphParams['GraphType'] != 'Scatter' and self.hasDuplicates(df[filters_tmp_p]):
					raise ValueError("Data contains duplicate values, Please use Aggregated Functions or plot a scatter chart")

				df_p = df[reqd_cols]
				#pass
			df1 = df_p
		DebugMsg("extract_data Aggrega",self.GraphParams["Aggregate_Func"])

		# fig = make_subplots()
		if df1 is not None:
			if len(self.GraphParams["Xaxis"]) > 1:
				self.newXAxisColName = "#" + "-".join(self.GraphParams["Xaxis"])
				df1[self.newXAxisColName] = ""
				df1 = df1.sort_values(by=self.GraphParams["Xaxis"])
				for col in self.GraphParams["Xaxis"]:
					df1[self.newXAxisColName] = (
						df1[self.newXAxisColName] + df1[col].astype(str) + ","
					)
			elif len(self.GraphParams["Xaxis"])==1:
				self.newXAxisColName = self.GraphParams["Xaxis"][0]
			else :
				self.newXAxisColName = "#index"
				df1[self.newXAxisColName]=df1.index.copy()
		return df1

	def split_filter_part(self,filter_part):

		DebugMsg2("Inside def split_filter_part(self,filter_part):")
		for operator_type in self.operators:
			for operator in operator_type:
				if operator in filter_part:
					ret_operator=operator_type[0].strip()
					name_part, value_part = filter_part.split(operator, 1)
					name = name_part[name_part.find("{") + 1 : name_part.rfind("}")]

					value_part = value_part.strip()
					v0 = value_part[0]
					str_value=False
					if v0 == value_part[-1] and v0 in ("'", '"', "`"):
						value = value_part[1:-1].replace("\\" + v0, v0)
						str_value=True
					if ret_operator == 'contains' or ret_operator == 'not_contains':
							value = str(value_part)
					elif ret_operator == 'isin' or ret_operator == 'not_in':
						value = value_part.split(",")
					elif not str_value:
						try:
							value = float(value_part)
						except ValueError:
							value = value_part
						

					# word operators need spaces after them in the filter string,
					# but we don't want these later
					return name, ret_operator, value

		return [None] * 3

	def update_filter(self,filter,append=True):

		DebugMsg2("Inside def update_filter(self,filter,append=True):")
		if not self.showingPreAggregatedData():
			key="FilterAgregatedData"
		else:
			key="Filters"
		if append:
			self.GraphParams[key] =  self.GraphParams[key].strip() + "\n" + filter
		else:
			self.GraphParams[key] =  filter.strip()


	def update_table_df(self):


		DebugMsg2("Inside def update_table_df(self):")
		if self.GlobalParams['TableDfIndex'] == 'None' or self.GlobalParams['TableDfIndex'] is None:
			self.table_df=pd.DataFrame()
		elif self.showingPreAggregatedData():
			self.table_df=self.filtered_df[self.GlobalParams['TableDfIndex']]
		else:
			self.table_df=self.plot_df[self.GlobalParams['TableDfIndex']]

	def update_column_names(self):

		DebugMsg2("Inside def update_column_names(self):")
		self.GlobalParams['Datatable_columns']=[]
		if self.plot_df[self.default_df_index] is not None:
			for col in self.plot_df[self.default_df_index].columns:
				if not str(col).startswith('#'):
					self.GlobalParams['Datatable_columns'].append(col)

	def isfloat(self,potential_float):

		DebugMsg2("Inside def isfloat(self,potential_float):")
		try:
			float(potential_float)
	##Try to convert argument into a float

			return True
		except ValueError:
			return False

	def create_eval_func(self,df,filter_expr):

		DebugMsg2("Inside def create_eval_func(self,df,filter_expr):")
		retval=filter_expr
		
		### Add double quotes autoamtically if .= some string
		matches= re.findall("(\{\S*?\}\s+.=\s+)(\S+)",retval)
		for groups in matches:
			if  (not re.match("\{.*\}",groups[1]))  and (not re.match("\".*\"",groups[1])) and (not self.isfloat(groups[1])):
				retval=retval.replace("".join(groups),groups[0] + "\"" + groups[1] + "\"")



		matches= re.findall("(\{)(\S*?)(}\s+contains\s+)(\"!\s+)(\S*)(\")",retval)
		if matches is None:
			matches= re.findall("(\{)(\S*?)(}\s+not_contains\s+)(\"!\s+)(\S*)(\")",retval)
		
		DebugMsg("create_eval_func Matches" , matches)
		for groups in matches:
			if is_string_dtype(df[groups[1]]):
				retval=retval.replace("".join(groups),"~df['" + groups[1] + "'].str.contains(\"" + groups[4] + "\")")
			elif is_numeric_dtype(df[groups[1]]):
				retval=retval.replace("".join(groups),"df['" + groups[1] + "'] != " + groups[4] )
				DebugMsg("RETVAL1", retval)

		matches= re.findall("(\{)([^}]*?)(}\s+contains\s+)(\S*)",retval)
		if matches is None:
			matches= re.findall("(\{)(\S*?)(}\s+not_contains\s+)(\"!\s+)(\S*)(\")",retval)
			
		
		for groups in matches:
			if is_numeric_dtype(df[groups[1]]):
				retval=retval.replace("".join(groups),"{" + groups[1] + "} == " + groups[3] )
				DebugMsg(retval)
				
		retval= re.sub("\{(\S*?)}(\s*=[^=])","\\1\\2",retval,1)
		retval= re.sub("\{(\S*?)}","df['\\1']",retval)
		retval= re.sub("\&\&", "&",retval)
		retval= re.sub("\|\|", "|",retval)
		if re.search("\s+contains\s+(\S*)",retval):
#            retval= re.sub("\s+contains\s+(\S*)", ".str.contains('\\1') == True",retval)
			retval,replacements= re.subn("(\S+)\s+contains\s+(\"[^\"]*\")", "(\\1.str.contains(\\2) == True)",retval)
			if replacements == 0:
				retval= re.sub("(\S+)\s+contains\s+(\S*)", "(\\1.str.contains('\\2') == True)",retval)
			DebugMsg("create_eval_func Retval=",retval)
		if re.search("(\S+)\s+not_contains\s+(\S*)",retval):
			retval,replacements= re.subn("(\S+)\s+not_contains\s+(\"[^\"]*\")", "~(\\1.str.contains(\\2) == True)",retval)
			if replacements == 0:
				retval= re.sub("(\S+)\s+not_contains\s+(\S*)", "~(\\1.str.contains('\\2') == True)",retval)
			DebugMsg("create_eval_func Retval=",retval)
		retval= retval.replace(".str.contains('#blank')",".isna()")
		DebugMsg("create_eval_func Filter Expr: " ,  retval)
		return retval

	def create_eval_func2(self,filter_expr):

		DebugMsg2("Inside def create_eval_func2(self,filter_expr):")
		retval=filter_expr
		DebugMsg("Filter Expr init: " ,  retval)

		retval= re.sub("\{(\S*?)}(\s*=[^=])","\\1\\2",retval,1)
		retval= re.sub("\{(\S*?)}","df['\\1']",retval)
		retval= re.sub("\&\&", "&",retval)
		retval= re.sub("\|\|", "|",retval)
		retval= re.sub("\s*contains\s*(\S*)", ".str.contains('\\1')",retval)
		retval= retval.replace(".str.contains('#blank')",".isna()")
		DebugMsg("Filter Expr: " ,  retval)
		
		return retval

	def get_legends(self,df,legend_cols):

		DebugMsg2("Inside def get_legends(self,df,legend_cols):")
		self.PrimaryLegendsColName=None
		legends=[]
		if len(legend_cols)>1:
			PrimaryLegendsColName = "#" + "-".join(legend_cols)
			df[PrimaryLegendsColName] = ""
			for col in legend_cols:
				df[PrimaryLegendsColName] = (
					df[PrimaryLegendsColName] + df[col].astype(str) + ":"
				)
		elif len(legend_cols)==1:
			PrimaryLegendsColName=legend_cols[0]
		else:
			return self.PrimaryLegendsColName,legends
		
		self.PrimaryLegendsColName=PrimaryLegendsColName
		legends=self.nan_to_blank(df[PrimaryLegendsColName].drop_duplicates())
		natsort_key1 = natsort.natsort_keygen(key=lambda y: str(y).lower())
		try:
			legends.sort(key=natsort_key1)
		except:
			pass

		return PrimaryLegendsColName,legends


	
	def update_fig(self, df, yaxis_cols, legend_cols, secondary_axis, fig,number_of_dfs,current_df_count,df_index,all_xaxis_vals):


	
		DebugMsg2("Inside def update_fig(self, df, yaxis_cols, legend_cols, secondary_axis, fig,number_of_dfs,current_df_count,df_index,all_xaxis_vals):")
		if len(yaxis_cols) == 0:
			return fig

		PrimaryLegendsColName,legends=self.get_legends(df,legend_cols)
		DebugMsg("Update Fig Legends",legends)
		if PrimaryLegendsColName is not None:
			self.GlobalParams['LegendTitle']=PrimaryLegendsColName
		else:
			self.GlobalParams['LegendTitle']=""

		append_yaxis_name_in_legend=False
		if len(yaxis_cols) > 1:
			append_yaxis_name_in_legend=True

		DebugMsg("Update Fig Legends2",legends)

		PlotFunc = self.GraphTypeMap[self.GraphParams["GraphType"]]
		mode = self.GraphModeMap[self.GraphParams["GraphType"]]

		no_of_legends=len(yaxis_cols)
		DebugMsg("Update no_of_legends yaxiscols",yaxis_cols)
		if len(legends) > 0 :
			no_of_legends=len(legends)* len(yaxis_cols)
		col=0
		DebugMsg("UPdate legends)",legends)
		DebugMsg("UPdate len(legends)",len(legends))
		DebugMsg("UPdate len(yaxis_cols)",len(yaxis_cols))
		DebugMsg("UPdate no_of_legends Legends2",no_of_legends)
		DebugMsg2("number_of_dfs",number_of_dfs)

		if (self.GraphParams["GraphType"] in  ["Pie"]):
			legend_names=[]
			self.GlobalParams['LegendTitle']=str(self.GraphParams["Xaxis"])
			for yaxis_col in yaxis_cols:
				if len(legends)==0:
					legend_name=yaxis_col
					legend_names.append(legend_name)
				for legend in legends:
					legend_name=legend
					DebugMsg("LegendName",legend)
					if append_yaxis_name_in_legend:
						if legend_name != "":
							legend_name=str(yaxis_col) + "<br>" + str(legend_name)
					legend_names.append(legend_name)
			
			legend_temp=[]
			for idx in range(number_of_dfs):
				for legend in legend_names:
					legend_temp.append("F" + str(idx + 1) + ":" + str(legend))
			legend_names=legend_temp

					


			if current_df_count == 1:
				fig = make_subplots(rows=number_of_dfs, cols=no_of_legends, specs=[ list(np.repeat([{'type':'domain'}], no_of_legends)) ] * number_of_dfs 
					, subplot_titles=tuple(legend_names))
			#fig = make_subplots(rows=1, cols=no_of_legends])
		self.GlobalParams['SecAxisTitles']=OrderedSet()
		self.GlobalParams['PrimaryAxisTitles']=OrderedSet()
		self.GlobalParams['available_legends']=OrderedSet()

		for yaxis_col in yaxis_cols:
			if len(legends)==0:
				legends=[""]
			first_legend=True
			for legend in legends:
				legend_name=legend
				secondary_axis=False
				if append_yaxis_name_in_legend:
					if legend_name != "":
						legend_name=str(yaxis_col) + "-" + str(legend_name)
					else:
						legend_name=yaxis_col

					self.GlobalParams['available_legends'].add(yaxis_col)
					if yaxis_col in self.GraphParams['Secondary_Legends']:
						secondary_axis=True
						self.GlobalParams['SecAxisTitles'].add(yaxis_col)
					elif len(self.GraphParams['Secondary_Legends'])>0:
						self.GlobalParams['PrimaryAxisTitles'].add(yaxis_col)
				else:
					self.GlobalParams['available_legends'].add(legend)
					if legend in self.GraphParams['Secondary_Legends']:
						secondary_axis=True
						self.GlobalParams['SecAxisTitles'].add(legend)
					elif len(self.GraphParams['Secondary_Legends'])>0:
						if first_legend:
							first_legend=False
							self.GlobalParams['PrimaryAxisTitles'].add(str(yaxis_col) + '<br>')   
						self.GlobalParams['PrimaryAxisTitles'].add(legend)
					else:
						self.GlobalParams['PrimaryAxisTitles'].add(yaxis_col)

				if self.add_df_index_in_legend:
					legend_name="F" + str(df_index) + ":" + str(legend_name)

				dftmp=df
				if PrimaryLegendsColName is not None: 
					if legend=="#blank":
						dftmp=df[df[PrimaryLegendsColName].isna()] 
					else:
					   #DebugMsg("legends",df[PrimaryLegendsColName])  
						dftmp = df[df[PrimaryLegendsColName] == legend]
				if len(self.GraphParams["Xaxis"])>0:
					dftmp =dftmp.sort_values( by=self.GraphParams["Xaxis"])
				if (self.GraphParams["GraphType"] in  ["Pie"]):
					col=col+1
					x=dftmp[self.newXAxisColName]
					y=dftmp[yaxis_col]
					fig.add_trace(
						PlotFunc( labels=x, values=y, hole=0.3,name=str(legend_name)),current_df_count,col
					)

				elif (self.GraphParams["GraphType"] in  ["Bar","BarH", "BarStacked", "BarStackedH"]):
					orient='v'
					x=dftmp[self.newXAxisColName]
					y=dftmp[yaxis_col]
					
					if (self.GraphParams["GraphType"]  in  ["BarH", "BarStackedH"]):
						orient='h'
						y=dftmp[self.newXAxisColName]
						x=dftmp[yaxis_col]
					fig.add_trace(
						PlotFunc(
							x=x, y=y, name=str(legend_name),orientation=orient
						),
						secondary_y=secondary_axis,
					)
				elif self.GraphParams["GraphType"] == "Area":
					fig.add_trace(
						PlotFunc(
							x=dftmp[self.newXAxisColName],
							y=dftmp[yaxis_col],
							name=str(legend_name),
							mode=mode,
							stackgroup="one",
						),
						secondary_y=secondary_axis,
					)
				elif self.GraphParams["GraphType"] == "Histogram":
					fig.add_trace(
						PlotFunc(
							x=dftmp[yaxis_col],
							name=str(legend_name),
						),
					)
				elif self.GraphParams["GraphType"] == "Scatter":
					t = []
					hovertemplate = (
						"<b>"
						+ self.newXAxisColName
						+ ": %{x} </b><br>"
						+ "<b>"
						+ str(yaxis_col)
						+ ": %{y} </b><br>"
					)
					custom_data = None
					colno = 0
					#if not self.isAggregated():
					for col in self.GraphParams["Scatter_Labels"]:
						t.append(dftmp[col])
						hovertemplate = (
							hovertemplate + col + ": %{customdata[" + str(colno) + "]}<br>"
						)
						colno += 1

					if len(t) > 0:
						custom_data = np.stack(tuple(t), axis=-1)

					fig.add_trace(
						PlotFunc(
							x=dftmp[self.newXAxisColName],
							y=dftmp[yaxis_col],
							name=str(legend_name),
							mode=mode,
							customdata=custom_data,
							hovertemplate=hovertemplate,
						),
						secondary_y=secondary_axis,
					)
				else:
					if len(all_xaxis_vals) > 0 :
						tmp_xaxis_vals=set(dftmp[self.newXAxisColName])
						missing_vals=list(all_xaxis_vals- tmp_xaxis_vals)
						tmpx=pd.DataFrame.from_dict({self.newXAxisColName:missing_vals,yaxis_col:None})
						dftmp=dftmp.append(tmpx)
					dftmp =dftmp.sort_values( by=self.newXAxisColName)

					fig.add_trace(
						PlotFunc(
							x=dftmp[self.newXAxisColName],
							y=dftmp[yaxis_col],
							name=str(legend_name),
							mode=mode,
						),
						secondary_y=secondary_axis,
					)

		return fig

	def strlist(self,list1):

		DebugMsg2("Inside def strlist(self,list1):")
		retlist=[]
		for ele in list1:
			retlist.append(str(ele))
		return retlist


	def initialize_figs(self):


		DebugMsg2("Inside def initialize_figs(self):")
		for group in self.groups:
			grpid = self.get_groupid(group)
			self.figs[grpid] = go.Figure()

	def update_graph(self,plot_df_indexes):

		DebugMsg2("Inside def update_graph(self,plot_df_indexes):")
		number_of_dfs=0
		self.add_df_index_in_legend=False
		for idx in self.df_indexes:
			if self.df[idx] is not None:
				number_of_dfs+=1
		if number_of_dfs> 1:
			self.add_df_index_in_legend=True


		for group in self.groups:
			grpid = self.get_groupid(group)
			self.figs[grpid] = go.Figure()
			self.figs[grpid] = make_subplots(specs=[[{"secondary_y": True}]])
			current_df_count=0
			df_indexes=self.df_indexes
			if 'All' in plot_df_indexes:
				df_indexes=self.df_indexes
			elif 'None' in plot_df_indexes:
				df_indexes=[]
			else:
				df_indexes=plot_df_indexes

			all_xaxis_vals=[]

			if is_string_dtype(self.plot_df[self.default_df_index][self.newXAxisColName]) and self.GraphParams["GraphType"] == "Line":
						### to align the xaxis values for line graphs  in multiple files
						### if not aligned, graphs could be distored
				DebugMsg("Getting values of all Xaxis points")
				for df_index in df_indexes:
					if self.plot_df[df_index] is None:
						continue
					all_xaxis_vals=all_xaxis_vals+ list(self.plot_df[df_index][self.newXAxisColName])

				all_xaxis_vals=set(all_xaxis_vals)

			for df_index in df_indexes:
				current_df_count+=1
				if self.df[df_index] is None:
					continue
				df=self.plot_df[df_index]
				if df is None:
					continue
				self.figs[grpid] =self.update_fig(
					df,
					self.GraphParams["Primary_Yaxis"],
					self.GraphParams["Primary_Legends"],
					False,
					self.figs[grpid],
					number_of_dfs,
					current_df_count,
					df_index,all_xaxis_vals
				)

			self.figs[grpid].update_layout(
				hoverlabel=dict(namelength=-1),
				legend_title=self.GlobalParams["LegendTitle"],
				margin={"l": 2, "r": 2, "t": 40, "b": 40},
				height=1000
			)
			if self.GraphParams["GraphType"] == "Scatter":

				self.figs[grpid].update_layout(hovermode="closest")
			else:
				self.figs[grpid].update_layout(hovermode="x")

			self.figs[grpid].update_xaxes(title_text=str(self.GraphParams["Xaxis"]))

			# Set y-axes titles
			if len(self.GlobalParams['PrimaryAxisTitles'])>0:
				t=",".join(self.strlist(self.GlobalParams['PrimaryAxisTitles']))
				t='<br>'.join(textwrap.wrap(t,width=50)).replace("<br>,","<br>")
				self.figs[grpid].update_yaxes(
					title_text=t, secondary_y=False
				)
			if len(self.GlobalParams['SecAxisTitles'])>0:
				t=",".join(self.strlist(self.GlobalParams['SecAxisTitles']))
				t='<br>'.join(textwrap.wrap(t,width=50)).replace("<br>,","<br>")
				self.figs[grpid].update_yaxes(
				   # title_text=",", secondary_y=True
					title_text=t, secondary_y=True
				)

			if (self.GraphParams["GraphType"] == "BarStacked") or (
				self.GraphParams["GraphType"] == "BarStackedH" ) or (
				self.GraphParams["GraphType"] == "Area"
			):
				self.figs[grpid].update_layout(barmode="stack")

			if (self.GraphParams["GraphType"] != "BarH") and (
				self.GraphParams["GraphType"] != "BarStackedH" ):
				self.figs[grpid].update_yaxes(rangemode="tozero")
		Info("updated_figs")
		return ""


	def filter_sort_df(self,df, Allfilter, df_index,update_prev=True):


		DebugMsg2("Inside def filter_sort_df(self,df, Allfilter, df_index,update_prev=True):")
		## update_prev added to remove the recursive loop
		DebugMsg("filter_sort_df :: Entered", Allfilter)
		newFilters=Allfilter
		filters=Allfilter
		step_cnt=0
		update_previous_operations=False
		Operations_Done=[]
		FiltersApplied=[]
		for filter in filters:
			step_cnt+=1
			DebugMsg("filter_sort_df Filter= " + filter)
			DebugMsg("filter_sort_df 1 shape",df.shape)
			filter_expr=self.create_eval_func(df,filter)
			DebugMsg("filter_sort_df Step " + str(step_cnt) + " :: " + str(filter_expr))
			DebugMsg("filter_sort_df shape",df.shape)
			if filter.startswith("SortBy:"):
				filter=re.sub("^SortBy:","",filter)
				sort_by=json.loads(filter)
				if len(sort_by)>0:
					df = df.sort_values(
						[col["column_id"] for col in sort_by],
						ascending=[col["direction"] == "asc" for col in sort_by],
						inplace=False,
					)
			elif filter != "":
				DebugMsg("filter_sort_df Filter EXpr= " + filter_expr)
				if (re.match("^\s*\S*\s*=",filter_expr) and (not re.match("^\s*\S*\s*=\s*=",filter_expr) )) :
					DebugMsg("filter_sort_df Filter EXpr2= " + filter_expr)
					df=pd.eval(filter_expr,target=df)
					for col in df.columns:
						if col not in self.df[df_index].columns :
							if (not self.isAggregated()) or (not update_prev):
								self.df[df_index][col]=np.nan
							self.GlobalParams['ColumnsUpdated']=True
					if (not self.isAggregated()) or self.showingPreAggregatedData() or (not update_prev):
						DebugMsg("filter_sort_df updated df.index",df_index)
						self.df[df_index].loc[df.index]=df
						update_previous_operations=True
						if update_prev:
							Operations_Done=filters[:step_cnt]
							
							Allfilter=FiltersApplied
				else:
					DebugMsg("filter_sort_df Before filter shape",df.shape)
					df=df[pd.eval(filter_expr)]
					FiltersApplied.append(filter)
					DebugMsg("filter_sort_df After filter shape",df.shape)

		DebugMsg("update_previous_operations", update_previous_operations )
		DebugMsg("update_prev", update_prev)
		if update_previous_operations and update_prev:
			DebugMsg("update_previous_operations and update_prev" , Allfilter )
			DebugMsg("update_previous_operations and update_prev2 " , len(self.GraphParams['PreviousOperations']) )
			DebugMsg("update_previous_operations and update_prev2 " , self.GraphParams['PreviousOperations'])

			if len(self.GraphParams['PreviousOperations']) == 0  or self.GraphParams['PreviousOperations'][-1] != Operations_Done:
				self.GraphParams['PreviousOperations'].append(Operations_Done)
				DebugMsg("updated newFilters", newFilters )
			newFilters=Allfilter
		return df,newFilters


	def filter_sort_df2(self,dff, sort_by, filter):


		DebugMsg2("Inside def filter_sort_df2(self,dff, sort_by, filter):")
		if dff is not None:
			filtering_expressions = filter.split(" && ")
			for filter_part in filtering_expressions:
				col_name, operator, filter_value = self.split_filter_part(filter_part)
				if col_name not in dff.columns:
					continue

				if (filter_value=="#blank") :
					if operator in ["eq","contains"]:
						tmpindex=dff[col_name].isna()
						dff = dff.loc[tmpindex]
					elif operator == "ne":
						tmpindex=~dff[col_name].isna()
						dff = dff.loc[tmpindex]
				elif operator in ("eq", "ne", "lt", "le", "gt", "ge"):
					# these operators match pandas series operator method names
					dff = dff.loc[getattr(dff[col_name], operator)(filter_value)]
				elif operator == "contains":
					tmpindex=dff[col_name].astype(str).str.contains(str(filter_value))
					dff = dff.loc[tmpindex]
				elif operator == "datestartswith":
					# this is a simplification of the front-end filtering logic,
					# only works with complete fields in standard format
					dff = dff.loc[dff[col_name].str.startswith(filter_value)]

			if len(sort_by):
				dff = dff.sort_values(
					[col["column_id"] for col in sort_by],
					ascending=[col["direction"] == "asc" for col in sort_by],
					inplace=False,
				)
		return dff

	def get_number_of_records(self):

		DebugMsg2("Inside def get_number_of_records(self):")
		retval=[]
		for df_index in self.df_indexes:
			if self.showingPreAggregatedData():
				df=self.filtered_df[df_index]
			else:
				df=self.plot_df[df_index]
			if df is not None:
				retval.append(html.H3("    File" + df_index + ": " + str(df.shape[0]),style={'margin-left': "40px"}))
			else:
				retval.append(html.H3("    File" + df_index + ": " + str("Not loaded"),style={'margin-left': "40px"}))
		return retval


	def update_table(self,page_current, page_size,df_index):


		DebugMsg2("Inside def update_table(self,page_current, page_size,df_index):")
		if df_index == 'None' or df_index is None:
			df=pd.DataFrame()
		elif self.showingPreAggregatedData():
			df=self.filtered_df[df_index]
			self.table_df=df
		else:
			df=self.plot_df[df_index]
			self.table_df=df
	
		retval=df.iloc[
			page_current * page_size : (page_current + 1) * page_size
		].to_dict("records")
		return retval

	def get_fig(self, grpid):

		DebugMsg2("Inside def get_fig(self, grpid):")
		return self.figs[grpid]

	def get_dropdown_values(self, type,df=None):

		DebugMsg2("Inside def get_dropdown_values(self, type,df=None):")
		list_of_dic = []
		if df is None:
			if self.df[self.default_df_index] is None:
				return list_of_dic
			df=self.df[self.default_df_index]
		if type == "GraphType":
			for col in self.GraphTypeMap:
				list_of_dic.append({"label": col, "value": col})
		elif type == "Secondary_Legends" :
			if self.GlobalParams['available_legends'] is not None:
				for col in self.GlobalParams['available_legends']:
					list_of_dic.append({"label": col, "value": col})
			else:
				list_of_dic.append({"label": "", "value": ""})
		elif type == "Aggregate_Func":
			list_of_dic.append({"label": "None", "value": ""})
			for col in self.aggregateFuncs:
				list_of_dic.append({"label": col, "value": col})
		elif type == "SavedGraphNames":
			for col in self.SavedGraphList:
				list_of_dic.append({"label": col, "value": col})
		elif type == "HistoricalGraphNames":
			for col in self.HistoricalGraphList:
				list_of_dic.append({"label": col, "value": col})
		elif type == "plot_index":
			list_of_dic.append({"label": "All", "value": "All"})
			list_of_dic.append({"label": "None", "value": "None"})
			for idx in self.df_indexes:
				if self.DataFile[idx] is not None:
					list_of_dic.append({"label": idx, "value": idx})
		elif type == "df_index":
			list_of_dic.append({"label": "None", "value": "None"})
			for idx in self.df_indexes:
				if self.DataFile[idx] is not None:
					list_of_dic.append({"label": idx, "value": idx})
		elif type == "input_recentlyLoadedFiles":
			filelist=[]
			if os.path.exists(self.RecentFilesListPath):
				with open(self.RecentFilesListPath) as json_file:
					filelist=json.load(json_file)  
					if "recent" in filelist:
						for col in filelist["recent"]:
							list_of_dic.append({"label": col, "value": col})
		elif type == "AvailableDataTypes":
			for idx in self.AvailableDataTypes:
				list_of_dic.append({"label": idx, "value": idx})
		elif type == "AvailableSeparators":
			for idx in self.separatorMap:
				list_of_dic.append({"label": idx, "value": idx})
		elif type == "AvailableSheetNames":
			list_of_dic.append({"label": "data", "value": "data"})
			list_of_dic.append({"label": "data2", "value": "data2"})
		elif type == "Functions":
			list_of_dic.append({"label": "Add Tag", "value": "Add Tag"})
		elif type == "InputFileFormats":
			list_of_dic.append({"label": "text", "value": "text"})
			list_of_dic.append({"label": "pickle", "value": "pickle"})
			list_of_dic.append({"label": "sqldb", "value": "sqldb"})
			list_of_dic.append({"label": "xlsx", "value": "xlsx"})
			list_of_dic.append({"label": "json", "value": "json"})
		else :
			for col in df.columns:
				list_of_dic.append({"label": col, "value": col})
		return list_of_dic

	def layout_tab(self,display):

		DebugMsg2("Inside def layout_tab(self,display):")
		selected_tab='tab-basic'
		tabs=html.Div([
			dcc.Tabs(
				id="tabs-with-classes",
				value=selected_tab,
				parent_className='custom-tabs',
				className='custom-tabs-container',
				children=[
					dcc.Tab(
						label='Plot',
						value='tab-basic',
						className='custom-tab',
						selected_className='custom-tab--selected',
					),
					dcc.Tab(
						label='DataType',
						value='tab-2',
						className='custom-tab',
						selected_className='custom-tab--selected'
					),
					
				]),
			html.Div(id='tabs-content-classes',children=self.render_tab_content(selected_tab))
		], 
		style=dict(display=display,width='100%')
		)
		return tabs


	def get_Outputs_tab(self):


		DebugMsg2("Inside def get_Outputs_tab(self):")
		Outputs = list()
		Outputs.append(Output("tab1_container", "style"))
		Outputs.append(Output("tab2_container", "style"))
		return Outputs

	def get_Inputs_tab(self):

		DebugMsg2("Inside def get_Inputs_tab(self):")
		return Input('tabs-with-classes', 'value')

	def get_tab_containers_styles(self,tab):

		DebugMsg2("Inside def get_tab_containers_styles(self,tab):")
		number_of_tabs=2
		tab_container_styles=[dict(display='none')]* number_of_tabs
		current_tab=-1
		if tab == 'tab-basic':
			current_tab=0
		elif tab == 'tab-2':
			current_tab=1
		tab_container_styles[current_tab]=dict(display='block')
		return tab_container_styles

	def render_tab_content(self,tab):

		DebugMsg2("Inside def render_tab_content(self,tab):")
		tabs_styles=self.get_tab_containers_styles('tab-basic')
		return self.layout_tab1(tabs_styles[0]) +  self.layout_tab2(tabs_styles[1]) 
		


	def layout_plot_inputs(self):
		


		DebugMsg2("Inside def layout_plot_inputs(self):")
		tab1_display="inline-table"
		if self.ControlMode:
			disp1="table"
			disp='inline-table'
		else:
			disp1='none'
			disp='none'
		new_divs = []

		for txtbox in self.GraphParamsOrder:
			multi=True
			clearable=True
			def_value=None
			if txtbox=="GraphType" or txtbox=="Aggregate_Func":
				multi=False
			if txtbox=="GraphType" :
				clearable=False
				def_value="Scatter"
			new_divs.append( html.H3(txtbox,style=dict(display=tab1_display,width='15%')))
			new_divs.append( html.Div([
						dcc.Dropdown(
							id="input_{}".format(txtbox),
							options=self.get_dropdown_values(txtbox),
							value=def_value,
							multi=multi,
							clearable=clearable)],
							style=dict(display=tab1_display,width='30%')
						),
			)
			new_divs.append( html.Div(style=dict(display=tab1_display,width='5%')))



		new_divs.append(
			html.Div(
				[
					html.H3("Additional_Labels",style=dict(display=disp,width='15%')),
					html.Div([
					dcc.Dropdown(
						id="input_{}".format("Scatter_Labels"),
						options=self.get_dropdown_values("Scatter_Labels"),
						value=None,
						multi=True,
					)],style=dict(display=disp,width='65%')) ,
					html.Div(style=dict(display=disp,width='5%')),
					html.Div([
					dcc.Dropdown(
						id="select_plot_df_index",
						options=self.get_dropdown_values("plot_index"),
						value="All",
						multi=True,
					)],style=dict(display=disp,width='10%')) ,
				],
				style=dict( display= "table",width='100%'),
			)
		)
		new_divs = html.Div(new_divs, style=dict(display=disp1,width='100%'))
		return new_divs


	def get_dtypes_display(self):


		DebugMsg2("Inside def get_dtypes_display(self):")
		display1=[html.H3('Show Data Types')]
		dtypes=self.get_dypes(None)
		if len(dtypes)>0:
			display1.append(dcc.Markdown("**"+ "Column" + "** : " +   "Datatype"))
			display1.append(dcc.Markdown("**"+ "######" + "** : " +   "########"))
		for col in dtypes:
			display1.append(dcc.Markdown("**"+ col + "** : " +   dtypes[col]))
		return display1

	def layout_display_data_types(self):

		DebugMsg2("Inside def layout_display_data_types(self):")

		disp='inline-block'
		divs=[]
		divs.append(
			html.Div(
			self.get_dtypes_display(),id='display_datatypes',
			 style=dict(display=disp,width='100%',height='100%')
			 ),
		)
		DebugMsg3("divs ", divs)
		return (divs)


	def layout_args(self,arguments):


		DebugMsg2("Inside def layout_args(self,arguments):")
		argslist_layout=[]
		max_args=5
		for arg in range(0,max_args):
			disp='inline-block'
			if arg>= arguments:
				disp='none'
			strarg=str(arg)
			argslist_layout.append(dcc.Input(
					id="input_arg" + strarg,
					type='text',
					placeholder='argument' + strarg,
					style=dict(display=disp,width='30%', height='100%')
				)
			)
			argslist_layout.append(html.Div([
			html.H3()], style=dict(display=disp,width='2%',height='100%', verticalAlign='center')))
		return argslist_layout




	def layout_functions(self):




		DebugMsg2("Inside def layout_functions(self):")
		disp='inline-block'
		divs=[]
		divs.append(
			html.Div([
		
			html.Div([
			html.H3("Execute Functions" )], style=dict(display=disp,width='100%',height='100%', verticalAlign='center')),

			html.Div([
				dcc.Dropdown(
					id="input_function",
					options=self.get_dropdown_values("Functions"),
					value=None,
				)], 
				style=dict(display=disp,width='25%',height='100%', verticalAlign='center')
			),
		
			html.Div([
			html.H3()], style=dict(display=disp,width='2%',height='100%', verticalAlign='center')),
			
			html.Div(self.layout_args(1),
					id="input_args_container",
				style=dict(display=disp,width='65%', verticalAlign='top')
			),   

			html.Button("Apply", id="btn_apply_func", n_clicks=0,style=dict(verticalAlign='top',display=disp,width='5%',height='100%')),

			],style={'display':'block','width':'100%','height' : '100%'} 
			)
		)
		return divs



	def layout_update_data_types(self):



		DebugMsg2("Inside def layout_update_data_types(self):")
		disp='inline-block'
		divs=[]
		divs.append(
			html.Div([
		
			html.Div([
			html.H3("Change data type of columns" )], style=dict(display=disp,width='100%',height='100%', verticalAlign='center')),

			html.Div([
				dcc.Dropdown(
					id="input_cols_dytpe",
					options=self.get_dropdown_values(""),
					value=None,
					multi=True,
				)], 
				style=dict(display=disp,width='25%',height='100%', verticalAlign='center')
			),
		
			html.Div([ html.H1()],style=dict(display=disp,width='5%')), 

			html.Div([
			html.H3("Current Dtypes", id="lbl_current_dype")], style=dict(display=disp,width='10%',height='100%', verticalAlign='center')),
			
			html.Div([
			html.H3("Select new Datatype" )], style=dict(display=disp,width='15%',height='100%', verticalAlign='center', textAlign='right')),

			html.Div([
			html.H1()],style=dict(display=disp,width='5%')),
			html.Div([
				dcc.Dropdown(
					id="input_cols_dytpe_val",
					options=self.get_dropdown_values("AvailableDataTypes"),
					value='string',
					multi=False,
					clearable=False,
			)],   
				style=dict(display=disp,width='15%',height='100%', verticalAlign='center')
			),
			html.Div([
			 dcc.Input(
						id="input_custom_datetime_format",
						type='text',
						placeholder='CustomDatetimeFormat',
						style=dict(width='100%',height='100%' )
			),
			html.A("Format Help",href="https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior",target="_bank")
			
			],
					id="input_custom_datetime_format_container",
				style=dict(display='none',width='10%', verticalAlign='top')
			),   

			html.Div([ html.H1()],style=dict(display=disp,width='2%')),
			html.Button("Apply", id="btn_apply_dtype", n_clicks=0,style=dict(verticalAlign='top',display=disp,width='5%',height='100%')),

			],style={'display':'block','width':'100%','height' : '100%'} 
			)
		)
		return divs
	
	def layout_tab1(self,tab_style):
	
		DebugMsg2("Inside def layout_tab1(self,tab_style):")
		divs=[]
		divs=self.layout_plot_top()

		divs.append(self.layout_plot_inputs())
		divs=divs + self.layout1() + self.layout_save_plots() +self.dataframe_layout(self.default_df_index) + self.layout_number_records()
		divs=[html.Div(divs,id='tab1_container', style=tab_style)]
		return divs

	def layout_tab2(self,tab_style):

		DebugMsg2("Inside def layout_tab2(self,tab_style):")
		divs=[]
		divs.append(html.Div(self.layout_update_data_types()  ))
		divs.append(html.Div(self.layout_functions()  ))
		divs.append(html.Div(self.layout_display_data_types() ))
		divs.append(html.Div([html.P([ html.Br()] * 5 ,id='dd-output-container')]))
		divs=[html.Div(divs,id='tab2_container',style=tab_style)]
		return divs


	def layout_filepath(self):


		DebugMsg2("Inside def layout_filepath(self):")
		disp='inline-block'
		divs=[]
		divs.append(
			html.Div(
				[
					html.Button("Reload Previous State", id="btn_page_reload_previous", n_clicks=0,style=dict(display='inline-block',width='1%',height='100%',verticalAlign='top')),
					html.Button("Load", id="btn_load", n_clicks=0,style=dict(display='inline-block',width='4%',height='100%',verticalAlign='top')),
					html.Div([
					dcc.Input(
						id="input_loadFileName",
						type='text',
						placeholder='Path of file to load',
						style=dict(height='80%' ,width='90%')
						)],
						style=dict(display=disp,width='27%',height='100%',verticalAlign='top')
					),
			#		dcc.Checklist(id='input_fileformat', options=[ {'label': 'xlsx', 'value': 'True'} ], value=[], style=dict(display='inline-block',width='3%',verticalAlign='top')) , 

					html.Div(
					children=dcc.Dropdown(id='input_fileformat',
										value="text",
										multi=False,
										clearable=False,
										options=self.get_dropdown_values("InputFileFormats")
									),
					style=dict(display=disp,width='6%',height='100%',verticalAlign='top')
					),

					html.Div(
					children=dcc.Dropdown(id='input_loadFileOptions',
										value="<tab>",
										multi=False,
										clearable=False,
										options=self.get_dropdown_values("AvailableSeparators")
									),
					style=dict(display=disp,width='9%',height='100%',verticalAlign='top')
					),
					html.Div([
					dcc.Input(
						id="input_skiprows",
						type='number',
						placeholder='SkipRows',
						style=dict(height='80%' ,width='90%')
						)],
						style=dict(display=disp,width='5%',height='100%',verticalAlign='top')
					),
					html.Div([
					dcc.Input(
						id="input_replaceWithNan",
						type='text',
						placeholder='ReplaceWithNan',
						style=dict(height='80%' ,width='90%')
						)],
						style=dict(display=disp,width='10%',height='100%',verticalAlign='top')
					),
					html.Div([
					dcc.Dropdown(
						id="input_recentlyLoadedFiles",
						options=self.get_dropdown_values("input_recentlyLoadedFiles"),
						value=None,
						multi=False,
						optionHeight=120,
						)],
						style=dict(display=disp,width='37%',height='100%',verticalAlign='center')
					),
				],
				style=dict(display='block',width='100%',height="35px")
			)
		)
		return divs

	def layout_filepath2(self):

		DebugMsg2("Inside def layout_filepath2(self):")
		disp='inline-block'
		divs=[]
		divs.append(
			html.Div(
				[
					html.Button("Load", id="btn_load2", n_clicks=0,style=dict(display='inline-block',width='5%',height='100%',verticalAlign='top')),
					html.Div([
					dcc.Input(
						id="input_loadFileName2",
						type='text',
						placeholder='Path of file to load',
						style=dict(height='80%' ,width='90%')
						)],
						style=dict(display=disp,width='27%',height='100%',verticalAlign='top')
					),
				#	dcc.Checklist(id='input_fileformat2', options=[ {'label': 'xlsx', 'value': 'True'} ], value=[], style=dict(display='inline-block',width='3%',verticalAlign='top')) , 

					html.Div(
					children=dcc.Dropdown(id='input_fileformat2',
										value="text",
										multi=False,
										clearable=False,
										options=self.get_dropdown_values("InputFileFormats")
									),
					style=dict(display=disp,width='6%',height='100%',verticalAlign='top')
					),
					html.Div(
					children=dcc.Dropdown(id='input_loadFileOptions2',
										value="<tab>",
										multi=False,
										clearable=False,
										options=self.get_dropdown_values("AvailableSeparators")),
					style=dict(display=disp,width='9%',height='100%',verticalAlign='top')
					),
					
					html.Div([
					dcc.Input(
						id="input_skiprows2",
						type='number',
						placeholder='SkipRows',
						style=dict(height='80%' ,width='90%')
						)],
						style=dict(display=disp,width='5%',height='100%',verticalAlign='top')
				   ),
					html.Div([
					dcc.Input(
						id="input_replaceWithNan2",
						type='text',
						placeholder='ReplaceWithNan',
						style=dict(height='80%' ,width='90%')
						)],
						style=dict(display=disp,width='10%',height='100%',verticalAlign='top')
					),
					html.Div([
					dcc.Dropdown(
						id="input_recentlyLoadedFiles2",
						options=self.get_dropdown_values("input_recentlyLoadedFiles"),
						value=None,
						optionHeight=120,
						multi=False,
						)],
						style={'display' : disp,
								'width'  : '37%',
								'height' : '100%'
						}
					),
				],
				style=dict(display='block',width='100%',height="35px")
			)
		)
		return divs



	def hidden_callback_collectors(self):



		DebugMsg2("Inside def hidden_callback_collectors(self):")
		divs=[]
		divs.append(html.Div(id="hidden-div1", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Div(id="hidden-div2", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Div(id="hidden-div3", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-input_dropdown_vals", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-input_dropdown_vals-from-prevops", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-input_vals-from-lastgraph", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-reset_collector1", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-reset_collector2", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-page_refresh", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-page_refresh2", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-loadfile", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-loadfile2", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden2-loadfile", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden2-loadfile2", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-dropdown_options_dfindex1", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-dropdown_options_dfindex2", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(dcc.Checklist(id="hidden-input_args", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-outputFunc", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_table-from-refreshbtn", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_table-from-colsupdate", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_table-from-tableparams1", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_table-from-filter", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_figure-from-refreshbtn", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-updateBE-from-filter", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-updateBE-from-refreshbtn", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-updateBE-from-reset", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_filters-from-sortby", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_filters-from-tablequery", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_filters-from-textareafilter", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_filters-from-previousops", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_filters-from-clrfilters", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_filters-from-reset", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_previousOps-from-history", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_previousOpsValue", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_inputvals", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_SecondayLegends", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_historyGraphs-from-refreshbtn", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_historyGraphs-from-savebtn", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_historyGraphs-from-fileload", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_savedGraphs-from-savebtn", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_savedGraphs-from-fileload", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_savedGraphs-from-savedGraphs", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update_dropdown_values-from-history", style={"display": "none",'width':'100%','border':'2px solid black'}))
		divs.append(html.Button(id="hidden-update_dropdown_values-from-reset", style={"display": "none",'width':'100%','border':'2px solid black'}))

		divs.append(html.Button(id="hidden-update-preaggregateddata", style={"display": "none",'width':'100%','border':'2px solid black'}))


		return divs

	def layout_plot_top(self):

		DebugMsg2("Inside def layout_plot_top(self):")
		divs=[]
		if self.ControlMode:
			disp='none'
			disp='inline-block'
		else:
			disp='inline-block'

		divs.append(
			html.Div(
				[
					html.Button("Refresh", id="refreshbtn", n_clicks=0,style=dict(display='inline-block',width='10%',height='100%',verticalAlign='top')),
					html.Button("Reset", id="btn_reset", n_clicks=0,style=dict(display='inline-block',width='10%',height='100%',verticalAlign='top')),
					html.Div([
					dcc.Dropdown(
						id="input_graphName",
						options=self.get_dropdown_values("SavedGraphNames"),
						value=None,
						multi=False)],
						style=dict(display=disp,width='50%',height='100%',verticalAlign='top')
					),
					html.Div([
					dcc.Dropdown(
						id="input_HistoricalgraphName",
						options=self.get_dropdown_values("HistoricalGraphNames"),
						value=None,
						multi=False)],
						style=dict(display=disp,width='20%',height='100%',verticalAlign='center')
					),
					html.Button("Download Excel", id="btn_download",style=dict(display='inline-block',width='10%',height='100%',verticalAlign='top')),
					dcc.Download(id="download-dataframe-xlsx"),
						
				],
				style=dict(display='block',width='100%',height="35px")
			)
		)
		return divs

	def layout_save_plots(self):

		DebugMsg2("Inside def layout_save_plots(self):")

		if self.ControlMode:
			disp='none'
			disp='inline-block'
		else:
			disp='inline-block'

		if self.ControlMode:
			disp1="block"
		else:
			disp1='none'
	
		
		save_layout=[ 
			html.Div(
				[
					dbc.Button("Save", id="btn_save",style=dict(display='inline-table',width='10%' )),
					dcc.Input(id="input_save",type="text",placeholder="GraphName",style=dict(display='inline-table',width='25%' )),
					dbc.Button("Clear Filters", id="btn_clearFilters",style=dict(display='inline-table',width='10%' )),
					dcc.Checklist(
						id='chk_PreAggregated',
						 options=[ {'label': 'PreAggregatedData', 'value': 'Yes'} ],
						value= self.GraphParams["ShowPreAggregatedData"],
						 style=dict(display='inline-table',width='15%')
						 )  ,
					html.Div([
					dcc.Dropdown(
						id="select_data_df_index",
						options=self.get_dropdown_values("df_index"),
						value=self.default_df_index,
						clearable=False,
						multi=False)],
						style=dict(display=disp,width='20%',height='100%',verticalAlign='center')
					)

				]
				+ [html.Div(
				 [
					 dcc.Textarea( id='textarea-filter', wrap="off", value='', style=dict(width='40%',display='inline-block' )),
					 html.Div(style=dict(width='3%',display='inline-block')),
					 dcc.Textarea( id='textarea-previous_ops', wrap="off", value='', style=dict(width='40%',display='inline-block' )),
				]
				,style=dict(display=disp,width='98%' )
				)]
				, style=dict(display=disp1,width='100%'),
			)
		]
		return save_layout
	
	def layout_number_records(self):
	
		DebugMsg2("Inside def layout_number_records(self):")
		divs=[]
		divs.append(html.H2(" Number of Records :  "))
		divs.append(html.H3(" - ",id="lbl_records"))
		return divs

	def layout(self):

		DebugMsg2("Inside def layout(self):")
		divs=[]

		if self.ControlMode:
			disp1="block"
		else:
			disp1='none'
	

		divs.append(self.layout_tab(disp1))
		divs = self.hidden_callback_collectors() +  self.layout_filepath() + self.layout_filepath2() +   divs  
		ret=html.Div(divs,
						style={"overflowX": "auto","overflowY": "auto"})
		return ret

	def filter_layout(self):

		DebugMsg2("Inside def filter_layout(self):")
		return [
			html.Div(
					children=dcc.Dropdown(id='col_select',
										options=self.get_dropdown_values("")),
					style={'display': 'inline-block', 'width': '30%', 'margin-left': '7%'}),
			# DataFrame filter containers
			html.Div([
				html.Div(children=dcc.RangeSlider(id='num_filter',
												updatemode='drag')),
				html.Div(children=html.Div(id='rng_slider_vals'), ),
			], id='container_num_filter', ),
			html.Div(id='container_str_filter',
					children=dcc.Input(id='str_filter')),
			html.Div(id='container_bool_filter',
					children=dcc.Dropdown(id='bool_filter',
										options=[{'label': str(tf), 'value': str(tf)}
													for tf in [True, False]])),
			html.Div(id='container_cat_filter',
					children=dcc.Dropdown(id='cat_filter', multi=True,
										options=[])),
			html.Div([
				dcc.DatePickerRange(id='date_filter',
									start_date=dt.datetime.now().date(),
									end_date=dt.datetime.now().date(),
									max_date_allowed=dt.datetime.now().date(),
									),
									#initial_visible_month=int(dt.datetime.now().timestamp())
			], id='container_date_filter'),
		]

	def layout1(self):

		DebugMsg2("Inside def layout1(self):")
		divs = []
		for group in self.groups:
			grpid = self.get_groupid(group)
			divs.append(
				html.Div(
					[
						html.H3(group, id="lbl_" + grpid),
						dcc.Graph(id="fig_" + grpid, figure=self.get_fig(grpid)),
					],
					style={"border": "2px black solid"},
				)
			)
		return [html.Div(divs, style=dict(columnCount=1))]

	def create_conditional_style(self):

		df=self.table_df
		DebugMsg2("Inside def create_conditional_style(self):" + str(df.shape))
		style=[]
		for col in df.columns:
			name_length = len(str(col))
			pixel = 50 + round(name_length*5)
			pixel = str(pixel) + "px"
			style.append({'if': {'column_id': col}, 'minWidth': pixel})
		return style

	def dataframe_layout(self,df_index):

		DebugMsg2("Inside def dataframe_layout(self,df_index):")
		if self.ControlMode:
			disp=None
		else:
			disp='none'
		disp='block'
		columns=["","","","",""]
		if self.plot_df[df_index] is not None:
			columns=self.plot_df[df_index].columns[:5]
		html_divs = [
			html.Div(
				className="row",
				children=[
					html.Div(
						dash_table.DataTable(
							id="table-paging-with-graph",
							columns=[{"name": i, "id": i} for i in sorted(columns)],
							page_current=0,
							page_size=self.GlobalParams['TablePageSize'],
							page_action="custom",
							filter_action="custom",
							filter_query="",
							sort_action="custom",
							sort_mode="multi",
							sort_by=[],
							style_data_conditional=self.create_conditional_style(),
							fixed_rows={'headers': True, 'data': 0},
							style_table={ 'overflowX': 'scroll','overflowY': 'scroll'  }
						),
						className="six columns", style=dict(display=disp)
					),
					html.Div(
						id="table-paging-with-graph-container", className="five columns"
					),
				],
					style={"display": None},
			)
		]
		return html_divs

	def update_inputs(self,FirstLoad):

		DebugMsg2("Inside def update_inputs(self,FirstLoad):")
		retval = []
		DebugMsg2("update_inputs Firstload", FirstLoad)
		for txtbox in self.GraphParamsOrder:
			if FirstLoad:
				retval.append(self.GraphParams[txtbox])
				DebugMsg2("update_inputs self.GraphParams[txtbox]", self.GraphParams[txtbox])
			else:
				retval.append(dash.no_update)
		if FirstLoad:
			retval.append(self.GraphParams["Scatter_Labels"])
		else:
			retval.append(dash.no_update)
		return retval

	def get_Outputs3(self):

		DebugMsg2("Inside def get_Outputs3(self):")
		return Output("download-dataframe-xlsx", "data")

	def get_Inputs3(self):

		DebugMsg2("Inside def get_Inputs3(self):")
		Inputs = list()
		Inputs.append(Input("btn_download", "n_clicks"))
		return Inputs

	def refresh_callback3(self,df_index):

		DebugMsg2("Inside def refresh_callback3(self,df_index):")
		retval = []
		retval = dcc.send_data_frame(
			self.plot_df[df_index].to_excel, "data.xlsx", sheet_name="Sheet1"
		)
		return retval

	def ClrFilter_Outputs(self):

		DebugMsg2("Inside def ClrFilter_Outputs(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_filters-from-clrfilters", "n_clicks"))
		return Outputs

	def ClrFilter_Inputs(self):

		DebugMsg2("Inside def ClrFilter_Inputs(self):")
		Inputs = list()
		Inputs.append(Input("btn_clearFilters", "n_clicks"))
		return Inputs


	def ClrFilter_callback(self ):


		DebugMsg2("Inside def ClrFilter_callback(self ):")
		retval=[1]
	   
		DebugMsg("ClrFilter_callback IsAggregated " , self.isAggregated())
		DebugMsg("ClrFilter_callback showingAggregatedData " , self.showingPreAggregatedData())
		if self.isAggregated() and (not  self.showingPreAggregatedData()):
			self.GraphParams["FilterAgregatedData"] = ""
		else:
			self.GraphParams["Filters"] = ""
		return retval




	def get_Outputs5(self):

		DebugMsg2("Inside def get_Outputs5(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_savedGraphs-from-savebtn", "n_clicks"))
		Outputs.append(Output("hidden-update_historyGraphs-from-savebtn", "n_clicks"))
		return Outputs

	def get_Inputs5(self):

		DebugMsg2("Inside def get_Inputs5(self):")
		Inputs = list()
		Inputs.append(Input("btn_save", "n_clicks"))
		Inputs.append(State("input_save", "value"))
		return Inputs


	def refresh_callback5(self, n_clicks,GraphName,df_index):


		DebugMsg2("Inside def refresh_callback5(self, n_clicks,GraphName,df_index):")
		retval = [1,1]
		if (n_clicks is not None) and (GraphName is not None):
			self.GraphParams['Name']=GraphName
			self.save_history(df_index,"SavedGraphs",GraphName)
			self.save_history(df_index)
		return retval


	def save_history(self,df_index,label="HistoricalGraphs",graphName=None):


		DebugMsg2("Inside def save_history(self,df_index,graphName=None):")
		retval = ""
		graphlist=None
		graphlist = self.loadMetadata(df_index,label)
		if graphlist is None:
			graphlist={}
		if graphName is None:
			graphName=len(graphlist)
		self.set_Graphid()
		already_present=False
		for g in graphlist:
			if graphlist[g]['GraphId'] == self.GraphParams['GraphId']:
				already_present=True
				if self.GraphParams['Name'] != "":
					graphname=self.GraphParams['Name']
				else:
					graphname=g 
				temp={graphname : graphlist[g] }
				del graphlist[g]
				temp.update(graphlist)
				graphlist=temp
				break
		if not already_present:
			if self.GraphParams['Name'] != "":
				graphname=self.GraphParams['Name']
			temp={graphName : self.GraphParams }
			temp.update(graphlist)
			graphlist=temp
		
		if len(graphlist) > 100:
			graphlist = {k: graphlist[k] for k in list(graphlist)[:100]}

		if self.DataFile[df_index] is not None :
			self.updateMetadata(label,graphlist,df_index)
			if label=="HistoricalGraphs":
				self.HistoricalGraphList= graphlist
			else:
				self.SavedGraphList= graphlist 
		return retval



	def read_lastGraphFile(self,idx):



		DebugMsg2("Inside def read_lastGraphFile(self,idx):")
		tmp = self.loadMetadata(idx,"LastGraph")
		if tmp is not None:
			self.GraphParams = tmp
			self.update_aggregate()


	def isAggregated(self):


		DebugMsg2("Inside def isAggregated(self):")
		agg_value=self.GraphParams["Aggregate_Func"] 
		if agg_value is None or agg_value == '' or agg_value==[]  :
			return False
		else:
			return True

	def showingPreAggregatedData(self):

		DebugMsg2("Inside def showingPreAggregatedData(self):")
		if "Yes" in self.GraphParams['ShowPreAggregatedData']:
			return True
		else:
			return False

	def update_aggregate(self,agg_value=None, new_update=False):

		DebugMsg2("Inside def update_aggregate(self,agg_value=None, new_update=False):")
		if new_update:
			if agg_value is None or agg_value == '' or agg_value==[]  :
				self.GraphParams["Aggregate"] = "No"
			else:
				self.GraphParams["Aggregate"] = "Yes"

		for param in self.GraphParams:
			if self.GraphParams[param] is None:
				self.GraphParams[param] = []


	def blank_to_nan(self,list1,unique=False):


		DebugMsg2("Inside def blank_to_nan(self,list1,unique=False):")
		tmp=list()
		for x in list1 :
			if x=="#blank":
				x=math.nan
			tmp.append(x)
		if unique:
			tmp=list(OrderedSet(tmp))
		return tmp

	def nan_to_blank(self,list1,unique=False):

		DebugMsg2("Inside def nan_to_blank(self,list1,unique=False):")
		if type(list1)==str:
			return list1
		try:
			if math.isnan(list1):
				return ['#blank']
			else:
				return list1
		except:
			tmp=list()
			for x in list1 :
				try:
					if math.isnan(x):
						x='#blank'
				except:
					pass
				tmp.append(x)
			if unique:
				tmp=list(OrderedSet(tmp))
			return tmp

	def get_Outputs2(self):

		DebugMsg2("Inside def get_Outputs2(self):")
		Outputs = list()
		for txtbox in self.GraphParamsOrder2:
			Outputs.append(Output("input_{}".format(txtbox), "options"))
		Outputs.append(Output("input_{}".format("Scatter_Labels"), "options"))
		Outputs.append(Output("input_cols_dytpe", "options"))
		return Outputs

	def get_Inputs2(self):

		DebugMsg2("Inside def get_Inputs2(self):")
		Inputs = list()
		Inputs.append(Input("hidden-input_dropdown_vals", "n_clicks"))
		Inputs.append(Input("hidden-input_dropdown_vals-from-prevops", "n_clicks"))
		Inputs.append(Input("hidden-input_vals-from-lastgraph", "n_clicks"))
		return Inputs


	def callback_update_options(self,n_clicks,df_index):


		DebugMsg2("Inside def callback_update_options(self,n_clicks,df_index):")
		DebugMsg("callback_update_options n_clicks" , n_clicks)
		retval = list()
		for txtbox in self.GraphParamsOrder2:
			retval.append(self.get_dropdown_values(txtbox,self.filtered_df[self.default_df_index]))
		retval.append(self.get_dropdown_values("Scatter_Labels",  self.filtered_df[self.default_df_index]))
		retval.append(self.get_dropdown_values("",  self.filtered_df[self.default_df_index]))
		return retval

	def get_OutputsReset(self):

		DebugMsg2("Inside def get_OutputsReset(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_dropdown_values-from-reset","n_clicks"))
		Outputs.append(Output("hidden-updateBE-from-reset", "n_clicks"))
		Outputs.append(Output("hidden-update_filters-from-reset", "n_clicks"))
		return Outputs

	def get_InputsReset(self):

		DebugMsg2("Inside def get_InputsReset(self):")
		Inputs = list()
		Inputs.append(Input("btn_reset", "n_clicks"))
		return Inputs


	def callbackReset(self):


		DebugMsg2("Inside def callbackReset(self):")
		DebugMsg("Reset Done")
		self.reset=True
		self.initialize_GraphParams()
		self.initialize_figs()
		return [1,1,1]


	def get_OutputsPageRefresh(self):


		DebugMsg2("Inside def get_OutputsPageRefresh(self):")
		Outputs = list()
		Outputs.append(Output("hidden2-loadfile", "n_clicks"))
		Outputs.append(Output("hidden2-loadfile2", "n_clicks"))
		return Outputs

	def get_InputsPageRefresh(self):

		DebugMsg2("Inside def get_InputsPageRefresh(self):")
		Inputs = list()
		Inputs.append(Input("btn_page_reload_previous", "n_clicks"))
		return Inputs


	def callbackPageRefresh(self):
		DebugMsg2("Inside def callbackPageRefresh(self):")
		DebugMsg("Page Refresh")
		self.initialize_GraphParams()
		for df_index in self.df_indexes:
			if self.DataFile[df_index] is not None:
				self.df[df_index] = self.read_file_in_df(self.DataFile[df_index])
		return 0

	def get_OutputsLoadRecentFile(self):

		DebugMsg2("Inside def get_OutputsLoadRecentFile(self):")
		Outputs = list()
		Outputs.append(Output("hidden-reset_collector1", "n_clicks"))
		Outputs.append(Output("btn_reset", "n_clicks"))
		Outputs.append(Output("hidden-dropdown_options_dfindex1", "n_clicks"))
		Outputs.append(Output("input_loadFileName", "value"))
		Outputs.append(Output("input_fileformat", "value"))
		Outputs.append(Output("input_loadFileOptions", "value"))
		Outputs.append(Output("input_skiprows", "value"))
		Outputs.append(Output("input_replaceWithNan", "value"))
		return Outputs

	def get_InputsLoadRecentFile(self):

		DebugMsg2("Inside def get_InputsLoadRecentFile(self):")
		Inputs = list()
		Inputs.append(Input("input_recentlyLoadedFiles", "value"))
		return Inputs


	def get_OutputsLoadFile(self):


		DebugMsg2("Inside def get_OutputsLoadFile(self):")
		Outputs = list()
		Outputs.append(Output("input_recentlyLoadedFiles", "options"))
		Outputs.append(Output("hidden-loadfile", "n_clicks"))
		return Outputs

	def get_InputsLoadFile(self):

		DebugMsg2("Inside def get_InputsLoadFile(self):")
		Inputs = list()
		Inputs.append(Input("btn_load", "n_clicks"))
		Inputs.append(State("input_loadFileName", "value"))
		Inputs.append(State("input_fileformat", "value"))
		Inputs.append(State("input_loadFileOptions", "value"))
		Inputs.append(State("input_skiprows", "value"))
		Inputs.append(State("input_replaceWithNan", "value"))
		return Inputs

	def get_Inputs_custom_datetime(self):

		DebugMsg2("Inside def get_Inputs_custom_datetime(self):")
		Inputs = list()
		Inputs.append(Input("input_cols_dytpe_val", "value"))
		return Inputs

	def get_Outputs_custom_datetime(self):

		DebugMsg2("Inside def get_Outputs_custom_datetime(self):")
		Outputs = list()
		Outputs.append(Output("input_custom_datetime_format_container", "style"))
		return Outputs

	def get_Inputs_applyFunc(self):

		DebugMsg2("Inside def get_Inputs_applyFunc(self):")
		Inputs = list()
		Inputs.append(Input("btn_apply_func", "n_clicks"))
		Inputs.append(State("input_function", "value"))
		Inputs.append(State("hidden-input_args", "value"))
		return Inputs

	def get_Outputs_applyFunc(self):

		DebugMsg2("Inside def get_Outputs_applyFunc(self):")
		Outputs = list()
		Outputs.append(Output("hidden-outputFunc", "n_clicks"))
		return Outputs
	
	def addTag(self,tag):
	
		DebugMsg2("Inside def addTag(self,tag):")
		DebugMsg("Add tag " + tag)
		col="#TAG"
		for df_index in self.df_indexes:
			if self.filtered_df[df_index] is not None:
				if col not in self.filtered_df[df_index].columns :
					self.filtered_df[df_index][col]=tag
				else:
					self.filtered_df[df_index][col]=self.filtered_df[df_index][col]+ "," + tag 

				if col not in self.df[df_index].columns :
					self.df[df_index][col]=np.nan
				self.df[df_index].loc[self.filtered_df[df_index].index,col]=self.filtered_df[df_index][col]
				self.GlobalParams['ColumnsUpdated']=True
				DebugMsg("DF after tagging " + str(df_index),self.df[df_index].columns)

	
	def callback_apply_func(self,function, args):

	
		DebugMsg2("Inside def callback_apply_func(self,function, args):")
		if function== "Add Tag":
			tagname=args[0]
			self.addTag(tagname)
		return [dash.no_update]



	def get_Inputs_args(self):



		DebugMsg2("Inside def get_Inputs_args(self):")
		Inputs = list()
		Inputs.append(Input("input_arg0", "value"))
		Inputs.append(Input("input_arg1", "value"))
		Inputs.append(Input("input_arg2", "value"))
		Inputs.append(Input("input_arg3", "value"))
		Inputs.append(Input("input_arg4", "value"))
		return Inputs

	def get_Outputs_args(self):

		DebugMsg2("Inside def get_Outputs_args(self):")
		Outputs = list()
		Outputs.append(Output("hidden-input_args", "value"))
		return Outputs


	def get_Inputs_update_dtype(self):


		DebugMsg2("Inside def get_Inputs_update_dtype(self):")
		Inputs = list()
		Inputs.append(Input("btn_apply_dtype", "n_clicks"))
		Inputs.append(Input("input_cols_dytpe", "value"))
		Inputs.append(State("input_cols_dytpe_val", "value"))
		Inputs.append(State("input_custom_datetime_format", "value"))
		return Inputs

	def get_Outputs_update_dtype(self):

		DebugMsg2("Inside def get_Outputs_update_dtype(self):")
		Outputs = list()
		Outputs.append(Output("lbl_current_dype", "children"))
		return Outputs

	def get_Inputsinput_fileformat(self):

		DebugMsg2("Inside def get_Inputsinput_fileformat(self):")
		Inputs = list()
		Inputs.append(Input("input_fileformat", "value"))
		Inputs.append(State("input_loadFileName", "value"))
		return Inputs

	def get_Outputsinput_fileformat(self):

		DebugMsg2("Inside def get_Outputsinput_fileformat(self):")
		Outputs = list()
		Outputs.append(Output("input_loadFileOptions", "placeholder"))
		Outputs.append(Output("input_loadFileOptions", "options"))
		return Outputs

	def get_Inputsinput_fileformat2(self):

		DebugMsg2("Inside def get_Inputsinput_fileformat2(self):")
		Inputs = list()
		Inputs.append(Input("input_fileformat2", "value"))
		Inputs.append(State("input_loadFileName2", "value"))
		return Inputs

	def get_Outputsinput_fileformat2(self):

		DebugMsg2("Inside def get_Outputsinput_fileformat2(self):")
		Outputs = list()
		Outputs.append(Output("input_loadFileOptions2", "placeholder"))
		Outputs.append(Output("input_loadFileOptions2", "options"))
		return Outputs

	def get_OutputsLoadFileValue(self):

		DebugMsg2("Inside def get_OutputsLoadFileValue(self):")
		Outputs = list()
		Outputs.append(Output("input_recentlyLoadedFiles", "value"))
		return Outputs

	def get_InputsLoadFileValue(self):

		DebugMsg2("Inside def get_InputsLoadFileValue(self):")
		Inputs = list()
		Inputs.append(Input("hidden-loadfile", "n_clicks"))
		Inputs.append(Input("hidden2-loadfile", "n_clicks"))
		return Inputs



	def is_same_file(self,DataFile,filename,fileFormat,fileLoadOptions,skiprows,replaceWithNan):

		DebugMsg2("Inside def is_same_file(self,DataFile,filename,isxlsx,sheetname,skiprows,replaceWithNan):")
		if ( 
			filename == DataFile['Path'] and
			fileFormat== DataFile['FileFormat'] and
			fileLoadOptions == DataFile['FileLoadOptions'] and
			skiprows == DataFile['SkipRows'] and
			replaceWithNan == DataFile['ReplaceWithNan'] 
		):
			return True
		else:
			return False



	def callbackLoadFile(self,filename,fileFormat,fileLoadOptions,skiprows,replaceWithNan,df_index,refreshDashboard):
		DebugMsg2("Inside def callbackLoadFile(self,filename, fileFormat,fileLoadOptions,skiprows,replaceWithNan,df_index,refreshDashboard):")
		if skiprows is None or skiprows == "":
			skiprows=0
		skiprows=int(skiprows)
		if filename is not None:
			filename=os.path.abspath(filename)
			if df_index != self.default_df_index and self.DataFile[self.default_df_index] is None:
				raise ValueError("Load the first file first")

		DebugMsg2("Loading Done filename", filename)
		if (self.DataFile[df_index] is None) or ( filename is None) or  (not self.is_same_file(self.DataFile[df_index],filename,fileFormat,fileLoadOptions,skiprows,replaceWithNan)):
			DebugMsg2("reset dfindex=",df_index)
			self.setDataFile(filename,fileFormat,fileLoadOptions,skiprows,replaceWithNan,df_index)

			if refreshDashboard:
				self.createDashboard(df_index,self.DashboardMode)
		return 0

	def get_OutputsLoadRecentFile2(self):

		DebugMsg2("Inside def get_OutputsLoadRecentFile2(self):")
		Outputs = list()
		Outputs.append(Output("hidden-reset_collector2", "n_clicks"))
		Outputs.append(Output("hidden-dropdown_options_dfindex2", "n_clicks"))
		Outputs.append(Output("input_loadFileName2", "value"))
		Outputs.append(Output("input_fileformat2", "value"))
		Outputs.append(Output("input_loadFileOptions2", "value"))
		Outputs.append(Output("input_skiprows2", "value"))
		Outputs.append(Output("input_replaceWithNan2", "value"))
		return Outputs

	def get_InputsLoadRecentFile2(self):

		DebugMsg2("Inside def get_InputsLoadRecentFile2(self):")
		Inputs = list()
		Inputs.append(Input("input_recentlyLoadedFiles2", "value"))
		return Inputs

	def get_OutputsLoadFileValue2(self):

		DebugMsg2("Inside def get_OutputsLoadFileValue2(self):")
		Outputs = list()
		Outputs.append(Output("input_recentlyLoadedFiles2", "value"))
		return Outputs

	def get_InputsLoadFileValue2(self):

		DebugMsg2("Inside def get_InputsLoadFileValue2(self):")
		Inputs = list()
		Inputs.append(Input("hidden-loadfile2", "n_clicks"))
		Inputs.append(Input("hidden2-loadfile2", "n_clicks"))
		return Inputs



	def get_OutputsLoadFile2(self):



		DebugMsg2("Inside def get_OutputsLoadFile2(self):")
		Outputs = list()
		Outputs.append(Output("input_recentlyLoadedFiles2", "options"))
		Outputs.append(Output("hidden-loadfile2", "n_clicks"))
		return Outputs

	def get_InputsLoadFile2(self):

		DebugMsg2("Inside def get_InputsLoadFile2(self):")
		Inputs = list()
		Inputs.append(Input("btn_load2", "n_clicks"))
		Inputs.append(State("input_loadFileName2", "value"))
		Inputs.append(State("input_fileformat2", "value"))
		Inputs.append(State("input_loadFileOptions2", "value"))
		Inputs.append(State("input_skiprows2", "value"))
		Inputs.append(State("input_replaceWithNan2", "value"))
		return Inputs

   

	def get_reset_collectors_inputs(self):
		
		DebugMsg2("Inside def get_reset_collectors_inputs(self):")
		Inputs = list()
		Inputs.append(Input("hidden-reset_collector1", "n_clicks"))
		Inputs.append(Input("hidden-reset_collector2", "n_clicks"))
		return Inputs

	def get_reset_collectors_outputs(self):

		DebugMsg2("Inside def get_reset_collectors_outputs(self):")
		Outputs = list()
		Outputs.append( Output("hidden-page_refresh2", "n_clicks"))
		Outputs.append( Output("hidden-input_vals-from-lastgraph", "n_clicks"))
		return Outputs

	def get_Outputs_update_dropdown_options(self):

		DebugMsg2("Inside def get_Outputs_update_dropdown_options(self):")
		Outputs = list()
		Outputs.append( Output("select_data_df_index", "options"))
		Outputs.append( Output("select_plot_df_index", "options"))
		return Outputs

	def get_Inputs_update_dropdown_options(self):

		DebugMsg2("Inside def get_Inputs_update_dropdown_options(self):")
		Inputs = list()
		Inputs.append(Input("hidden-dropdown_options_dfindex1", "n_clicks"))
		Inputs.append(Input("hidden-dropdown_options_dfindex2", "n_clicks"))
		return Inputs


	def get_Inputs_previousOps(self):


		DebugMsg2("Inside def get_Inputs_previousOps(self):")
		Inputs = list()
		Inputs.append(Input('textarea-previous_ops', 'n_blur')),
		Inputs.append(State('textarea-previous_ops', 'value')),
		return Inputs

	def get_Outputs_previousOps(self):

		DebugMsg2("Inside def get_Outputs_previousOps(self):")
		Outputs = list()
		Outputs.append(Output('textarea-previous_ops', 'n_clicks')),
		return Outputs


	def get_Inputs_display_dtypes(self):


		DebugMsg2("Inside def get_Inputs_display_dtypes(self):")
		Inputs = list()
		Inputs.append(Input('display_datatypes', 'n_clicks')),
		return Inputs

	def get_Outputs_display_dtypes(self):

		DebugMsg2("Inside def get_Outputs_display_dtypes(self):")
		Outputs = list()
		Outputs.append(Output('display_datatypes', 'children')),
		return Outputs
	
	def get_InputsGraphOptions(self):
	
		DebugMsg2("Inside def get_InputsGraphOptions(self):")
		Inputs = list()
		Inputs.append(Input("hidden-page_refresh2", "n_clicks"))
		return Inputs

	def callback_GraphOptions(self):

		DebugMsg2("Inside def callback_GraphOptions(self):")
		self.updateGraphList(self.default_df_index)
		retval=[1,1,1]
		return retval


	def get_OutputsGraphOptions(self):
		DebugMsg2("Inside def get_OutputsGraphOptions(self):")
		Outputs = list()
		Outputs.append(Output("hidden-input_dropdown_vals","n_clicks"))
		Outputs.append(Output("hidden-update_historyGraphs-from-fileload","n_clicks"))
		Outputs.append(Output("hidden-update_savedGraphs-from-fileload","n_clicks"))
		return Outputs
	
	def get_OutputsUpdateBE(self):
	
		DebugMsg2("Inside def get_OutputsUpdateBE(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_table-from-refreshbtn", "n_clicks"))
		Outputs.append(Output("hidden-update_historyGraphs-from-refreshbtn", "n_clicks")),
		Outputs.append(Output("input_Secondary_Legends", "options"))
		return Outputs

	def get_InputsUpdateBE(self):

		DebugMsg2("Inside def get_InputsUpdateBE(self):")
		Inputs = list()
		Inputs.append(Input("hidden-page_refresh2", "n_clicks"))
		Inputs.append(Input('hidden-updateBE-from-filter', 'n_clicks'))
		Inputs.append(Input('hidden-updateBE-from-refreshbtn', 'n_clicks'))
		Inputs.append(Input('hidden-updateBE-from-reset', 'n_clicks'))
		return Inputs

	def callback_BE(self):
		DebugMsg2("Inside def callback_BE(self):")
		if self.GraphParams["Primary_Yaxis"] is not None and len(self.GraphParams['Primary_Yaxis'])>0:
			for col in ["Primary_Legends","Scatter_Labels","Secondary_Legends"]:
				if self.GraphParams[col] is None:
					self.GraphParams[col]=[]

			if  len(self.GraphParams["Primary_Yaxis"])>0:
				for df_index in self.df_indexes:
					if self.df[df_index] is None:
						continue
	#                DebugMsg2("First Load self.df[df_index] " + df_index ,self.df[df_index])
					DebugMsg(" 1 self.filtered_df[df_index].shape =" ,self.filtered_df[df_index].shape)
					self.plot_df[df_index] = self.extract_data(self.filtered_df[df_index], self.GlobalParams["NewColumns"])
					if self.isAggregated():
						DebugMsg("self.isAggregated")
						self.plot_df[df_index],extra=self.filter_sort_df(self.plot_df[df_index],self.GraphParams["FilterAgregatedData"].split("\n"),df_index)
				self.update_graph(self.GraphParams["PlotIndexes"])
			else:
				for df_index in self.df_indexes:
					if self.df[df_index] is None:
						continue
					self.plot_df[df_index]=self.filtered_df[df_index] 
			
			self.set_Graphid()
			if self.DataFile[self.default_df_index] is not None:
				self.updateMetadata("LastGraph",self.GraphParams,self.default_df_index)
				if self.DataFile[self.default_df_index] is not None:
					self.save_history(self.default_df_index)
		
		else:
			DebugMsg2("Initializing Figs")
			self.initialize_figs()
			for df_index in self.df_indexes:
				self.plot_df[df_index]=self.filtered_df[df_index]
		
		
		self.update_column_names()
		retval=[1,1]
		retval.append(MC.get_dropdown_values("Secondary_Legends"))
		return retval


	def get_OutputsRefreshBtn(self):


		DebugMsg2("Inside def get_OutputsRefreshBtn(self):")
		Outputs = list()
		Outputs.append(Output("hidden-updateBE-from-refreshbtn", "n_clicks"))
		return Outputs

	def get_InputsRefreshBtn(self):

		DebugMsg2("Inside def get_InputsRefreshBtn(self):")
		Inputs = list()
		Inputs.append(Input("refreshbtn", "n_clicks"))
		for txtbox in self.GraphParamsOrder:
			Inputs.append(State("input_{}".format(txtbox), "value"))
		Inputs.append(State("input_{}".format("Scatter_Labels"), "value"))
		Inputs.append(State("chk_PreAggregated", "value"))
		Inputs.append(State("select_plot_df_index", "value"))
		return Inputs

	def callback_refreshbtn(
		self,
		Xaxis,
		GraphType,
		Primary_Yaxis,
		Primary_Legends,
		Aggregate_Func,
		Secondary_Legends,
		Scatter_Labels,
		ShowPreAggregatedData,
		plot_df_indexes
	):
		self.GraphParams["Primary_Yaxis"] = Primary_Yaxis
		self.GraphParams["Xaxis"] = Xaxis
		self.GraphParams["GraphType"] = GraphType
		self.GraphParams["Primary_Legends"] = Primary_Legends
		self.GraphParams["Aggregate_Func"] = Aggregate_Func
		self.GraphParams["Secondary_Legends"] = Secondary_Legends
		self.GraphParams["Scatter_Labels"] = Scatter_Labels
		self.GraphParams["ShowPreAggregatedData"] = ShowPreAggregatedData
		self.GraphParams["PlotIndexes"] = plot_df_indexes
		return [1]


	def get_InputsTableColsUpdate(self):


		DebugMsg2("Inside def get_InputsTableColsUpdate(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_table-from-refreshbtn", "n_clicks"))
		Inputs.append(Input("hidden-update_table-from-colsupdate", "n_clicks"))
		return Inputs

	def get_OutputsTableColsUpdate(self):
		DebugMsg2("Inside def get_OutputsTableColsUpdate(self):")
		Outputs = list()
		Outputs.append(Output("table-paging-with-graph", "columns"))
		Outputs.append(Output("hidden-update_figure-from-refreshbtn", "n_clicks"))
		return Outputs
	
	def callback_TableColsUpdate(self):
	
		DebugMsg2("Inside def callback_TableColsUpdate(self):")
		DebugMsg("callback_TableColsUpdate")
		retval=[]
		retval.append([{"name": i, "id": i} for i in MC.GlobalParams['Datatable_columns']])
		retval.append(1)
		DebugMsg("retval callback_TableColsUpdate",retval)
		return retval


	def get_InputsFilterSortBy(self):


		DebugMsg2("Inside def get_InputsFilterSortBy(self):")
		Inputs = list()
		Inputs.append(Input("table-paging-with-graph", "sort_by")),
		return Inputs

	def get_OutputsFilterSortBy(self):

		DebugMsg2("Inside def get_OutputsFilterSortBy(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_filters-from-sortby", "n_clicks"))
		return Outputs
	
	def callback_FilterSortBy(self,sort_by):
	
		DebugMsg2("Inside def callback_FilterSortBy(self,sort_by):")
		filter= "SortBy:" + json.dumps(sort_by)
		self.update_filter(filter)
		retval=[1]
		return retval

	def get_InputsFilterTableQuery(self):

		DebugMsg2("Inside def get_InputsFilterTableQuery(self):")
		Inputs = list()
		Inputs.append(Input("table-paging-with-graph", "filter_query")),
		return Inputs

	def get_OutputsFilterTableQuery(self):

		DebugMsg2("Inside def get_OutputsFilterTableQuery(self):")
		Outputs = list()
		Outputs.append(Output("table-paging-with-graph", "filter_query"))
		Outputs.append(Output("hidden-update_filters-from-tablequery", "n_clicks"))
		return Outputs
	
	def callback_FilterTableQuery(self,filter_query):
	
		DebugMsg2("Inside def callback_FilterTableQuery(self,filter_query):")
		retval=[""]
		if not filter_query.isspace():
			filter= re.sub("([^=><!])=([^=])","\\1==\\2",filter_query)
			self.update_filter(filter)
			retval.append(1)
		else:
			retval.append(dash.no_update)
		return retval

	def get_InputsTextAreaFilter(self):

		DebugMsg2("Inside def get_InputsTextAreaFilter(self):")
		Inputs = list()
		Inputs.append(Input('textarea-filter', 'n_blur')),
		Inputs.append(State('textarea-filter', 'value'))
		return Inputs

	def get_OutputsTextAreaFilter(self):

		DebugMsg2("Inside def get_OutputsTextAreaFilter(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_filters-from-textareafilter", "n_clicks"))
		return Outputs
	
	def callback_TextAreaFilter(self,filter):
	
		DebugMsg2("Inside def callback_TextAreaFilter(self,filter):")
		self.update_filter(filter,append=False)
		retval=[1]
		return retval

	def get_InputsFilter(self):

		DebugMsg2("Inside def get_InputsFilter(self):")
		Inputs = list()
		Inputs.append(Input('hidden-update_filters-from-sortby', 'n_clicks')),
		Inputs.append(Input('hidden-update_filters-from-tablequery', 'n_clicks')),
		Inputs.append(Input('hidden-update_filters-from-textareafilter', 'n_clicks')),
		Inputs.append(Input('hidden-update_filters-from-previousops', 'n_clicks')),
		Inputs.append(Input("hidden-update-preaggregateddata", "n_clicks"))
		Inputs.append(Input("hidden-update_filters-from-clrfilters", "n_clicks"))
		Inputs.append(Input("hidden-update_filters-from-reset", "n_clicks"))
		return Inputs


	def get_OutputsFilter(self):


		DebugMsg2("Inside def get_OutputsFilter(self):")
		Outputs = list()
		Outputs.append(Output('textarea-filter', 'value'))
		Outputs.append(Output('hidden-updateBE-from-filter', 'n_clicks'))
		Outputs.append(Output("hidden-update_previousOpsValue", "n_clicks"))
		Outputs.append(Output("hidden-input_dropdown_vals-from-prevops", "n_clicks"))
		return Outputs
	
	def callback_Filter(self,updateFig=True):
	
		DebugMsg2("Inside def callback_Filter(self,updateFig=True):")
		DebugMsg("CallBAck Filter")
		retval=[]
		if self.showingPreAggregatedData():
			DebugMsg("self.GraphParams[Filters]= " ,self.GraphParams["Filters"])

			self.GraphParams["Filters"]=self.GraphParams["Filters"].strip()
			new_filters=self.GraphParams["Filters"]
			for df_index in self.df_indexes:
				if self.df[df_index] is None:
					continue
				DebugMsg("dF_index=",df_index)
				self.filtered_df[df_index] = self.df[df_index].copy()
				org_cols=set(self.filtered_df[df_index].columns)
				self.filtered_df[df_index],new_filters=self.filter_sort_df(self.filtered_df[df_index],self.GraphParams["Filters"].split("\n"),df_index)
				new_filters="\n".join(new_filters)
				self.GlobalParams["NewColumns"]=list(set(self.filtered_df[df_index].columns)- org_cols)
				DebugMsg(" 0 self.filtered_df[df_index].shape =" ,self.filtered_df[df_index].shape)
			DebugMsg("New_filters=",new_filters)
			self.GraphParams["Filters"]=new_filters 
			retval.append(self.GraphParams["Filters"])
		else:
			DebugMsg("self.GraphParams[FilterAgregatedData]= " ,self.GraphParams["FilterAgregatedData"])
			self.GraphParams["FilterAgregatedData"]=self.GraphParams["FilterAgregatedData"].strip()
			retval.append(self.GraphParams["FilterAgregatedData"])
		if updateFig:
			retval.append(1)
		else:
			retval.append(dash.no_update)
		retval.append(1)
		retval.append(1)
		return retval



	def get_InputsTableUpdate(self):



		DebugMsg2("Inside def get_InputsTableUpdate(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_table-from-refreshbtn", "n_clicks"))
		Inputs.append(Input("hidden-update_table-from-tableparams1", "n_clicks"))
		return Inputs

	def get_OutputsTableUpdate(self):

		DebugMsg2("Inside def get_OutputsTableUpdate(self):")
		Outputs = list()
		Outputs.append(Output("table-paging-with-graph", "data"))
		Outputs.append(Output("table-paging-with-graph", "style_data_conditional"))
		Outputs.append(Output("lbl_records", "children"))
		return Outputs
	
	def callback_TableUpdate(self):
	
		DebugMsg2("Inside def callback_TableUpdate(self):")
		DebugMsg("callback_TableUpdate")
		retval=[]
		self.update_table_df()
		retval.append(self.table_df.iloc[
			self.GlobalParams['TableCurrentPage'] * self.GlobalParams['TablePageSize'] : (self.GlobalParams['TableCurrentPage'] + 1) * self.GlobalParams['TablePageSize']
		].to_dict("records"))
		retval.append(self.create_conditional_style())
		retval.append(self.get_number_of_records())
		#DebugMsg("retval callback_TableUpdate",retval)
		return retval


	def get_InputsTableInputs(self):


		DebugMsg2("Inside def get_InputsTableInputs(self):")
		Inputs = list()
		Inputs.append(Input("table-paging-with-graph", "page_current")),
		Inputs.append(Input("table-paging-with-graph", "page_size")),
		Inputs.append(Input("select_data_df_index", "value"))
		Inputs.append(Input("hidden-update-preaggregateddata", "n_clicks"))
		return Inputs

	def get_OutputsTableInputs(self):

		DebugMsg2("Inside def get_OutputsTableInputs(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_table-from-tableparams1", "n_clicks"))
		return Outputs
	
	def callback_TableInputs(self,page_current,page_size,data_df_index):
	
		DebugMsg2("Inside def callback_TableInputs(self,page_current,page_size,data_df_index):")
		retval=[dash.no_update]
		self.GlobalParams['TableCurrentPage']=page_current
		self.GlobalParams['TablePageSize']=page_size
		self.GlobalParams['TableDfIndex']=data_df_index
		retval=[1]
		return retval


	def get_InputsPreAggregateData(self):


		DebugMsg2("Inside def get_InputsPreAggregateData(self):")
		Inputs = list()
		Inputs.append(Input("chk_PreAggregated", "value"))
		return Inputs

	def get_OutputsPreAggregateData(self):

		DebugMsg2("Inside def get_OutputsPreAggregateData(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update-preaggregateddata", "n_clicks"))
		return Outputs
	
	def callback_PreAggregateData(self,preAggregated):
	
		DebugMsg2("Inside def callback_PreAggregateData(self,preAggregated):")
		retval=[dash.no_update]
		self.GraphParams["ShowPreAggregatedData"]=preAggregated
		retval=[1]
		return retval


	def get_InputsPreviousOpsValue(self):


		DebugMsg2("Inside def get_InputsPreviousOpsValue(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_previousOpsValue", "n_clicks"))
		return Inputs

	def get_OutputsPreviousOpsValue(self):

		DebugMsg2("Inside def get_OutputsPreviousOpsValue(self):")
		Outputs = list()
		Outputs.append(Output("textarea-previous_ops", "value"))
		return Outputs
	
	def callback_PreviousOpsValue(self):
	
		DebugMsg2("Inside def callback_PreviousOpsValue(self):")
		retval=[]
		return_str=json.dumps(self.GraphParams['PreviousOperations'],indent=1)
	 #   return_str=return_str.replace("\\n","\n")
	 #   return_str=re.sub("([^\\\\])\"","\\1",return_str)
	 #   return_str=re.sub("\\\\\"","\"",return_str)
	 #   return_str=re.sub("^\[","",return_str)
	 #   return_str=re.sub("\]$","",return_str)
	 #   return_str=return_str.strip()
		retval.append(return_str)
		return retval



	def get_InputsFigUpdate(self):
		DebugMsg2("Inside def get_InputsFigUpdate(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_figure-from-refreshbtn", "n_clicks"))
		return Inputs

	def get_OutputsFigUpdate(self):

		DebugMsg2("Inside def get_OutputsFigUpdate(self):")
		Outputs = list()
		for group in self.groups:
			grpid = self.get_groupid(group)
			Outputs.append(
				Output(component_id="lbl_" + grpid, component_property="children")
			)
			Outputs.append(
				Output(component_id="fig_" + grpid, component_property="figure")
			)
		return Outputs
	
	def callback_FigUpdate(self):
	
		DebugMsg2("Inside def callback_FigUpdate(self):")
		retval=[]
		for group in self.groups:
			grpid = self.get_groupid(group)
			retval.append(json.dumps(self.GraphParams))
			if grpid not in self.figs:
				self.figs[grpid]=None
			if self.figs[grpid] is None:
				self.figs[grpid]=go.Figure()
			retval.append(self.figs[grpid])
		return retval

	def get_InputsLoadHistoricalGraph(self):

		DebugMsg2("Inside def get_InputsLoadHistoricalGraph(self):")
		Inputs = list()
		Inputs.append(Input("input_HistoricalgraphName", "value")),
		return Inputs

	def get_OutputsLoadHistoricalGraph(self):

		DebugMsg2("Inside def get_OutputsLoadHistoricalGraph(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_previousOps-from-history", "n_clicks"))
		Outputs.append(Output("hidden-update_dropdown_values-from-history","n_clicks"))
		return Outputs
	
	def callback_LoadHistoricalGraph(self,graphName):
	
		DebugMsg2("Inside def callback_LoadHistoricalGraph(self,graphName):")
		if graphName is not None:
			retval=[1,1]
			self.GraphParams=self.HistoricalGraphList[graphName].copy()
		else:
			retval=[dash.no_update,dash.no_update]
		return retval

	def get_InputsLoadNamedGraph(self):

		DebugMsg2("Inside def get_InputsLoadNamedGraph(self):")
		Inputs = list()
		Inputs.append(Input("input_graphName", "value")),
		return Inputs

	def get_OutputsLoadNamedGraph(self):

		DebugMsg2("Inside def get_OutputsLoadNamedGraph(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_savedGraphs-from-savedGraphs", "n_clicks")),
		Outputs.append(Output("input_HistoricalgraphName", "value")),
		return Outputs
	
	def callback_LoadNamedGraph(self,graphName):
	
		DebugMsg2("Inside def callback_LoadNamedGraph(self,graphName):")
		if graphName is not None:
			retval=[1,graphName]
			self.save_history(self.default_df_index,"SavedGraphs",graphName)
		else:
			retval=[dash.no_update,dash.no_update]
		return retval



	def get_OutputsUpdateDropdownValues(self):
		DebugMsg2("Inside def get_OutputsUpdateDropdownValues(self):")
		Outputs = list()
		for txtbox in self.GraphParamsOrder:
			Outputs.append(Output("input_{}".format(txtbox), "value"))
		Outputs.append(Output("input_{}".format("Scatter_Labels"), "value"))
		return Outputs

	def get_InputsUpdateDropdownValues(self):

		DebugMsg2("Inside def get_InputsUpdateDropdownValues(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_dropdown_values-from-history","n_clicks"))
		Inputs.append(Input("hidden-update_dropdown_values-from-reset","n_clicks"))
		Inputs.append(Input("hidden-input_vals-from-lastgraph","n_clicks"))
		return Inputs

	def callback_UpdateDropdownValues(self):

		DebugMsg2("Inside def callback_UpdateDropdownValues(self):")
		return self.update_inputs(True)

	def get_InputsUpdateBEpreviousOps(self):

		DebugMsg2("Inside def get_InputsUpdateBEpreviousOps(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_previousOps-from-history", "n_clicks")),
		return Inputs

	def get_OutputsUpdateBEpreviousOps(self):

		DebugMsg2("Inside def get_OutputsUpdateBEpreviousOps(self):")
		Outputs = list()
		Outputs.append(Output("hidden-update_filters-from-previousops", "n_clicks"))
		return Outputs
	
	def callback_UpdateBEpreviousOps(self):
	
		DebugMsg2("Inside def callback_UpdateBEpreviousOps(self):")
		retval=[1]
		for df_index in self.df_indexes:
			DebugMsg("callback_UpdateBEpreviousOps df_index",df_index)
			if self.df[df_index] is None:
				continue
			DebugMsg("callback_UpdateBEpreviousOps FirstLoad df shape" + "df_index=" + df_index ,self.df[df_index].shape)
			DebugMsg("self.GraphParams['PreviousOperations']" ,self.GraphParams['PreviousOperations'])
			if len(self.GraphParams['PreviousOperations'])> 0:
				for filter in self.GraphParams['PreviousOperations']:
					self.filter_sort_df(self.df[df_index],filter,df_index,False)
				DebugMsg("callback_UpdateBEpreviousOps df",self.df[df_index])
				self.filtered_df[df_index] = self.df[df_index].copy()
		return retval


	def get_InputsSavedGraphs(self):


		DebugMsg2("Inside def get_InputsSavedGraphs(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_savedGraphs-from-savebtn", "n_clicks")),
		Inputs.append(Input("hidden-update_savedGraphs-from-fileload", "n_clicks")),
		Inputs.append(Input("hidden-update_savedGraphs-from-savedGraphs", "n_clicks")),
		return Inputs

	def get_OutputsSavedGraphs(self):

		DebugMsg2("Inside def get_OutputsSavedGraphs(self):")
		Outputs = list()
		Outputs.append(Output("input_graphName", "options"))
		return Outputs
	
	def callback_SavedGraphs(self):
	
		DebugMsg2("Inside def callback_SavedGraphs(self):")
		retval=[]
		retval.append(MC.get_dropdown_values("SavedGraphNames"))
		return retval


	def get_InputsHistoricalGraphs(self):


		DebugMsg2("Inside def get_InputsHistoricalGraphs(self):")
		Inputs = list()
		Inputs.append(Input("hidden-update_historyGraphs-from-refreshbtn", "n_clicks")),
		Inputs.append(Input("hidden-update_historyGraphs-from-savebtn", "n_clicks")),
		Inputs.append(Input("hidden-update_historyGraphs-from-fileload", "n_clicks")),
		return Inputs

	def get_OutputsHistoricalGraphs(self):

		DebugMsg2("Inside def get_OutputsHistoricalGraphs(self):")
		Outputs = list()
		Outputs.append(Output("input_HistoricalgraphName", "options"))
		return Outputs
	
	def callback_HistoricalGraphs(self):
	
		DebugMsg2("Inside def callback_HistoricalGraphs(self):")
		retval=[]
		retval.append(MC.get_dropdown_values("HistoricalGraphNames"))
		return retval

	def get_Outputs(self):

		DebugMsg2("Inside def get_Outputs(self):")
		Outputs = list()
		Outputs.append(Output("table-paging-with-graph", "data"))
		for group in self.groups:
			grpid = self.get_groupid(group)
			Outputs.append(
				Output(component_id="lbl_" + grpid, component_property="children")
			)
			Outputs.append(
				Output(component_id="fig_" + grpid, component_property="figure")
			)
		Outputs.append(Output("table-paging-with-graph", "columns"))
		Outputs.append(Output("lbl_records", "children"))
		Outputs.append(Output('textarea-filter', 'value'))
		Outputs.append(Output('textarea-previous_ops', 'value'))
		for txtbox in self.GraphParamsOrder:
			Outputs.append(Output("input_{}".format(txtbox), "value"))
		Outputs.append(Output("input_{}".format("Scatter_Labels"), "value"))
		Outputs.append(Output("hidden-input_dropdown_vals","n_clicks"))
		Outputs.append(Output("input_graphName", "options"))
		Outputs.append(Output("input_HistoricalgraphName", "options"))
		Outputs.append(Output("input_Secondary_Legends", "options"))
		Outputs.append(Output("table-paging-with-graph", "filter_query"))
		Outputs.append(Output("table-paging-with-graph", "style_data_conditional"))
		return Outputs

	def get_Inputs(self):

		DebugMsg2("Inside def get_Inputs(self):")
		Inputs = list()
		Inputs.append(Input("refreshbtn", "n_clicks"))
		Inputs.append(Input("hidden-page_refresh2", "n_clicks"))
		Inputs.append(Input("input_graphName", "value")),
		Inputs.append(Input("input_HistoricalgraphName", "value")),
		Inputs.append(Input("table-paging-with-graph", "page_current")),
		Inputs.append(Input("table-paging-with-graph", "page_size")),
		Inputs.append(Input("table-paging-with-graph", "sort_by")),
		Inputs.append(Input('textarea-filter', 'n_blur')),
		Inputs.append(Input("table-paging-with-graph", "filter_query"))
		Inputs.append(Input("btn_clearFilters", "n_clicks"))
		Inputs.append(Input("chk_PreAggregated", "value"))
		Inputs.append(Input("select_data_df_index", "value"))
		Inputs.append(State("select_plot_df_index", "value"))
		Inputs.append(State('textarea-filter', 'value'))
		Inputs.append(State('textarea-previous_ops', 'value'))
		for txtbox in self.GraphParamsOrder:
			Inputs.append(State("input_{}".format(txtbox), "value"))
		Inputs.append(State("input_{}".format("Scatter_Labels"), "value"))
		return Inputs

	def refresh_callback(
		self,
		Xaxis,
		GraphType,
		Primary_Yaxis,
		Primary_Legends,
		Aggregate_Func,
		Secondary_Legends,
		Scatter_Labels, filter,
		ShowPreAggregatedData,
		refresh_df,
		FirstLoad,
		FilterUpdate,
		showGraph,
		showHistoricalGraph,
		plot_df_indexes,
		org_idx
	):
		DebugMsg2("FirstLoad=" + str(FirstLoad))
		DebugMsg("showGraph=" + str(showGraph))
		DebugMsg("refresh_df=" + str(refresh_df))
		DebugMsg("reset=" + str(self.reset))

		self.GlobalParams['ColumnsUpdated']=False
		DebugMsg("PrimaryLegends",Primary_Legends)
		DebugMsg("PrimaryLegends",Primary_Legends)
		retval = []

		if self.reset :
			pass
		elif showGraph is not None:
			self.GraphParams=self.SavedGraphList[showGraph].copy()
			self.update_aggregate()
			refresh_df=True
		elif showHistoricalGraph is not None:
			self.GraphParams=self.HistoricalGraphList[showHistoricalGraph].copy()
			self.update_aggregate()
			refresh_df=True
		elif FirstLoad and self.DataFile[self.default_df_index] is not None:
			self.read_lastGraphFile(self.default_df_index)
			DebugMsg2("Read First Load Graphparams=" ,self.GraphParams)
		elif refresh_df and Primary_Yaxis is not None:
			self.GraphParams["Primary_Yaxis"] = Primary_Yaxis
			self.GraphParams["Xaxis"] = Xaxis
			self.GraphParams["GraphType"] = GraphType
			self.GraphParams["Primary_Legends"] = Primary_Legends
			self.GraphParams["Aggregate_Func"] = Aggregate_Func
			self.GraphParams["Secondary_Legends"] = Secondary_Legends
			self.GraphParams["Scatter_Labels"] = Scatter_Labels
		elif FilterUpdate:
			if self.isAggregated() and (not self.showingPreAggregatedData()):
				self.GraphParams["FilterAgregatedData"] = filter
			else:
				self.GraphParams["Filters"] = filter

		
		for col in ["Primary_Legends","Scatter_Labels","Secondary_Legends"]:
			if self.GraphParams[col] is None:
				self.GraphParams[col]=[]


		if refresh_df:
			self.readFileInitDash(org_idx)

		for df_index in self.df_indexes:
			if self.df[df_index] is None:
				continue
			if FirstLoad :
				if len(self.GraphParams['PreviousOperations'])> 0:
					for filter in self.GraphParams['PreviousOperations']:
						self.filter_sort_df(self.df[df_index],filter,df_index,False)
					DebugMsg("FirstLoad2 df",self.df[df_index])
					self.filtered_df[df_index] = self.df[df_index].copy()

			new_cols=[]
			if refresh_df or FilterUpdate:
				DebugMsg("FilterUpdate=" + str(FilterUpdate))
				DebugMsg(self.GraphParams["Filters"])
				self.filtered_df[df_index] = self.df[df_index].copy()
			if refresh_df or FilterUpdate or FirstLoad:
				DebugMsg("self.df[" + df_index + "]", self.df[df_index])
				DebugMsg("self.filtered_df[" + df_index + "]", self.filtered_df[df_index])
				org_cols=set(self.filtered_df[df_index].columns)
				self.filtered_df[df_index],self.GraphParams["Filters"]=self.filter_sort_df(self.filtered_df[df_index],self.GraphParams["Filters"],df_index)
				new_cols=list(set(self.filtered_df[df_index].columns)- org_cols)
				self.plot_df[df_index]=self.filtered_df[df_index]
				DebugMsg("FilterUpdate plot_df=" + str(self.filtered_df[df_index].head(2)))


		if self.GraphParams["Primary_Yaxis"] is not None and len(self.GraphParams["Primary_Yaxis"])>0:
			for df_index in self.df_indexes:
				if self.df[df_index] is None:
					continue
#                DebugMsg2("First Load self.df[df_index] " + df_index ,self.df[df_index])
				self.plot_df[df_index] = self.extract_data(self.filtered_df[df_index], new_cols)
				if self.isAggregated():
					self.plot_df[df_index],extra=self.filter_sort_df(self.plot_df[df_index],self.GraphParams["FilterAgregatedData"],df_index)
			self.update_graph(plot_df_indexes)

		else:
			for df_index in self.df_indexes:
				if self.df[df_index] is None:
					continue
				self.plot_df[df_index]=self.filtered_df[df_index] 
		
		
		self.GlobalParams['Datatable_columns']=[]
		if self.plot_df[org_idx] is not None:
			for col in self.plot_df[org_idx].columns:
				if not str(col).startswith('#'):
					self.GlobalParams['Datatable_columns'].append(col)

		DebugMsg2("self.reset " , self.reset)
		if (not FirstLoad) and (not self.reset) and MC.DataFile[MC.default_df_index] is not None:
			MC.set_Graphid()
			MC.updateMetadata("LastGraph",MC.GraphParams,MC.default_df_index)
				
		self.reset=False

		for group in self.groups:
			grpid = self.get_groupid(group)
			retval.append(json.dumps(self.GraphParams))
			if grpid not in self.figs:
				self.figs[grpid]=None
			if self.figs[grpid] is None:
				self.figs[grpid]=go.Figure()
			retval.append(self.figs[grpid])
			if MC.DataFile[MC.current_df_index] is not None:
				self.save_history(MC.current_df_index)

		return retval

	


def get_str_dtype(df, col):

	


	DebugMsg2("Inside def get_str_dtype(df, col):")
	"""Return dtype of col in df"""
	dtypes = ['datetime', 'bool', 'int', 'float',
			'object', 'category']
	for d in dtypes:
		try:
			if d in str(df.dtypes.loc[col]).lower():
				return d
		except KeyError:
			return None

if __name__ == "__main__":
	argparser = argparse.ArgumentParser(description="Dashboard")
	argparser.add_argument(
		"-port", metavar="8050",  required=True ,type=int, 
		help="Port number to start server on")
	argparser.add_argument(
		"-file", metavar="datafile.csv", required=False,default=None, help="DashboardDataDir")
	argparser.add_argument(
		"-DashboardMode", action='store_true', help="DashboardDataDir")

	argparser.add_argument(
		"-sheet", metavar="Sheet1",  default=None,
		help="SheetName of the sheet in xlsx, used with -xlsx argument")

	argparser.add_argument(
		"-skiprows", metavar="0",  default=0,type=int, 
		help="Number of rowsd to be skipped from top while reading sheet")

	argparser.add_argument(
		"-treat_as_missing_value", metavar="MISSING,NA",  default=None,
		help="Values in data which should be treated as missing instead of a string")


	argparser.add_argument(
		"-isxlsx", action='store_true', help="If the input file is in xlsx form")

	argparser.add_argument(
		"-verbose", action='store_true', help="Enable detailed log")

	argparser.add_argument(
		"-debug", action='store_true', help="Enable Debugging mode")

	args = argparser.parse_args()
	debug=args.debug
	port=args.port

	Info("Start")
	if args.file is not None:
		assert os.path.exists(args.file)
		if (args.isxlsx):
			sheet_names=get_xlsx_sheet_names(args.file)
			if len(sheet_names) > 1:
				if args.sheet== None:
					raise ValueError("xlsx files contains more than 1 sheet i.e " + 
							str(sheet_names) + "\n" + "Please specfy sheetname using -sheetname argument")



	MC = Dashboard(datafile=args.file,isxlsx=args.isxlsx, sheetname=args.sheet, skiprows=args.skiprows, replace_with_nan=args.treat_as_missing_value ,DashboardMode=args.DashboardMode)

	app = MC.app


	@app.callback(MC.get_OutputsRefreshBtn(), MC.get_InputsRefreshBtn(),prevent_initial_call=True)
	def callback_refreshbtn(
		n_clicks1,
		Xaxis,
		GraphType,
		Primary_Yaxis,
		Primary_Legends,
		Aggregate_Func,
		Secondary_Legends,
		Scatter_Labels,
		ShowPreAggregatedData,
		plot_df_indexes
	):
		DebugMsg("Triggered inside callback_refreshbtn")

		return MC.callback_refreshbtn(
		Xaxis,
		GraphType,
		Primary_Yaxis,
		Primary_Legends,
		Aggregate_Func,
		Secondary_Legends,
		Scatter_Labels,
		ShowPreAggregatedData,
		plot_df_indexes
		)

	@app.callback(MC.get_OutputsUpdateBE(), MC.get_InputsUpdateBE(),prevent_initial_call=True)
	def callback_BE(n_clicks1,nclicks_2,n_clicks3,n_cliks4):
		DebugMsg2("###Callback callback_BE(n_clicks1,nclicks_2,n_clicks3):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_BE()


	@app.callback(MC.get_OutputsFigUpdate(), MC.get_InputsFigUpdate(),prevent_initial_call=True)
	def callback_FigUpdate(n_clicks1):
		DebugMsg2("###Callback callback_FigUpdate(n_clicks1):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_FigUpdate()

	@app.callback(MC.get_OutputsHistoricalGraphs(), MC.get_InputsHistoricalGraphs(),prevent_initial_call=True)
	def callback_HistoricalGraphs(n_clicks1,nclicks_2,n_clicks3):
		DebugMsg2("###Callback callback_HistoricalGraphs(n_clicks1,nclicks_2,n_clicks3):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_HistoricalGraphs()

	@app.callback(MC.get_OutputsLoadHistoricalGraph(), MC.get_InputsLoadHistoricalGraph(),prevent_initial_call=True)
	def callback_LoadHistoricalGraph(value):
		DebugMsg2("###Callback callback_LoadHistoricalGraph(value):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_LoadHistoricalGraph(value)

	@app.callback(MC.get_OutputsLoadNamedGraph(), MC.get_InputsLoadNamedGraph(),prevent_initial_call=True)
	def callback_LoadNamedGraph(value):
		DebugMsg2("###Callback callback_LoadNamedGraph(value):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_LoadNamedGraph(value)

	@app.callback(MC.get_OutputsSavedGraphs(), MC.get_InputsSavedGraphs(),prevent_initial_call=True)
	def callback_SavedGraphs(n_clicks1,nclicks_2,n_clicks3):
		DebugMsg2("###Callback callback_SavedGraphs(n_clicks1,nclicks_2,n_clicks3):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_SavedGraphs()

	@app.callback(MC.get_OutputsPreAggregateData(), MC.get_InputsPreAggregateData(),prevent_initial_call=True)
	def callback_PreAggregateData(preAggregated):
		DebugMsg2("###Callback callback_PreAggregateData(preAggregated):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_PreAggregateData(preAggregated)

	@app.callback(MC.get_OutputsPreviousOpsValue(), MC.get_InputsPreviousOpsValue(),prevent_initial_call=True)
	def callback_PreviousOpsValue(n_clicks):
		DebugMsg2("###Callback callback_PreviousOpsValue(n_clicks):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_PreviousOpsValue()


	@app.callback(MC.get_OutputsUpdateBEpreviousOps(), MC.get_InputsUpdateBEpreviousOps(),prevent_initial_call=True)
	def callback_UpdateBEpreviousOps(n_clicks):
		DebugMsg2("###Callback callback_UpdateBEpreviousOps(n_clicks):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_UpdateBEpreviousOps()

	@app.callback(MC.get_OutputsUpdateDropdownValues(), MC.get_InputsUpdateDropdownValues(),prevent_initial_call=True)
	def callback_UpdateDropdownValues(n_clicks,n_clicks2,n_clicks3):
		DebugMsg2("###Callback callback_UpdateDropdownValues(n_clicks,n_clicks2):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_UpdateDropdownValues()

	@app.callback(MC.get_OutputsTableInputs(), MC.get_InputsTableInputs(),prevent_initial_call=True)
	def callback_TableInputs(page_current,page_size,data_df_index,n_clicks1):
		DebugMsg2("###Callback callback_TableInputs(page_current,page_size,data_df_index,n_clicks1):", dash.callback_context.triggered[0]['prop_id'])
		DebugMsg("callback_TableInputs")
		return MC.callback_TableInputs(page_current,page_size,data_df_index)

	@app.callback(MC.get_OutputsTableUpdate(), MC.get_InputsTableUpdate(),prevent_initial_call=True)
	def callback_TableUpdate(nclicks_ref,nclicks_tab):
		DebugMsg2("###Callback callback_TableUpdate(nclicks_ref,nclicks_tab):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_TableUpdate()

	@app.callback(MC.get_OutputsTableColsUpdate(), MC.get_InputsTableColsUpdate(),prevent_initial_call=True)
	def callback_TableColsUpdate(nclicks_ref,nclicks_tab):
		DebugMsg2("###Callback callback_TableColsUpdate(nclicks_ref,nclicks_tab):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_TableColsUpdate()

	@app.callback(MC.get_OutputsFilterSortBy(), MC.get_InputsFilterSortBy(),prevent_initial_call=True)
	def callback_FilterSortBy(sort_by):
		DebugMsg2("###Callback callback_FilterSortBy(sort_by):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_FilterSortBy(sort_by)

	@app.callback(MC.get_OutputsFilterTableQuery(), MC.get_InputsFilterTableQuery(),prevent_initial_call=True)
	def callback_FilterTableQuery(filter_query):
		DebugMsg2("###Callback callback_FilterTableQuery(filter_query):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_FilterTableQuery(filter_query)

	@app.callback(MC.get_OutputsGraphOptions(),MC.get_InputsGraphOptions(),prevent_initial_call=True)
	def callback_GraphOptions(nclicks):
		DebugMsg2("###Callback callback_GraphOptions(nclicks):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_GraphOptions() 

	@app.callback(MC.get_OutputsTextAreaFilter(), MC.get_InputsTextAreaFilter(),prevent_initial_call=True)
	def callback_TextAreaFilter(n_blur,filter):
		DebugMsg2("###Callback callback_TextAreaFilter(n_blur,filter):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_TextAreaFilter(filter)

	@app.callback(MC.ClrFilter_Outputs(), MC.ClrFilter_Inputs(),prevent_initial_call=True)
	def ClrFilter_callback(n_blur):
		DebugMsg2("###Callback ClrFilter_callback(n_blur):", dash.callback_context.triggered[0]['prop_id'])
		return MC.ClrFilter_callback()

	@app.callback(MC.get_OutputsFilter(), MC.get_InputsFilter(),prevent_initial_call=True)
	def callback_Filter(n_clicks_sort,nclicks_fquery,nclicks_text,nclicks_prev_ops,update_preaggregated,nclicks_clrfilter,n_clicks_reset):
		DebugMsg2("###Callback callback_Filter(n_clicks_sort,nclicks_fquery,nclicks_text,nclicks_prev_ops,update_preaggregated,nclicks_clrfilter,n_clicks_reset):", dash.callback_context.triggered[0]['prop_id'])
		DebugMsg("Triggered callback_Filter" + str(dash.callback_context.triggered ))
		trig_id =  dash.callback_context.triggered[0]['prop_id'].split('.')
		if trig_id[0] =="hidden-update-preaggregateddata":
			return MC.callback_Filter(updateFig=False)
		else:
			return MC.callback_Filter()

   # @app.callback(MC.get_Outputs(), MC.get_Inputs(),prevent_initial_call=True)
	def update_output(
		n_clicks,n_clicks_refresh, graphname, historicalgraph,
		page_current, page_size, sort_by, advfltr_click,
		filter_query,
		click_clrfilter,
		chk_PreAggregatedData,
		data_df_index,
		plot_df_indexes,
		filter, 
		previous_ops,
		Xaxis,
		GraphType,
		Primary_Yaxis,
		Primary_Legends,
		Aggregate_Func,
		Secondary_Legends,
		Scatter_Labels,
		trig_id=None
	):

		DebugMsg("DEBUG1: update_output(",
	   str(n_clicks) + "," + str(graphname) + "," + str(historicalgraph) + "," + 
		 str(page_current) + ", " + str(page_size) + ", " + str(sort_by) + ", " + str(advfltr_click) + "," + 
		 str(filter_query) + "," + 
		 str(click_clrfilter) + "," + 
		 str(filter) + "," + 
		 str(Xaxis) + "," + 
		 str(GraphType) + "," + 
		 str(Primary_Yaxis) + "," + 
		 str(Primary_Legends) + "," + 
		 str(Aggregate_Func) + "," + 
		 str(Secondary_Legends) + "," + 
		 str(Scatter_Labels) +
		 ")"
		)


		FirstLoad=False
		refresh_df=False
		FilterUpdate=False
		showGraph=None
		showHistoricalGraph=None
		clearFilter=False
		PreAggrClick=False
		DebugMsg("page_current=" + str(page_current))
		DebugMsg("p=" + str(page_size))
		DebugMsg("sort_by=" + str(sort_by))
		DebugMsg("filter=" + str(filter))
		DebugMsg2("n_clicks=" + str(n_clicks))
		GraphType=GraphType
		retval=[]
		if trig_id is None:
			trig_id =  dash.callback_context.triggered[0]['prop_id'].split('.')
		DebugMsg(dash.callback_context.triggered)
		
		if trig_id[0] =="" :
			FirstLoad=True
		elif trig_id[0] =="hidden-page_refresh2" and (n_clicks_refresh is not None and n_clicks_refresh > 0):
			FirstLoad=True

		elif trig_id[0] =="refreshbtn" :
			refresh_df=True
			DebugMsg2("ab FirstLoad=" , FirstLoad)
			if n_clicks==0 :
				FirstLoad=True
		elif trig_id[0] =="chk_PreAggregated" or trig_id[0] =="select_data_df_index":
			PreAggrClick=True
		else:
			FilterUpdate=True

		if trig_id[0]=="table-paging-with-graph" :
			DebugMsg("update_inputs : Filter Query " + filter_query)
			if trig_id[1]=="filter_query":
				DebugMsg("update_inputs : Filter Query " + filter_query)
				if not filter_query.isspace():
					filter=filter.strip() 
					filter+= ("\n" + re.sub("([^=><!])=([^=])","\\1==\\2",filter_query))
			elif trig_id[1]=="sort_by":
				DebugMsg("update_inputs " + str(sort_by))
				if not str(sort_by).isspace():
					filter=filter.strip() 
					filter+= ("\nSortBy:" + json.dumps(sort_by))
			filter=filter.strip() 

		elif trig_id[0]=="btn_clearFilters":
			filter=""
			clearFilter=True
		elif trig_id[0]=="input_graphName":
			if trig_id[1]==None:
				raise dash.exceptions.PreventUpdate
			showGraph=graphname
		elif trig_id[0]=="input_HistoricalgraphName":
			if trig_id[1]==None:
				raise dash.exceptions.PreventUpdate
			showHistoricalGraph=historicalgraph
#        DebugMsg("#### DEBUG RETVAL", retval)

		DebugMsg2("2 FirstLoad=" , FirstLoad)
		MC.GraphParams['ShowPreAggregatedData']=chk_PreAggregatedData
		MC.update_aggregate(Aggregate_Func,new_update=True)

		DebugMsg("NITIn1234" + str(showGraph))

		if not PreAggrClick:
			t2=MC.refresh_callback( Xaxis, GraphType, Primary_Yaxis, Primary_Legends, Aggregate_Func, 
								Secondary_Legends, Scatter_Labels,  filter, chk_PreAggregatedData,
								refresh_df,
								FirstLoad,FilterUpdate,showGraph,showHistoricalGraph,plot_df_indexes,MC.current_df_index)
		else:
			t2=[dash.no_update,dash.no_update]
		t1=[MC.update_table(page_current, page_size,data_df_index)]

		t3=[[{"name": i, "id": i} for i in MC.GlobalParams['Datatable_columns']]]
		t4=[MC.get_number_of_records()]
		if (showGraph is not None) and MC.ControlMode:
			FirstLoad=True

		if (showHistoricalGraph is not None) and MC.ControlMode:
			FirstLoad=True

		t5=MC.update_inputs(FirstLoad)
		retval=t1  + t2+t3 + t4 

		if MC.aggregate and (not MC.GlobalParams['PreAggregatedData']):
			retval.append(MC.GraphParams['FilterAgregatedData'])
		else:
			retval.append(MC.GraphParams['Filters'])

		retval.append("\n".join(MC.GraphParams['PreviousOperations']).strip())

		retval=retval+ t5 ## Input boxes values

		if MC.GlobalParams['ColumnsUpdated']:
			retval.append(1)
		else:
			retval.append(1)

		MC.updateGraphList(MC.current_df_index)
		retval.append(MC.get_dropdown_values("SavedGraphNames"))
		retval.append(MC.get_dropdown_values("HistoricalGraphNames"))
		retval.append(MC.get_dropdown_values("Secondary_Legends"))

		if clearFilter:
			retval.append("")
		else:
			retval.append("")
			#retval.append(dash.no_update)
		retval.append(MC.create_conditional_style())
		return retval

	@app.callback(MC.get_Outputs2(), MC.get_Inputs2(),prevent_initial_call=True)
	def update_options( n_clicks,n_clicks2,_clicks2):
		DebugMsg2("###Callback update_options( n_clicks,n_clicks2):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_update_options(n_clicks,MC.current_df_index)

	@app.callback(MC.get_OutputsReset(), MC.get_InputsReset(),prevent_initial_call=True)
	def clearAl( n_clicks):
		DebugMsg2("###Callback clearAl( n_clicks):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callbackReset()

	@app.callback(MC.get_Outputs3(), MC.get_Inputs3(), prevent_initial_call=True)
	def func(n_clicks):
		DebugMsg2("###Callback func(n_clicks):", dash.callback_context.triggered[0]['prop_id'])
		return dcc.send_data_frame(
			MC.table_df.to_excel, "data.xlsx", sheet_name="Sheet1"
		)

#    @app.callback(MC.get_Outputs4(), MC.get_Inputs4())
#    def agg_chkbox(value):
#        return MC.refresh_callback4(value)

	@app.callback(MC.get_Outputs5(), MC.get_Inputs5(),prevent_initial_call=True)
	def saveGraph(clicks,value):
		DebugMsg2("###Callback saveGraph(clicks,value):", dash.callback_context.triggered[0]['prop_id'])
		return MC.refresh_callback5(clicks,value,MC.current_df_index)

	@app.callback(MC.get_Outputs_tab(), MC.get_Inputs_tab(),prevent_initial_call=True)
	def updateTab(tab):
		DebugMsg2("###Callback updateTab(tab):", dash.callback_context.triggered[0]['prop_id'])
		return MC.get_tab_containers_styles(tab)


	@app.callback(MC.get_OutputsLoadRecentFile(), MC.get_InputsLoadRecentFile(),prevent_initial_call=True)
	def loadRecentFile(input_value):
		DebugMsg2("###Callback loadRecentFile(input_value):", dash.callback_context.triggered[0]['prop_id'])
		df_index="1"
		value=None
		if input_value is None:
			MC.callbackLoadFile(input_value,"text",None,None,None,df_index,True)
			return [dash.no_update,1,1,"","text","<tab>","",""]
		temp=input_value.split("#")
		value=temp[0]
		fileFormat=temp[1]
		fileLoadOptions=temp[2]
		skiprows=temp[3]
		replaceWithNaN=temp[4]
		if value is not None:
			MC.callbackLoadFile(value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN,df_index,True)
			return [1,dash.no_update,1,value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN]

	@app.callback(MC.get_OutputsLoadFile(), MC.get_InputsLoadFile(),prevent_initial_call=True)
	def loadFile(clicks,input_value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN):
		DebugMsg2("###Callback loadFile(clicks,input_value,isxlsx,sheetname,skiprows,replaceWithNaN):", dash.callback_context.triggered[0]['prop_id'])
		if input_value is not None:
			df_index="1"
			MC.callbackLoadFile(input_value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN,df_index,False)
			return [MC.get_dropdown_values("input_recentlyLoadedFiles"), 1]

	@app.callback(MC.get_OutputsLoadFileValue(), MC.get_InputsLoadFileValue(),prevent_initial_call=True)
	def loadFileValue(clicks1,clicks2):
		DebugMsg2("###Callback loadFileValue(clicks1,clicks2):", dash.callback_context.triggered[0]['prop_id'])
		DebugMsg2("Datafile val",MC.DataFile['1'])
		if MC.DataFile['1'] is not None:
			return [MC.getDataFileName(MC.DataFile['1'])]
		else:
			return [dash.no_update]


	@app.callback(MC.get_OutputsLoadRecentFile2(), MC.get_InputsLoadRecentFile2(),prevent_initial_call=True)
	def loadRecentFile2(input_value):
		DebugMsg2("###Callback loadRecentFile2(input_value):", dash.callback_context.triggered[0]['prop_id'])
		df_index="2"
		value=None
		if input_value is None:
			MC.callbackLoadFile(input_value,"text",None,None,None,df_index,False)
			return [dash.no_update,1,"",[],"<tab>","",""]

		temp=input_value.split("#")
		value=temp[0]
		fileFormat=temp[1]
		fileLoadOptions=temp[2]
		skiprows=temp[3]
		replaceWithNaN=temp[4]
		if value is not None:
			MC.callbackLoadFile(value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN,df_index,False)
			return [1,1,value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN]

	@app.callback(MC.get_OutputsLoadFile2(), MC.get_InputsLoadFile2(),prevent_initial_call=True)
	def loadFile2(clicks,input_value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN):
		DebugMsg2("###Callback loadFile2(clicks,input_value,isxlsx,sheetname,skiprows,replaceWithNaN):", dash.callback_context.triggered[0]['prop_id'])
		if input_value is not None:
			df_index="2"
			MC.callbackLoadFile(input_value,fileFormat,fileLoadOptions,skiprows,replaceWithNaN,df_index,False)
			return [MC.get_dropdown_values("input_recentlyLoadedFiles"), 1]
			
	@app.callback(MC.get_OutputsLoadFileValue2(), MC.get_InputsLoadFileValue2(),prevent_initial_call=True)
	def loadFileValue2(clicks1,clicks2):
		DebugMsg2("###Callback loadFileValue2(clicks1,clicks2):", dash.callback_context.triggered[0]['prop_id'])
		if MC.DataFile['2'] is not None:
			DebugMsg2("loadFileValue2 updated value")
			return [MC.getDataFileName(MC.DataFile['2'])]
		else:
			return [dash.no_update]
			


	@app.callback(MC.get_reset_collectors_outputs(), MC.get_reset_collectors_inputs(),prevent_initial_call=True)
	def loadFilecomb(clicks1,clicks2):
		DebugMsg2("###Callback loadFilecomb(clicks1,clicks2):", dash.callback_context.triggered[0]['prop_id'])
		trig_id =  dash.callback_context.triggered[0]['prop_id'].split('.')
		if trig_id[0] =="hidden-reset_collector1" and clicks1 > 0:
			retval=1
			return [retval,1]
		elif trig_id[0] =="hidden-reset_collector2" and clicks2 > 0:
			retval=2
			return [retval,dash.no_update]
		return [dash.no_update]


	@app.callback(MC.get_Outputs_update_dropdown_options(), MC.get_Inputs_update_dropdown_options(),prevent_initial_call=True)
	def updateDropDownOptions(clicks1,clicks2):
		DebugMsg2("###Callback updateDropDownOptions(clicks1,clicks2):", dash.callback_context.triggered[0]['prop_id'])
		return [MC.get_dropdown_values("df_index"),MC.get_dropdown_values("plot_index")]

	@app.callback(MC.get_OutputsPageRefresh(),MC.get_InputsPageRefresh(),prevent_initial_call=True)
	def page_refresh(clicks):
		DebugMsg2("###############################")
		DebugMsg2("###Callback page_refresh(clicks):", dash.callback_context.triggered[0]['prop_id'])
		DebugMsg2("###############################")
		MC.loadLastLoadedFiles()
		#return [1,dash.no_update] 
		return [1,1] 

	@app.callback(MC.get_Outputsinput_fileformat(),MC.get_Inputsinput_fileformat(),prevent_initial_call=True)
	def fileFormat(fileformat,filepath):
		DebugMsg2("###Callback fileFormat(fileformat,filepath):", dash.callback_context.triggered[0]['prop_id'])
		DebugMsg2("fileformat",fileformat)
		if fileformat == "xlsx":
			return ["SheetName",get_xlsx_sheet_names(filepath,return_As_dropdown_options=True)]
		elif fileformat == "sqldb":
			DebugMsg2("eqrew",get_sqlite_table_names(filepath,return_As_dropdown_options=True))
			return ["TableName",get_sqlite_table_names(filepath,return_As_dropdown_options=True)]
		else :
			return ["Separator",MC.get_dropdown_values("AvailableSeparators")]
	
	@app.callback(MC.get_Outputsinput_fileformat2(),MC.get_Inputsinput_fileformat2(),prevent_initial_call=False)
	def fileFormat2(fileformat,filepath):
		DebugMsg2("###Callback fileFormat2(fileformat,filepath):", dash.callback_context.triggered[0]['prop_id'])
		if fileformat == "xlsx":
			return ["SheetName",get_xlsx_sheet_names(filepath,return_As_dropdown_options=True)]
		elif fileformat == "sqldb":
			DebugMsg2("eqrew",get_sqlite_table_names(filepath,return_As_dropdown_options=True))
			return ["TableName",get_sqlite_table_names(filepath,return_As_dropdown_options=True)]
		else :
			return ["Separator",MC.get_dropdown_values("AvailableSeparators")]


	@app.callback(MC.get_Outputs_update_dtype(),MC.get_Inputs_update_dtype(),prevent_initial_call=True)
	def update_dtypes(nclicks,cols,new_dtype,custom_datetime_fmt):
		DebugMsg2("###Callback update_dtypes(nclicks,cols,new_dtype,custom_datetime_fmt):", dash.callback_context.triggered[0]['prop_id'])
		trig_id =  dash.callback_context.triggered[0]['prop_id'].split('.')
		if trig_id[0] =="btn_apply_dtype" :
			MC.update_dtype(cols,new_dtype,custom_datetime_fmt)
		return [json.dumps(MC.get_dypes(cols))]
			

	@app.callback(MC.get_Outputs_previousOps(),MC.get_Inputs_previousOps(),prevent_initial_call=True)
	def update_previousOps(nblur,value):
		DebugMsg2("###Callback update_previousOps(nblur,value):", dash.callback_context.triggered[0]['prop_id'])
		MC.GraphParams['PreviousOperations']=json.loads(value)
		return [0]

	@app.callback(MC.get_Outputs_display_dtypes(),MC.get_Inputs_display_dtypes(),prevent_initial_call=True)
	def update_dtypes(nclicks):
		DebugMsg2("###Callback update_dtypes(nclicks):", dash.callback_context.triggered[0]['prop_id'])
		return [MC.get_dtypes_display()]


	@app.callback(MC.get_Outputs_custom_datetime(),MC.get_Inputs_custom_datetime(),prevent_initial_call=True)
	def update_dtypes(dtype):
		DebugMsg2("###Callback update_dtypes(dtype):", dash.callback_context.triggered[0]['prop_id'])
		if dtype=="datetime_custom_format":
			return [dict(display='inline-block',width='10%', verticalAlign='top')]
		else:
			return [dict(display='none',width='10%', verticalAlign='top')]


	@app.callback(MC.get_Outputs_args(),MC.get_Inputs_args(),prevent_initial_call=True)
	def update_args(arg1,arg2,arg3,arg4,arg5):
		DebugMsg2("###Callback update_args(arg1,arg2,arg3,arg4,arg5):", dash.callback_context.triggered[0]['prop_id'])
		return [[arg1,arg2,arg3,arg4,arg5]]

	@app.callback(MC.get_Outputs_applyFunc(),MC.get_Inputs_applyFunc(),prevent_initial_call=True)
	def update_func(nclicks,function,args):
		DebugMsg2("###Callback update_func(nclicks,function,args):", dash.callback_context.triggered[0]['prop_id'])
		return MC.callback_apply_func(function,args) 
		
	#@app.callback(Output('table-paging-with-graph', 'data'),
	#@app.callback(Output('table-paging-with-graph', 'data'),
#    @app.callback(Output('textarea-filter', 'value'),
#                [Input('col_select', 'value'),
#                Input('num_filter', 'value'),
#                Input('cat_filter', 'value'),
#                Input('str_filter', 'value'),
#                Input('bool_filter', 'value'),
#                Input('date_filter', 'start_date'),
#                Input('date_filter', 'end_date')])
	def filter_table(col, numbers, categories, string,
					bool_filter, start_date, end_date):
		if all([param is None for param in [col, numbers, categories,
											string, bool_filter, start_date,
											end_date]]):
			raise dash.exceptions.PreventUpdate
		sample_df=MC.plot_df
		if numbers and (get_str_dtype(sample_df, col) in ['int', 'float']):
			df = sample_df[sample_df[col].between(numbers[0], numbers[-1])]
#            return "numbers"
		elif categories and (get_str_dtype(sample_df, col) == 'category'):
			df = sample_df[sample_df[col].isin(categories)]
 #           return "cate"
		elif string and get_str_dtype(sample_df, col) == 'object':
			df = sample_df[sample_df[col].str.contains(string, case=False)]
  #          return "str"
		elif (bool_filter is not None) and (get_str_dtype(sample_df, col) == 'bool'):
			bool_filter = True if bool_filter == 'True' else False
			df = sample_df[sample_df[col] == bool_filter]
   #         return "bool"
		elif start_date and end_date and (get_str_dtype(sample_df, col) == 'datetime'):
			df = sample_df[sample_df[col].between(start_date, end_date)]
	#        return "datea"
		else:
			return str(get_str_dtype(sample_df, col)) + " # "  +str(col)
		return str(get_str_dtype(sample_df, col)) + " # "  +str(col)


	#waitress.serve(app.server, host="0.0.0.0", port=port,connection_limit=20)
	#update_output(1,None,0, 20, [], None,"",None,"",['mem_bucketed'],"Scatter",['CPU_TIME'],None,None,None,None,['refreshbtn', 'n_clicks'] )

	#app.run_server(debug=True,port=find_free_port())
	app.run_server(debug=True,port=port)
