#! /usr/bin/arch -x86_64 /usr/bin/env python

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
from waitress import serve
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

print("############################################")
print("############################################")
print("############################################")
print("############################################")
debug=False

def DebugMsg(msg1,msg2=None):
    if debug:
        print(msg1)
        if msg2 is not None:
            print(msg2)


def get_xlsx_sheet_names(xlsx_file):
    with ZipFile(xlsx_file) as zipped_file:
        summary = zipped_file.open(r'xl/workbook.xml').read()
    soup = BeautifulSoup(summary, "html.parser")
    sheets = [sheet.get("name") for sheet in soup.find_all("sheet")]
    return sheets


# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options



class Metrics2:
    def __init__(self,  datafile,isxlsx=False,sheetname=None,skiprows=0,replace_with_nan=None, DashboardMode=False,):
        self.init_constants()
        self.fileMtimes = dict()
        self.df=None
        self.newXAxisColName = "#"
        self.DatatoDownload = None
        self.DataFile = {'Path': datafile,
                         'isXlsx':isxlsx,
                         'Sheet': sheetname,
                         'SkipRows': skiprows,
                         'ReplaceWithNan' : replace_with_nan
                        }
        self.LastGraphFile = "./LastGraphType"
        self.SavedGraphsFile = "./SavedGraphs"
        self.ControlMode=not DashboardMode


        self.GlobalParams={}
        self.GlobalParams['available_legends']=OrderedSet()
        self.GlobalParams['SecAxisTitles']=OrderedSet()
        self.GlobalParams['PrimaryAxisTitles']=OrderedSet()
        self.GlobalParams['LegendTitle']="Legend"
        self.GlobalParams['Datatable_columns']=[]
        self.GlobalParams['columns_updated']=False

        if os.path.exists(self.LastGraphFile):
            with open(self.LastGraphFile) as json_file:
                self.GraphParams = json.load(json_file)
                self.update_aggregate()
        else:
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
            self.GraphParams["SortAgregatedData"] = ""
            self.GraphParams["PreviousOperations"] = []

        self.update_aggregate()
        self.groups = [[json.dumps(self.GraphParams)]]

        self.DF_read_copy = dict()

        self.Dashboard()
        self.getGraphList()

        self.filtered_df = self.df.copy()
        self.plot_df=self.filtered_df
        self.initialize_figs()
        #self.update_graph()
        self.app = dash.Dash()
        self.app.layout = html.Div(self.layout())


    def getGraphList(self):           
        self.GraphList=dict()
        if os.path.exists(self.SavedGraphsFile):
            with open(self.SavedGraphsFile) as json_file:
                self.GraphList=json.load(json_file)  

    def get_Graphid(self):           
        x=self.GraphParams.copy()
        x['GraphId']=""
        x['Name']=""
        zlib.adler32(bytes(json.dumps(x),'UTF-8'))
        return id

    def update_dtypes(self,df1):           
        for col in self.dtypes:
            if col in df1.columns:
                if self.dtypes[col] == 'datetime':
                    df1[col]=pd.to_datetime(df1[col])
                else:
                    df1[col]=df1[col].astype(self.dtypes[col])
        return df1

    def init_constants(self):
        self.dtypes= {
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
            ["notin "],
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
        }
        self.GraphModeMap = {
            "Bar": "",
            "BarH": "",
            "BarStacked": "",
            "BarStackedH": "",
            "Pie": "",
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

        self.GraphParamsOrder2 = [
            "Xaxis",
            "GraphType",
            "Primary_Yaxis",
            "Primary_Legends",
            "Aggregate_Func"
        ]
        self.GraphParamsOrder = self.GraphParamsOrder2 + [ "Secondary_Legends"]


    def read_file_in_df(self,  FileInfo):
        mtime = os.path.getmtime(FileInfo['Path'])
        if FileInfo['Path'] not in self.fileMtimes:
            self.fileMtimes[FileInfo['Path']] = 0

        if mtime > self.fileMtimes[FileInfo['Path']]:
            print("Reading file " + str(FileInfo['Path'])  )
            self.fileMtimes[FileInfo['Path']] = mtime
            if FileInfo['isXlsx']:
                df=pd.read_excel(FileInfo['Path'],sheet_name=FileInfo['Sheet'],skiprows=FileInfo['SkipRows'])
            else:
                df=pd.read_csv(FileInfo['Path'], sep="\t")

            replace_dict=dict()
            if FileInfo['ReplaceWithNan'] is not None:
                for nan_value in FileInfo['ReplaceWithNan'].split(","):
                    replace_dict[nan_value]=np.nan
            df = df.replace(replace_dict)
            df = df.convert_dtypes(convert_integer=False,convert_floating=False,convert_string=False)
            df = df.replace({pd.NA: np.nan})

                
            self.DF_read_copy[FileInfo['Path']] = self.update_dtypes(df)
            
        else:
            print("File not changed")
        return self.DF_read_copy[FileInfo['Path']].copy()

    def Dashboard(self):
        if self.df is None:
            self.df = self.read_file_in_df(self.DataFile)

        self.figs = dict()

    def get_groupid(self, group):
        return "TopLevelID"
        # return "-".join(group)
    
    def hasDuplicates(self,df):
        s=set()
        i=0
        for x in df.index:
            i+=1
            s.add(str(list(df.loc[x])))
            if len(s) < i:
                return True
        return False

    def extract_data(self, df , keep_cols=[]):
        if len(self.GraphParams["Xaxis"]) ==0 or  ( '#index' in self.GraphParams["Xaxis"]):
            df['#index']=df.index.copy()
            self.GraphParams["Xaxis"]=['#index']
        DebugMsg("Test1",self.GraphParams['Xaxis'])
        DebugMsg("Test1",self.GraphParams['Primary_Legends'])
        filters_tmp_p = list(set(self.GraphParams["Xaxis"] + self.GraphParams["Primary_Legends"]))
        filters_tmp_p2=list(set(filters_tmp_p + keep_cols))

        DebugMsg("Test1 filters_tmp_p2",filters_tmp_p2)
        DebugMsg("Test1 filters_tmp_p",filters_tmp_p)
        DebugMsg("Test1 keep_cols",keep_cols)
        DebugMsg("Test1 Primary_Yaxis",self.GraphParams["Primary_Yaxis"])
        DebugMsg("Test1 Scatter_Labels",self.GraphParams["Scatter_Labels"])
        df1 = None
        if len(self.GraphParams["Primary_Yaxis"]) > 0:
            df_p = None
            if self.aggregate:
                reqd_cols=list(set(filters_tmp_p2 + self.GraphParams["Scatter_Labels"]+ self.GraphParams["Primary_Yaxis"]))
                for col in self.GraphParams["Primary_Legends"]:
                        df[col] = df[col].astype(str).replace("nan", "#blank")
                for col in (keep_cols + self.GraphParams["Scatter_Labels"] + self.GraphParams["Primary_Yaxis"]):
                    if col not in filters_tmp_p: 
                        df[col]=pd.to_numeric(df[col],errors='coerce')
                df_p = (
                    df[ reqd_cols].groupby(filters_tmp_p)
                    .agg(self.GraphParams['Aggregate_Func'])
                )
                df_p=df_p.reset_index()
                df_p=df_p[list(set(filters_tmp_p + self.GraphParams["Scatter_Labels"] + self.GraphParams["Primary_Yaxis"]))]
            else:
                if self.GraphParams['GraphType'] != 'Scatter' and self.hasDuplicates(df[filters_tmp_p]):
                    raise ValueError("Data contains duplicate values, Please use Aggregated Functions")

                df_p = df[
                    OrderedSet(
                        filters_tmp_p2
                        + self.GraphParams["Primary_Yaxis"]
                        + self.GraphParams["Scatter_Labels"]
                    )
                ]
                #pass
            df1 = df_p

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
                    elif ret_operator == 'isin' or ret_operator == 'notin':
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

    def create_eval_func(self,filter_expr):
        retval=filter_expr
        DebugMsg("Filter Expr init: " ,  retval)
        retval= re.sub("\{(\S*?)}(\s*=[^=])","\\1\\2",retval,1)
        retval= re.sub("\{(\S*?)}","df['\\1']",retval)
        retval= re.sub("\&\&", "&",retval)
        retval= re.sub("\|\|", "|",retval)
        retval= re.sub("\s*contains\s*(\S*)", ".str.contains('\\1')",retval)
        DebugMsg("Filter Expr: " ,  retval)
        
        return retval

    def get_legends(self,df,legend_cols):
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


    
    def update_fig(self, df, yaxis_cols, legend_cols, secondary_axis, fig):
        if len(yaxis_cols) == 0:
            return fig

        PrimaryLegendsColName,legends=self.get_legends(df,legend_cols)
        if PrimaryLegendsColName is not None:
            self.GlobalParams['LegendTitle']=PrimaryLegendsColName

        append_yaxis_name_in_legend=False
        if len(yaxis_cols) > 1:
            append_yaxis_name_in_legend=True

        PlotFunc = self.GraphTypeMap[self.GraphParams["GraphType"]]
        mode = self.GraphModeMap[self.GraphParams["GraphType"]]

        no_of_legends=len(yaxis_cols)
        if len(legends) > 0 :
            no_of_legends=len(legends)* len(yaxis_cols)
        col=0

        if (self.GraphParams["GraphType"] in  ["Pie"]):
            legend_names=[]
            self.GlobalParams['LegendTitle']=str(self.GraphParams["Xaxis"])
            for yaxis_col in yaxis_cols:
                if len(legends)==0:
                    legend_name=yaxis_col
                    legend_names.append(legend_name)
                for legend in legends:
                    legend_name=legend
                    if append_yaxis_name_in_legend:
                        if legend_name != "":
                            legend_name=yaxis_col + "<br>" + legend_name
                    legend_names.append(legend_name)
                    
            fig = make_subplots(rows=1, cols=no_of_legends, specs=[list(np.repeat([{'type':'domain'}],no_of_legends))]
            ,subplot_titles=tuple(legend_names))
            #fig = make_subplots(rows=1, cols=no_of_legends])
        self.GlobalParams['SecAxisTitles']=OrderedSet()
        self.GlobalParams['PrimaryAxisTitles']=OrderedSet()
        self.GlobalParams['available_legends']=OrderedSet()

        for yaxis_col in yaxis_cols:
            if len(legends)==0:
                legends=[""]
            first_legend=True
            for legend in legends:
                #print(legend)
                legend_name=legend
                secondary_axis=False
                if append_yaxis_name_in_legend:
                    if legend_name != "":
                        legend_name=yaxis_col + "-" + legend_name
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

                #print("NITinq2")
                #print(self.GlobalParams['SecAxisTitles'])
                #print("SecondaryLegends2")
                #print(self.GraphParams["Secondary_Legends"])
                #print(str(legend_name))
                #print(secondary_axis)
                #print()
                

                #print(legend)
                dftmp=df
                if PrimaryLegendsColName is not None: 
                    if legend=="#blank":
                       dftmp=df[df[PrimaryLegendsColName].isna()] 
                    else:
                       DebugMsg("legends",df[PrimaryLegendsColName])  
                       dftmp = df[df[PrimaryLegendsColName] == legend]
                if len(self.GraphParams["Xaxis"])>0:
                    dftmp =dftmp.sort_values( by=self.GraphParams["Xaxis"])
                #            print(dftmp.head())
                if (self.GraphParams["GraphType"] in  ["Pie"]):
                    col=col+1
                    x=dftmp[self.newXAxisColName]
                    y=dftmp[yaxis_col]
                    fig.add_trace(
                        PlotFunc( labels=x, values=y, hole=0.3,name=str(legend_name)),1,col
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
                    #if not self.aggregate:
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
        retlist=[]
        for ele in list1:
            retlist.append(str(ele))
        return retlist


    def initialize_figs(self):
        for group in self.groups:
            grpid = self.get_groupid(group)
            self.figs[grpid] = go.Figure()

    def update_graph(self):
        df=self.plot_df
        for group in self.groups:
            grpid = self.get_groupid(group)
            self.figs[grpid] = go.Figure()
            if df is None:
                continue
            self.figs[grpid] = make_subplots(specs=[[{"secondary_y": True}]])
            self.figs[grpid] =self.update_fig(
                df,
                self.GraphParams["Primary_Yaxis"],
                self.GraphParams["Primary_Legends"],
                False,
                self.figs[grpid],
            )

            self.figs[grpid].update_layout(
                hoverlabel=dict(namelength=-1),
                legend_title=self.GlobalParams["LegendTitle"],
                margin={"l": 2, "r": 2, "t": 40, "b": 40},
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
        print("updated_figs")
        return ""


    def filter_sort_df(self,df, Allfilter, update_prev=True):
        ## update_prev added to remove the recursive loop
        filters=Allfilter.split("\n")
        step_cnt=0
        update_previous_operations=False
        for filter in filters:
            step_cnt+=1
            print(str(step_cnt) + " " + str(self.create_eval_func(filter)))
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
                filter_expr=self.create_eval_func(filter)
                if (re.match("^\s*\S*\s*=",filter_expr) and (not re.match("^\s*\S*\s*=\s*=",filter_expr) )) :
                    df=pd.eval(filter_expr,target=df)
                    for col in df.columns:
                        if col not in self.df.columns:
                            self.df[col]=np.nan
                            self.GlobalParams['columns_updated']=True
                        self.df.loc[df.index]=df
                        update_previous_operations=True
                else:
                    print(df.dtypes)
                    df=df[pd.eval(filter_expr)]
        if update_previous_operations and update_prev:
            if len(self.GraphParams['PreviousOperations']) == 0  or self.GraphParams['PreviousOperations'][-1] != Allfilter:
                self.GraphParams['PreviousOperations'].append(Allfilter)
        return df


    def filter_sort_df2(self,dff, sort_by, filter):
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

    def update_table(self,page_current, page_size):
        retval=self.plot_df.iloc[
            page_current * page_size : (page_current + 1) * page_size
        ].to_dict("records")
        return retval

    def get_fig(self, grpid):
        return self.figs[grpid]

    def get_dropdown_values(self, type,df=None):
        if df is None:
            df=self.df
        list_of_dic = []
        if type == "GraphType":
            for col in self.GraphTypeMap:
                list_of_dic.append({"label": col, "value": col})
        elif type == "Secondary_Legends" :
            if self.GlobalParams['available_legends'] is not None:
                #print("NITINq3")
                #print(self.GlobalParams['available_legends'])
                for col in self.GlobalParams['available_legends']:
                    list_of_dic.append({"label": col, "value": col})
            else:
                list_of_dic.append({"label": "", "value": ""})
        elif type == "Aggregate_Func":
            list_of_dic.append({"label": "None", "value": ""})
            for col in self.aggregateFuncs:
                list_of_dic.append({"label": col, "value": col})
        elif type == "SavedGraphNames":
            for col in self.GraphList:
                list_of_dic.append({"label": col, "value": col})
        else :
            for col in df.columns:
                list_of_dic.append({"label": col, "value": col})
        return list_of_dic
    
    def layout(self):
        if self.aggregate:
            agg_val=["Yes"]
        else:
            agg_val=[]
        divs=[]
        divs.append(html.Div(id="hidden-div1", style={"display": "none",'width':'100%','border':'2px solid black'}))
        divs.append(html.Div(id="hidden-div2", style={"display": "none",'width':'100%','border':'2px solid black'}))
        divs.append(html.Button(id="hidden-input_dropdown_vals", style={"display": "none",'width':'100%','border':'2px solid black'}))
        if self.ControlMode:
            disp='none'
            disp='inline-table'
        else:
            disp='inline-table'
        divs.append(
            html.Div(
                [
                    html.Button("Refresh", id="refreshbtn", n_clicks=0,style=dict(display='inline-table',width='10%')),
                    html.Button("Clear All", id="btn_clearall", n_clicks=0,style=dict(display='inline-table',width='10%')),
                    html.Button("Download Excel", id="btn_download",style=dict(display='inline-table',width='10%')),
                    dcc.Dropdown(
                        id="input_graphName",
                        options=self.get_dropdown_values("SavedGraphNames"),
                        value=None,
                        multi=False,
                        style=dict(display=disp,width='60%')
                    ),
                    dcc.Download(id="download-dataframe-xlsx"),
                        
                ],
                style=dict(display='table',width='100%')
            )
        )

        #divs.append(html.Button("Download Excel", id="btn_download2"))
        #divs.append(html.Button("Download Excel", id="btn_download3"))

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

            new_divs.append( html.H3(txtbox,style=dict(display='inline-table',width='15%')))
            new_divs.append(
                        dcc.Dropdown(
                            id="input_{}".format(txtbox),
                            options=self.get_dropdown_values(txtbox),
                            value=def_value,
                            multi=multi,
                            clearable=clearable,
                            style=dict(display='inline-table',width='35%')
                        ),
            )
        if self.ControlMode:
            disp1="table"
        else:
            disp1='none'

        new_divs = html.Div(new_divs, style=dict(display=disp1,width='100%'))
        divs.append(new_divs)

        if self.ControlMode:
            disp='inline-table'
        else:
            disp='none'

        divs.append(
            html.Div(
                [
                    html.H3("Additional_Labels",style=dict(display=disp,width='15%')),
                    dcc.Dropdown(
                        id="input_{}".format("Scatter_Labels"),
                        options=self.get_dropdown_values("Scatter_Labels"),
                        value=None,
                        multi=True,
                        style=dict(display=disp,width='85%')
                    ),
                ],
                style=dict( display= "table",width='100%'),
            )
        )
        style=dict(display=disp,width='24%' )
        save_layout=[ 
            html.Div(
                [
                    dbc.Button("Save", id="btn_save",style=dict(display='inline-table',width='10%' )),
                    dcc.Input(id="input_save",type="text",placeholder="GraphName",style=dict(display='inline-table',width='25%' )),
                    dbc.Button("Clear Filters", id="btn_clearFilters",style=dict(display='inline-table',width='10%' )),
                ]
                + [html.Div([dcc.Textarea(
                id='textarea-filter',
                wrap="off",
                value='',
                style=dict(width='95%' )
                )],style=dict(display=disp,width='95%' )
                )]
                , style=dict(display=disp1,width='100%'),
            )
        ]
        #divs = divs + self.layout1() + save_layout + self.filter_layout()   +self.dataframe_layout()
        divs = divs + self.layout1() + save_layout   +self.dataframe_layout()
        divs.append(html.H2(" No of Records :  "))
        divs.append(html.H3(" - ",id="lbl_records"))
        ret=html.Div(divs,
                        style={"overflowX": "auto","overflowY": "auto"})
        return ret

    def filter_layout(self):
        return [
            html.Div(id='container_col_select',
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
        df=self.plot_df
        style=[]
        for col in df.columns:
            name_length = len(col)
            pixel = 50 + round(name_length*5)
            pixel = str(pixel) + "px"
            style.append({'if': {'column_id': col}, 'minWidth': pixel})
        return style

    def dataframe_layout(self):
        #for i in sorted(self.plot_df.columns):
           #print(i)
        if self.ControlMode:
            disp=None
        else:
            disp='none'
        columns=["","","","",""]
        if self.plot_df is not None:
            columns=self.plot_df.columns[:5]
        html_divs = [
            html.Div(
                className="row",
                children=[
                    html.Div(
                        dash_table.DataTable(
                            id="table-paging-with-graph",
                            columns=[{"name": i, "id": i} for i in sorted(columns)],
                            page_current=0,
                            page_size=20,
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
        retval = []
        for txtbox in self.GraphParamsOrder:
            if FirstLoad:
                retval.append(self.GraphParams[txtbox])
            else:
                retval.append(dash.no_update)
        if FirstLoad:
            retval.append(self.GraphParams["Scatter_Labels"])
        else:
            retval.append(dash.no_update)
        return retval

    def get_Outputs3(self):
        return Output("download-dataframe-xlsx", "data")

    def get_Inputs3(self):
        Inputs = list()
        Inputs.append(Input("btn_download", "n_clicks"))
        return Inputs

    def refresh_callback3(self):
        retval = []
        retval = dcc.send_data_frame(
            self.plot_df.to_excel, "data.xlsx", sheet_name="Sheet1"
        )
        return retval

    def get_Outputs4(self):
        return Output("hidden-div1", "children")

    def get_Inputs4(self):
        Inputs = list()
        Inputs.append(Input("input_Aggregate_Func", "value"))
        return Inputs

    def refresh_callback4(self, agg_value):
        retval = None
        if agg_value is None or agg_value == '' or agg_value==[]  :
            self.aggregate = False
            self.GraphParams["Aggregate"] = "No"
        else:
            self.aggregate = True
            self.GraphParams["Aggregate"] = "Yes"
        return retval

    def ClrFilter_Outputs(self):
        return Output("table-paging-with-graph", "filter_query")

    def ClrFilter_Inputs(self):
        Inputs = list()
        Inputs.append(Input("btn_clearFilters", "n_clicks"))
        return Inputs


    def ClrFilter_callback(self, n_clicks):
        retval=[]
        for txtbox in self.GraphParamsOrder:
            retval.append(dash.no_update)
        retval.append(dash.no_update)

        if n_clicks>0:
            if self.aggregate:
                self.GraphParams["FilterAgregatedData"] = ""
            else:
                self.GraphParams["Filters"] = ""
        return retval




    def get_Outputs5(self):
        return Output("hidden-div2", "children")

    def get_Inputs5(self):
        Inputs = list()
        Inputs.append(Input("btn_save", "n_clicks"))
        Inputs.append(State("input_save", "value"))
        return Inputs


    def refresh_callback5(self, n_clicks,GraphName):
        retval = ""
        if (n_clicks is not None) and (GraphName is not None):
            self.GraphParams['Name']=GraphName
            graphlist={}
            if os.path.exists(self.SavedGraphsFile):
                with open(self.SavedGraphsFile) as json_file:
                    graphlist=json.load(json_file)  
            graphlist[GraphName]=self.GraphParams
            with open(self.SavedGraphsFile, "w") as outfile:
                json.dump(graphlist,outfile)
        return retval


    def read_lastGraphFile(self):
        print("Reading " + self.LastGraphFile)
        with open(self.LastGraphFile) as json_file:
            self.GraphParams = json.load(json_file)
            self.update_aggregate()

    def update_aggregate(self):
        if self.GraphParams["Aggregate"] == "Yes":
            self.aggregate = True
        else:
            self.aggregate = False
        for param in self.GraphParams:
            if self.GraphParams[param] is None:
                self.GraphParams[param] = []


    def blank_to_nan(self,list1,unique=False):
        tmp=list()
        for x in list1 :
            if x=="#blank":
                x=math.nan
            tmp.append(x)
        if unique:
            tmp=list(OrderedSet(tmp))
        return tmp

    def nan_to_blank(self,list1,unique=False):
        if type(list1)==str:
            return list1
        try:
            if math.isnan(list1):
                return '#blank'
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
        Outputs = list()
        for txtbox in self.GraphParamsOrder2:
            Outputs.append(Output("input_{}".format(txtbox), "options"))
        Outputs.append(Output("input_{}".format("Scatter_Labels"), "options"))
        return Outputs

    def get_Inputs2(self):
        Inputs = list()
        Inputs.append(Input("hidden-input_dropdown_vals", "n_clicks"))
        return Inputs


    def get_OutputsClrAll(self):
        Outputs = list()
        Outputs.append(Output("input_{}".format("Scatter_Labels"), "options"))
        return Outputs

    def get_InputsClrAll(self):
        Inputs = list()
        Inputs.append(Input("btn_clearall", "n_clicks"))
        return Inputs

    def callback_update_options(self,n_clicks):
        retval = list()
        for txtbox in self.GraphParamsOrder2:
            if n_clicks > 0:
                retval.append(self.get_dropdown_values(txtbox,self.filtered_df))
            else:
                retval.append(dash.no_update)
        if n_clicks > 0:
            retval.append(self.get_dropdown_values("Scatter_Labels", self.filtered_df))
        else:
            retval.append(dash.no_update)
        return retval


    def get_Outputs(self):
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
        for txtbox in self.GraphParamsOrder:
            Outputs.append(Output("input_{}".format(txtbox), "value"))
        Outputs.append(Output("input_{}".format("Scatter_Labels"), "value"))
        Outputs.append(Output("hidden-input_dropdown_vals","n_clicks"))
        Outputs.append(Output("input_graphName", "options"))
        Outputs.append(Output("input_Secondary_Legends", "options"))
        Outputs.append(Output("table-paging-with-graph", "filter_query"))
        return Outputs

    def get_Inputs(self):
        Inputs = list()
        Inputs.append(Input("refreshbtn", "n_clicks"))
        Inputs.append(Input("input_graphName", "value")),
        Inputs.append(Input("table-paging-with-graph", "page_current")),
        Inputs.append(Input("table-paging-with-graph", "page_size")),
        Inputs.append(Input("table-paging-with-graph", "sort_by")),
        Inputs.append(Input('textarea-filter', 'n_blur')),
        Inputs.append(Input("table-paging-with-graph", "filter_query"))
        Inputs.append(Input("btn_clearFilters", "n_clicks"))
        Inputs.append(State('textarea-filter', 'value'))
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
        refresh_df,
        FirstLoad,
        FilterUpdate,
        showGraph
    ):
        print("FirstLoad=" + str(FirstLoad))
        print("showGraph=" + str(showGraph))

        self.GlobalParams['columns_updated']=False
        DebugMsg("PrimaryLegends",Primary_Legends)
        retval = []
        if showGraph is not None:
            self.GraphParams=self.GraphList[showGraph].copy()
            self.update_aggregate()
            refresh_df=True
            FirstLoad=True
        elif refresh_df and Primary_Yaxis is not None:
            self.GraphParams["Primary_Yaxis"] = Primary_Yaxis
            self.GraphParams["Xaxis"] = Xaxis
            self.GraphParams["GraphType"] = GraphType
            self.GraphParams["Primary_Legends"] = Primary_Legends
            self.GraphParams["Aggregate_Func"] = Aggregate_Func
            self.GraphParams["Secondary_Legends"] = Secondary_Legends
            self.GraphParams["Scatter_Labels"] = Scatter_Labels
        elif FirstLoad and os.path.exists(self.LastGraphFile):
            print("Reading First Load" )
            self.read_lastGraphFile()
        elif FilterUpdate:
            if self.aggregate:
                self.GraphParams["FilterAgregatedData"] = filter
                self.GraphParams["SortAgregatedData"] = ""
            else:
                self.GraphParams["Filters"] = filter
        #print("SecondaryLegends")
        #print(self.GraphParams["Secondary_Legends"])
        DebugMsg("Test0",self.GraphParams['Xaxis'])
        DebugMsg("Test0",self.GraphParams['Primary_Legends'])

        for col in ["Primary_Legends","Scatter_Labels","Secondary_Legends"]:
            if self.GraphParams[col] is None:
                self.GraphParams[col]=[]


        if refresh_df:
            self.Dashboard()
#            self.filtered_df = self.df.copy()

        if FirstLoad:
            DebugMsg("FirstLoaf df",self.df)
            for filter in self.GraphParams['PreviousOperations']:
                self.filter_sort_df(self.df,filter,False)
            DebugMsg("FirstLoad2 df",self.df)
            if len(self.GraphParams['PreviousOperations'])> 0:
                self.filtered_df = self.df.copy()


        if refresh_df or FilterUpdate:
            print("FilterUpdate=" + str(FilterUpdate))
            print(self.GraphParams["Filters"])
            self.filtered_df = self.df.copy()
        if refresh_df or FilterUpdate or FirstLoad:
            org_cols=set(self.filtered_df.columns)
            self.filtered_df=self.filter_sort_df(self.filtered_df,self.GraphParams["Filters"])
            new_cols=list(set(self.filtered_df.columns)- org_cols)
            self.plot_df=self.filtered_df
            print("FilterUpdate plot_df=" + str(self.filtered_df.head(2)))


        if self.GraphParams["Primary_Yaxis"] is not None and len(self.GraphParams["Primary_Yaxis"])>0:
#            pprint(self.GraphParams)
#            print("self.aggregate2: " + str(self.aggregate))
            DebugMsg("First Load PRevious Operations",self.filtered_df.columns)
            self.plot_df = self.extract_data(self.filtered_df, new_cols)
            if self.aggregate:
                self.plot_df=self.filter_sort_df(self.plot_df,self.GraphParams["FilterAgregatedData"])

            self.update_graph()
            #pprint(self.GraphParams)
            #print("self.aggregate1: " + str(self.aggregate))
        else:
            self.plot_df=self.filtered_df 
        
        self.GlobalParams['Datatable_columns']=[]
        if self.plot_df is not None:
            for col in self.plot_df.columns:
                #print(col)
                if not col.startswith('#'):
                    self.GlobalParams['Datatable_columns'].append(col)

        if not FirstLoad:
            with open(MC.LastGraphFile, "w") as outfile:
                json.dump(MC.GraphParams, outfile)

        for group in self.groups:
            grpid = self.get_groupid(group)
            retval.append(json.dumps(self.GraphParams))
            if grpid not in self.figs:
                self.figs[grpid]=None
            if self.figs[grpid] is None:
                self.figs[grpid]=go.Figure()
            retval.append(self.figs[grpid])
        return retval

    


def get_str_dtype(df, col):
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
        "-file", metavar="datafile.csv", required=True, help="DashboardDataDir")
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

    print("Start")
    assert os.path.exists(args.file)
    if (args.isxlsx):
        sheet_names=get_xlsx_sheet_names(args.file)
        if len(sheet_names) > 1:
            if args.sheet== None:
                raise ValueError("xlsx files contains more than 1 sheet i.e " + 
                        str(sheet_names) + "\n" + "Please specfy sheetname using -sheetname argument")



    MC = Metrics2(datafile=args.file,isxlsx=args.isxlsx, sheetname=args.sheet, skiprows=args.skiprows, replace_with_nan=args.treat_as_missing_value ,DashboardMode=args.DashboardMode)

    app = MC.app

    @app.callback(MC.get_Outputs(), MC.get_Inputs(),prevent_initial_callback=True)
    def update_output(
        n_clicks,graphname,
        page_current, page_size, sort_by, advfltr_click,
        filter_query,
        click_clrfilter,
        filter,
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
       str(n_clicks) + "," + str(graphname) + "," + 
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
        clearFilter=False
        #print("page_current=" + str(page_current))
        #print("p=" + str(page_size))
        #print("sort_by=" + str(sort_by))
        #print("filter=" + str(filter))
        #print("n_clicks=" + str(n_clicks))
        GraphType=GraphType
        retval=[]
        if trig_id is None:
            trig_id =  dash.callback_context.triggered[0]['prop_id'].split('.')
        print("trig_id=" + str(trig_id) + " Filter=" + filter)
        
        if trig_id[0] =="":
            FirstLoad=True
        elif trig_id[0] =="refreshbtn":
            refresh_df=True
            if n_clicks==0 :
                FirstLoad=True
        else:
            FilterUpdate=True

        if trig_id[0]=="table-paging-with-graph" :
            if trig_id[1]=="filter_query":
                print("update_inputs " + filter_query)
                if not filter_query.isspace():
                    filter=filter.strip() 
                    filter+= ("\n" + re.sub("([^=><])=([^=])","\\1==\\2",filter_query))
            elif trig_id[1]=="sort_by":
                print("update_inputs " + str(sort_by))
                if not str(sort_by).isspace():
                    filter=filter.strip() 
                    filter+= ("\nSortBy:" + json.dumps(sort_by))
            filter=filter.strip() 

#        elif trig_id[0]=="textarea-filter" and  trig_id[1]=="n_blur": 
#            if filter_query not in filter.split("\n"):
#                filter=filter.strip()
#                filter=filter + "\n" + re.sub("=[^=]","==",filter_query)
#                filter=filter.strip()
        elif trig_id[0]=="btn_clearFilters":
            filter=""
            clearFilter=True
        elif trig_id[0]=="input_graphName":
            if trig_id[1]==None:
                raise dash.exceptions.PreventUpdate
            showGraph=graphname

        print("NITIn1234" + str(showGraph))

        t2=MC.refresh_callback( Xaxis, GraphType, Primary_Yaxis, Primary_Legends, Aggregate_Func, 
                                Secondary_Legends, Scatter_Labels,  filter,refresh_df,
                                FirstLoad,FilterUpdate,showGraph)
        t1=[MC.update_table(page_current, page_size)]
        t3=[[{"name": i, "id": i} for i in sorted(MC.GlobalParams['Datatable_columns'])]]

        t4=[0]
        if MC.plot_df is not None:
            t4=[str(MC.plot_df.shape[0])]

        if (showGraph is not None) and MC.ControlMode:
            FirstLoad=True

        t5=MC.update_inputs(FirstLoad)
        retval=t1  + t2+t3 + t4 

        if FirstLoad:
            retval.append(MC.GraphParams['Filters'])
        else:
            retval.append(filter)    


        retval=retval+ t5 ## input boxes values
        if MC.GlobalParams['columns_updated']:
            retval.append(1)
        else:
            retval.append(1)
            #retval.append(dash.no_update)

        MC.getGraphList()
        retval.append(MC.get_dropdown_values("SavedGraphNames"))
        retval.append(MC.get_dropdown_values("Secondary_Legends"))
        if clearFilter:
            retval.append("")
        else:
            retval.append(dash.no_update)
        return retval

    @app.callback(MC.get_Outputs2(), MC.get_Inputs2(),prevent_initial_callback=True)
    def update_options( n_clicks):
        print("update oprions")
        return MC.callback_update_options(n_clicks)

    @app.callback(MC.get_Outputs3(), MC.get_Inputs3(), prevent_initial_call=True)
    def func(n_clicks):
        return dcc.send_data_frame(
            MC.plot_df.to_excel, "data.xlsx", sheet_name="Sheet1"
        )

    @app.callback(MC.get_Outputs4(), MC.get_Inputs4())
    def agg_chkbox(value):
        return MC.refresh_callback4(value)

    @app.callback(MC.get_Outputs5(), MC.get_Inputs5(),prevent_initial_callback=True)
    def saveGraph(clicks,value):
        return MC.refresh_callback5(clicks,value)

   # @app.callback(MC.Sec_Legends_Outputs(), MC.Sec_Legends_Inputs(),prevent_initial_callback=True)
   # def updateSecLegendsDropDown(value):
   #     return MC.Sec_Legends_Callback()

 #   @app.callback(MC.ClrFilter_Outputs(), MC.ClrFilter_Inputs(),prevent_initial_callback=True)
 #   def saveGraph(clicks):
 #       return MC.ClrFilter_callback(clicks)

#Adv_Filter  @app.callback([Output(x, 'style')
#Adv_Filter              for x in ['container_num_filter', 'container_str_filter',
#Adv_Filter                          'container_bool_filter', 'container_cat_filter',
#Adv_Filter                          'container_date_filter']],
#Adv_Filter              [Input('col_select', 'value')])
#Adv_Filter  def dispaly_relevant_filter_container(col):
#Adv_Filter      if col is None:
#Adv_Filter          return [{'display': 'none'} for i in range(5)]
#Adv_Filter      dtypes = [['int', 'float'], ['object'], ['bool'],
#Adv_Filter              ['category'], ['datetime']]
#Adv_Filter      result = [{'display': 'none'} if get_str_dtype(MC.plot_df, col) not in d
#Adv_Filter              else {'display': 'inline-block',
#Adv_Filter                      'margin-left': '7%',
#Adv_Filter                      'width': '400px'} for d in dtypes]
#Adv_Filter      return result
#Adv_Filter
#Adv_Filter  @app.callback(Output('rng_slider_vals', 'children'),
#Adv_Filter              [Input('num_filter', 'value')])
#Adv_Filter  def show_rng_slider_max_min(numbers):
#Adv_Filter      if numbers is None:
#Adv_Filter          raise dash.exceptions.PreventUpdate
#Adv_Filter      return 'from:' + ' to: '.join([str(numbers[0]), str(numbers[-1])])
#Adv_Filter
#Adv_Filter
#Adv_Filter  @app.callback([Output('num_filter', 'min'),
#Adv_Filter              Output('num_filter', 'max'),
#Adv_Filter              Output('num_filter', 'value')],
#Adv_Filter              [Input('col_select', 'value')])
#Adv_Filter  def set_rng_slider_max_min_val(column):
#Adv_Filter      sample_df=MC.plot_df
#Adv_Filter      if column is None:
#Adv_Filter          raise dash.exceptions.PreventUpdate
#Adv_Filter      if column and (get_str_dtype(sample_df, column) in ['int', 'float']):
#Adv_Filter          minimum = sample_df[column].min()
#Adv_Filter          maximum = sample_df[column].max()
#Adv_Filter          return minimum, maximum, [minimum, maximum]
#Adv_Filter      else:
#Adv_Filter          return None, None, None


  #  @app.callback([Output('btn_advfilter', 'n_clicks')],
  #              [State('textarea-filter', 'value')])
  #  def update_filter_textbox(filters):
  #      return [1]
        

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


  #  serve(app.server, host="0.0.0.0", port=8051)
    #update_output(1,None,0, 20, [], None,"",None,"",['mem_bucketed'],"Scatter",['CPU_TIME'],None,None,None,None,['refreshbtn', 'n_clicks'] )

    app.run_server(debug=True, port=8054)