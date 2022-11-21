from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED
import concurrent.futures
import sys
import traceback
from datetime import datetime
import networkx as nx
import pytz
import uuid
import json
from dataclasses import dataclass
import time
import re, graphviz, pydot
from IPython.display import display

from .genie_metadata import GenieMetadataHelper
from .genie_run_notebook import GenieRunNotebook
from .genie_scheduler import GenieScheduler

class Genie(GenieRunNotebook, GenieScheduler, GenieMetadataHelper):

    def __init__(self, spk, msspkutil, number_of_threads=4, cellTimeout=1800):
        self.tz = pytz.timezone('Asia/Kolkata')
        self.threads = number_of_threads
        self.zeros = []
        self.count = {}
        self.graph = nx.DiGraph()
        self.pipelineName = ""
        self.pipelinerunid = ''
        self.prop = {}
        # self.executeStatus = {}
        self.metadatarows = []
        self.subpipeline = 0
        self.fail = 0
        self.fail_nb = ""
        self.fail_msg = ""
        self.cellTimeoutInSeconds = cellTimeout #The child notebook will timeout if a code cell takes more time than this parameter
        self.pipelineMetadataDict = {} #It is set when running the master pipeline and the value is shared among the subpipelines
        self.spark = spk
        self.msspark = msspkutil

    def clearVariables(self):
        self.graph.clear()
        self.count.clear()
        self.zeros.clear()
        self.fail = 0
        self.fail_nb = ""
        self.fail_msg = ""
        
    
    #Only to be called by the master pipeline, and not the sub-pipelines
    #In the metadata table, for a record, the pipeline value should be the same as the name of sub-pipeline (not id of the sub-pipeline)
    def readMetadata(self,pipelineName,runStatus,prevRunID):
        # st = GenieMetadataHelper()
        rows = self.getStorageMetaData(self.spark,pipelineName,runStatus,prevRunID)
        self.pipelineMetadataDict[pipelineName] = rows
        for row in rows:
            if row.Type == 'pipeline':
                self.readMetadata(row.notebookname,runStatus,prevRunID)     

    # Validation checks for a graph 
    def checkGraph(self):
        flag = 0
        try:
            nx.find_cycle(self.graph, orientation="original")
        except nx.exception.NetworkXNoCycle as e:
            flag=1
        finally:
            if flag==0:
                raise Exception("Cycle Detected in pipeline. Pls remove cycle.")
        for node in self.graph.nodes():
            if node not in self.count.keys():
                raise Exception("Invalid dependency - "+node)

    #Creates a DAG for to run the notebooks in a specified order
    def initializeGraph(self):
        try:
            rows = self.metadatarows
            
            #Adding nodes to graph and initializing global variables
            for row in rows:
                self.graph.add_node(row.notebookid)
                self.count[row.notebookid] = 0
                self.prop[row.notebookid] = row
                # self.executeStatus[row.notebookid] = 0
            
            #Building edges in graph
            for row in rows:
                if row.Dependencies is not None and row.Dependencies != "":
                    deps = row.Dependencies.split(',')
                    for i in deps:
                        if i in self.count.keys():
                            self.graph.add_edge(i,row.notebookid)
                        else:
                            print("Invalid dependency = ",i," for notebook ",row.notebookid)
            
            #Cycle check
            self.checkGraph()
        except Exception as e:
            raise Exception(f"[{self.pipelineName}] Error in initializeGraph function. Error- "+str(e))
    
    #This function is used when we need to restart a pipeline midway (from failed notebooks)
    def traverse(self, node):
        if self.prop[node].status=='success':
            # self.executeStatus[node] = 1
            for child in self.graph.successors(node):
                self.count[child] +=1
                if self.count[child] == self.graph.in_degree(child):
                    self.traverse(child)
        else:
            self.zeros.append(node)
    
    #Zeros refer to the starting notebooks in a pipeline
    def initializeZeros(self,param):
        for node in self.graph.nodes():
            if self.graph.in_degree(node) == 0:
                if param=='start':
                    self.zeros.append(node)
                else:
                    self.traverse(node)
     
    def run_subpipeline(self,pipelineId,runStatus):
        print(f"Execution of {pipelineId} started")
        obj = Genie(self.spark,self.msspark,self.threads,self.cellTimeoutInSeconds)
        
        obj.subpipeline = 1
        obj.ec = self.ec
        obj.pipelineMetadataDict = self.pipelineMetadataDict

        obj.execute(runStatus,self.prop[pipelineId].notebookname,self.pipelinerunid)
        status = "success" if obj.fail==0 else "fail"
        if self.fail == 0 and obj.fail==1:
            self.fail_nb = obj.fail_nb
            self.fail_msg = obj.fail_msg
            self.fail = obj.fail
        print(f"Execution of {pipelineId} ended")
        return (pipelineId,status, obj.fail_msg)
    
    # def checkExecutionStatus():
    #     for a in self.executeStatus.keys():
    #         if self.executeStatus[a] == 0:
    #             start = datetime.now(self.tz)
    #             spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{a}','Unknown','{start}','{start}','{self.fail_msg}','{self.prop[a].Type}') """)

    #Main function
    
    def removeSingleQuotes(self,msg):
        return msg.replace("'","")

    def initializeThreadPool(self):
        self.ec = ThreadPoolExecutor(max_workers=self.threads)

    def getRunStatus(self,runStatus,triggerType):
        try:
            runStatus = runStatus.lower()
            triggerType = triggerType.lower()
            if runStatus=="":
                if triggerType == "ScheduleTrigger".lower() or triggerType=="":
                    runStatus = "start"
                elif triggerType == "Manual".lower():
                    runStatus = "restart"
                else:
                    raise Exception(f"Invalid parameter={triggerType} for triggerType. Acceptable values are 'Manual', 'Scheduled' or empty string.")
            return runStatus
        except Exception as e:
            raise Exception(f"[{self.pipelineName}] Error in getRunStatus function. Error- "+str(e)) from e      

    def getPrevRunID(self, pipelineName):
        try:
            prev = self.spark.sql(f"select pipelineRunID from genie.log where id='{pipelineName}' order by start desc limit 1")
            if prev.count() >0:
                return prev.head()[0]
            else:
                raise Exception(f"No previous records found for {pipelineName}. So cannot restart")
                return ""
        except Exception as e:
            raise Exception(f"[{pipelineName}] Error in getPrevRunID function. Error- "+str(e)) from e


    def visualizeGraph(self, pipeline):
        st = GenieMetadataHelper()
        self.metadatarows = st.getStorageMetaData(pipeline,"start","")
        self.initializeGraph()
        graph = nx.drawing.nx_pydot.to_pydot(self.graph)
        graph.set_rankdir("LR")
        gvz=graphviz.Source(graph.to_string())
        display(gvz)

    def execute(self, runStatus, pipelineName, runID="", triggerType=""):
        try:            
            start = datetime.now(self.tz)
            self.pipelineName = pipelineName
            self.pipelinerunid = uuid.uuid4() if runID=="" else runID          
            
            # The following commented line is for implicitly getting the value of "state" variable 
            # based on "runID" and "triggerType" parameters. Currently disabled.
            
            # runStatus = self.getRunStatus(runStatus,triggerType)
            
            prevRunID = ""
            # print(runStatus,triggerType)   
            if runStatus=="start" or runStatus == "restart":
                if self.subpipeline == 0:
                    if runStatus == "restart":
                        prevRunID = self.getPrevRunID(pipelineName)
                        if prevRunID != "":
                            self.pipelinerunid = prevRunID
                    print("Current RunID =",self.pipelinerunid)
                    self.readMetadata(pipelineName,runStatus,prevRunID)
                    self.initializeThreadPool()
                self.metadatarows = self.pipelineMetadataDict[pipelineName]
                self.clearVariables()
                self.initializeGraph()
                self.initializeZeros(runStatus)
            else:
                raise Exception(f"Invalid parameter={runStatus} for state. Acceptable values are 'Start', 'Restart' or empty string.")
            
            self.spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{pipelineName}','in progress','{start}',NULL,NULL,'pipeline') """)

            self.scheduler(runStatus)
            end = datetime.now(self.tz)
            status = "success" if self.fail==0 else "fail"
            if len(self.zeros) == 0:
                msg = "No notebooks executed. All were successful in last run."
                self.spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{self.pipelineName}','{status}','{start}','{end}','{msg}','pipeline') """)
                print(f"No notebooks executed for {self.pipelineName}. All were successful in last run.")
            else:
                self.spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{self.pipelineName}','{status}','{start}','{end}','{self.fail_msg}','pipeline') """)
        
        except Exception as e:
            raise Exception(f"[{self.pipelineName}] Error in Execute function. Error- "+str(e)) from e

        if self.fail == 1 and self.subpipeline==0:
            raise Exception(f"[{self.pipelineName}] Pls check genie.log table. An error occurred in the notebook/pipeline "+self.fail_nb+" with the exception- "+self.fail_msg)