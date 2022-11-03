#!/usr/bin/env python
# coding: utf-8

# ## GenieProcessExecution
# 
# 
# 

# In[4]:

#Sub pipelines are run recursively through another object of same class Genie
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
import re

@dataclass
class GenieMetadata:
    notebookid: str
    notebookname: str
    Dependencies: str
    Type: str
    status: str
    notebookparams: str
    retry: str
    retryintervalinseconds: str
    start: str

class GenieMetadataHelper:
    def metaDataParser(self, collection):
     return  [GenieMetadata(row['id'],row['name'],row['Dependencies'],row['Type'],row['status'],row['params'],row['retry'],row['retryintervalinseconds'],row['start']) for row in collection]
     
    def getStorageMetaData(self, spark, pipelineName, runStatus, prevRunID=""):
        if runStatus == "restart" :
            collection = spark.sql(f"""Select nb.id,nb.name,nb.Dependencies,nb.Type,l.status,nb.params, nb.retry, nb.retryintervalinseconds, l.start
                        from genie.metadata nb
                        left join (SELECT id,status,cast(start as timestamp),pipelineRunID,
                        ROW_NUMBER() OVER (PARTITION BY id ORDER BY cast(end as timestamp) DESC) ranked_order
                        FROM genie.log ) l
                        on nb.id = l.id and l.pipelineRunID = '{prevRunID}' and l.ranked_order = 1
                        WHERE nb.pipeline='{pipelineName}' and isActive=1
                        """).collect()
        else:
            collection = spark.sql(f"""Select nb.id,nb.name,nb.Dependencies,nb.Type,'success' as status,nb.params, nb.retry, nb.retryintervalinseconds,'' as start
                        from genie.metadata nb
                        WHERE nb.pipeline='{pipelineName}' and isActive=1
                        """).collect()
        metaDataRows = self.metaDataParser(collection)
        return metaDataRows

class Genie:

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
        st = GenieMetadataHelper()
        rows = st.getStorageMetaData(self.spark,pipelineName,runStatus,prevRunID)
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
    
    def removeSingleQuotes(self,msg):
        return msg.replace("'","")
    
    def run_notebook(self,notebookid):
        try:
            start,status,msg = "","",""
            if self.prop[notebookid].retry == "":
                self.prop[notebookid].retry = 0
            retry = 0 if self.prop[notebookid].retry == "" or self.prop[notebookid].retry is None else int(self.prop[notebookid].retry)
            retryinterval = 0 if self.prop[notebookid].retryintervalinseconds == "" or self.prop[notebookid].retryintervalinseconds is None else int(self.prop[notebookid].retryintervalinseconds)
            for i in range(-1,retry):
                try:
                    start = datetime.now(self.tz)
                    notebook = self.prop[notebookid].notebookname
                    print(f"[{start}] Execution of {notebookid} started")
                    self.spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{notebookid}','in progress','{start}',NULL,NULL,'notebook') """)
                    if self.prop[notebookid].notebookparams is not None and self.prop[notebookid].notebookparams != "":
                        msg = self.msspark.run(notebook,self.cellTimeoutInSeconds,json.loads(self.prop[notebookid].notebookparams))
                    else:
                        msg = self.msspark.run(notebook,self.cellTimeoutInSeconds)
                    status = "success"
                except BaseException as e:
                    status = "fail"
                    msg = str(e)[:512]
                finally:
                    end = datetime.now(self.tz)
                    #Escape error messages for sql
                    msg = self.removeSingleQuotes(msg)
                    self.spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{notebookid}','{status}','{start}','{end}','{msg}','notebook') """)
                    if status=="success":
                        break
                    if i!= retry:
                        time.sleep(retryinterval)    #For waiting time between refresh
                if status == "fail":
                    print(f"[{end}] Execution of {notebookid} failed")
            return(notebookid,status,msg)
        except Exception as e:
            raise Exception(f"[{notebookid}] Error in run_notebook function. Error- "+str(e))

    
    def scheduler(self,runStatus):
        try:
            futures ={}
            for root in self.zeros:
                if self.prop[root].Type == 'notebook':
                    futures[self.ec.submit(self.run_notebook,root)] = 1
                else:
                    futures[self.ec.submit(self.run_subpipeline,root,runStatus)] = 1
            while futures: #Queue of futures, children futures are added while parent futures are removed whenever a parent future completes
                done, not_done = wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for fut in done:
                    exception = fut.exception()
                    if exception is not None:
                        print("Exception received from future",fut," Exception-",str(exception) )
                        if self.fail == 0:
                            m = re.search(r"\[([A-Za-z0-9_]+)\]", str(exception))
                            if m is not None:
                                self.fail_nb = m.group(1)
                            self.fail_msg = self.removeSingleQuotes(str(exception))
                            self.fail = 1
                    else:    
                        nb_result = fut.result()
                        nb_name = nb_result[0]
                        if nb_result[1] == "success":
                            for child_obj in self.graph.successors(nb_name):
                                self.count[child_obj] +=1
                                if self.count[child_obj] == self.graph.in_degree(child_obj):
                                    if self.prop[child_obj].Type == 'notebook':
                                        futures[self.ec.submit(self.run_notebook,child_obj)] = 1
                                    else:
                                        futures[self.ec.submit(self.run_subpipeline,child_obj,runStatus)] = 1
                        else:
                            if self.fail == 0:
                                self.fail_nb = nb_result[0]
                                self.fail_msg = self.removeSingleQuotes(nb_result[2])
                                self.fail = 1
                        # self.executeStatus[nb_name] = 1
                    del futures[fut]
        except Exception as e:
            raise Exception(f"[{self.pipelineName}] Error in Scheduler function. Error- "+str(e))

    
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
        obj.metadatarows = st.getStorageMetaData(pipeline,"start","")
        obj.initializeGraph()
        graph = nx.drawing.nx_pydot.to_pydot(obj.graph)
        graph.set_rankdir("LR")
        gvz=graphviz.Source(graph.to_string())
        display(gvz)

    def execute(self, runStatus, pipelineName, runID="", triggerType=""):
        try:            
            start = datetime.now(self.tz)
            self.pipelineName = pipelineName
            self.pipelinerunid = uuid.uuid4() if runID=="" else runID
            
            
            
            runStatus = self.getRunStatus(runStatus,triggerType)
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