from datetime import datetime
import time, json

class GenieRunNotebook:
    
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
                        try:
                            self.spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{notebookid}','in progress','{start}',NULL,NULL,'notebook') """)
                        except Exception as e:
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
                        try:
                            self.spark.sql(f"""Insert into genie.log values('{self.pipelinerunid}','{notebookid}','{status}','{start}','{end}','{msg}','notebook') """)
                        except Exception as e:
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
