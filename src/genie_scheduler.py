import re
from concurrent.futures import wait
import concurrent.futures


class GenieScheduler():
  
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
