## Synapse Genie Framework
**Efficient Utilization of Spark Pools**

![image](https://user-images.githubusercontent.com/45026856/198976270-7c8437db-ebf3-4590-86ba-4407edac39c1.png)

 ## What is Genie Framework?

Genie Framework improves Spark Pool utilization by executing multiple Synapse notebooks on the same spark pool instance. It takes into account the sequence and dependencies between Notebook activities in an ETL pipeline. It ensures efficient usage of full cluster of resources available in a spark pool .

## When to use Genie Framework?

Genie Framework is highly helpful when you have a set of related notebooks that are part of a Synapse pipeline. In any group of notebooks, the workload varies from small to large. By executing such varied workloads on a single spark pool instance we can improve the resource utilization and reduce spark pool costs.
It is also ideal to execute Notebooks through Genie framework when the workloads are a mix of CPU and IO intensive operations.

During development of a Data Application in Synapse, we tend to develop multiple notebooks for a pipeline. This is likely due to multiple developers working on tasks independently or due to different transformation logic required to process different datasets. In such a case the workload per notebook is mixed . Some notebooks have a small workload and others have large workloads. When these notebooks are invoked from a pipeline, each notebook spins up a separate spark pool and reserves these nodes till the execution is complete.  This results in additional cost, Execution time and inefficient usage of pool resources. Even a small notebook with few lines of code reserves at least 2 nodes and incurs cost for spin up, execution and de allocation time. 

## How does Genie Framework work?

Genie Framework is a metadata driven utility written in Python. It is Implemented using threading (ThreadPoolExecutor module) and Directed Acyclic Graph (Networkx library) . It consists of a wrapper notebook, that reads metadata of Notebook information and executes them within a single spark session . Each notebook is invoked on a thread with MSSparkutils.run() command based on the available resources in the spark pool. The dependencies between notebooks are understood and tracked through a Directed Acyclic Graph.

Here is an example of metadata 

![image](https://user-images.githubusercontent.com/45026856/198976909-6bedb74e-07db-4241-ba25-21bd9e087bc2.png)

Sample graph screenshot that Genie builds for its execution.

![image](https://user-images.githubusercontent.com/45026856/198977019-ecdb3e78-ac76-480a-b731-e1c0ed9f13eb.png)


## What are the Capabilities of Genie Framework?

- Metadata driven orchestration of notebooks 
- Supports easy onboarded to replace any ADF/Synapse pipeline which has only Notebook activities and Sub-Pipelines.
- Configurable retry/restart mechanism for failed notebooks.
- Easy debugging with execution trace.
- Error messages logged for future verification and run telemetry.
- Notebook activation/removal from pipeline through metadata.
- Ability to pass parameters to notebooks similar to pipeline activity


 **Benefits**

- Significant increase in spark pool utilization and reduction in execution times\Costs.
- Eliminates the need to configure node types and sizes per notebook.
- Enable notebook activation\removal from pipeline through metadata.
- Enable global views , UDFs etc., usage across notebooks.
- Prevent small notebooks from hogging resources unnecessarily.
- Prevent long running notebooks from delaying other unrelated notebooks.

## Ok , I am in! How do I onboard?
Genie Framework consists of a Python Wheel package and a Synapse Notebook.
Notebook information is stored as Metadata for the framework and the wrapper notebook is to be invoked through a Synapse pipeline. 
For detailed instructions about onboarding \installation of the framework to your Synapse workspace, please go through the onboarding document for Genie Framework.

## What should you watch out for ?

**Note**: Genie Framework is not an official tool of Azure Synapse Product. And will not be officially supported by Synapse. It is a sample utility developed independently and to be considered as an Example of how notebooks can be run on the same instance of spark pool.

This Framework is not ideal for notebooks which use parallel threading heavily and run-on very large datasets (in TB’s). It is better suited when we have a mix of small and large workloads as mentioned in the when to use Genie framework section.


**Session isolation**
In Genie Framework , All Notebooks run with the same spark context and spark session by default. This causes any temporary views defined in one notebook to be available in another. At times, this may lead collision if two notebooks have different definitions but this definition is overwritten in the spark session. To avoid this, use a naming convention for views that denotes the notebook in which it is run. Alternatively, to isolate the spark session on a notebook from other notebooks in Genie framework , we can set the spark session on the first cell of the below notebook in the below manner

**For Python Notebook**:
![image](https://user-images.githubusercontent.com/45026856/198977106-fae91a5c-9cd2-4382-a836-8ed42ec01a5f.png)


**For Scala Notebook**:

![MicrosoftTeams-image (4)](https://user-images.githubusercontent.com/99250812/203032176-fa97e4ec-b181-4314-98bd-229a4e65881d.png)

## Any statistics or cost savings 

In our test observations, when notebooks were run in parallel through Genie framework , we could observe a rise of 50 % in Executor utilization , when compared to running directly in a pipeline with isolated resources. 

The above tests also resulted in reduced execution time or reduction in spark pool size, that has reduced our spark pool costs significantly

**Note**: These statistics are only for a sample individual run , the actual savings may vary greatly based on your own workload .


## Appendix

For more on [optimizing spark jobs on Synapse](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-performance#optimize-job-execution) 

Run [notebooks with threadpool](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/microsoft-spark-utilities?pivots=programming-language-python#notebook-utilities)
