## Synapse Genie Framework
**Efficient Utilization of Spark Pools**

<img width="324" alt="GenieNewLamp" src="https://user-images.githubusercontent.com/45026856/204741903-7e972b55-e48a-46f0-8962-0cd6c606b461.png">

 ## What is Genie Framework?

Synapse Genie Framework improves Spark pool utilization by executing multiple Synapse notebooks on the same Spark pool instance. It considers the sequence and dependencies between notebook activities in an ETL pipeline. It results in higher usage of full cluster for resources available in a Spark pool.

## When to use Genie Framework?

Synapse Genie is helpful when you have a set of related notebooks that are part of a Synapse pipeline. In any group of notebooks, the workload varies from small to large. By executing such varied workloads on a single Spark pool instance, you can improve resource utilization and reduce Spark pool costs. 

It is also ideal to execute notebooks through Genie framework when the workloads are a mix of CPU and IO intensive operations.

During development of a data application in Synapse, we tend to develop multiple notebooks for a pipeline. This is likely due to multiple developers working on tasks independently or due to different transformation logic required to process different datasets. In such a case, the workload per notebook is of varied size. Some notebooks have a small workload and others have large workloads. When these notebooks are invoked from a pipeline, each notebook spins up a separate Spark pool and reserves these nodes till the execution is complete. This results in additional cost, execution time and inefficient usage of pool resources. Even a small notebook with a few lines of code reserves at least two nodes and incurs cost for spin up, execution and deallocation time. Genie utility tries to avoid this by attempting to run all notebooks within a single Spark context.  

## How does Genie Framework work?

Genie Framework is a metadata driven utility written in Python. It is implemented using threading (ThreadPoolExecutor module) and directed acyclic graph (Networkx library). It consists of a wrapper notebook, that reads metadata of notebooks and executes them within a single Spark session. Each notebook is invoked on a thread with MSSparkutils.run() command based on the available resources in the Spark pool. The dependencies between notebooks are understood and tracked through a directed acyclic graph.

Here is an example of metadata 

![image](https://user-images.githubusercontent.com/45026856/198976909-6bedb74e-07db-4241-ba25-21bd9e087bc2.png)

Sample graph screenshot that Genie builds for its execution.

![image](https://user-images.githubusercontent.com/45026856/198977019-ecdb3e78-ac76-480a-b731-e1c0ed9f13eb.png)


## What are the Capabilities of Genie Framework?

- Provides metadata-driven orchestration of notebooks.
- Supports easy onboarding to replace any Azure Data Factory/Synapse pipeline which has only notebook activities and sub-pipelines.
- Has a configurable retry/restart mechanism for failed notebooks.
- Logs error messages for future verification and run telemetry.
- Able to activate and/or remove notebooks from pipeline through metadata.
- Can pass parameters to notebooks like pipeline activity.


 **What are the benefits of Synapse Genie?**

- Increased Spark pool utilization and reduction in execution time and costs.
- Eliminates the need to configure node types and sizes per notebook.
- Enables notebook activation and/or removal from pipeline through metadata.
- Enables global views, user-defined functions (UDFs), and usage across notebooks.
- Prevents small notebooks from hogging resources unnecessarily.
- Puts a stop to long running notebooks from delaying other unrelated notebooks.

## What are the Output Statistics?
Early observations have shown a reduction of execution time for a pipeline by 30% to 50%. Of course, this will vary per workload. In a few cases, we reduced the size of our Spark pool cluster rather than opting for a reduction in just pipeline execution duration after onboarding to Genie framework. Over time, both choices translate to significant cost savings.

## Ok , I am in! How do I onboard?
Genie utility consists of a Python Wheel package and a Synapse notebook. Notebook information is stored as metadata for the framework and the wrapper notebook is to be invoked through a Synapse pipeline.

For detailed instructions about onboarding and installation of the framework to your Synapse workspace, please go through the [onboarding document](https://github.com/microsoft/SynapseGenie/blob/main/Onboarding.md) for Genie Framework.

## What else do I need to know before implementing ?

Genie Framework **is not** a direct release of Azure Synapse Analytics products and will not be officially supported by Synapse. It is a utility developed by the HR Data Insights team within the Microsoft Digital Employee Experience organization. This framework was developed as a way in which notebooks can be run on the same instance of Spark pool and can help higher resource utilization, thus reducing execution time and cost.

This Framework is not ideal for notebooks which use parallel threading heavily and run-on exceptionally large datasets (in TB’s). It is better suited when we have a mix of small and large workloads as mentioned in the "When to use Genie framework?" section above.


**Session isolation**
In Genie Framework , all Notebooks run with the same Spark context and Spark session by default. This causes any temporary views defined in one notebook to be available in another. At times, this may lead collision if two notebooks have different definitions but one definition is overwritten in the spark session. 

To avoid this, use a naming convention for views that denotes the notebook in which it is run. Alternatively, to isolate the spark session on a notebook from other notebooks in Genie framework , we can set the spark session on the first cell of the below notebook in the below manner

**For Python Notebook**:
![image](https://user-images.githubusercontent.com/45026856/198977106-fae91a5c-9cd2-4382-a836-8ed42ec01a5f.png)


**For Scala Notebook**:

![MicrosoftTeams-image (4)](https://user-images.githubusercontent.com/99250812/203032176-fa97e4ec-b181-4314-98bd-229a4e65881d.png)

Using the Synapse Genie utility can reduce execution time of your pipeline, there by reducing the overall costs. One can try and reduce the Spark pool node sizes to verify if the workload can be run on smaller cluster as all spark pool resources are available for the master genie notebook.

## Roadmap 

 - Provide Session isolation without code changes on the client side
 - Ability to trigger restart of a Genie instance run from a Datafactory\Synapse pipeline
 - Simplify Onboarding to Genie to Support Bulk onboarding

## Appendix

For more on [optimizing spark jobs on Synapse](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-performance#optimize-job-execution) 

Run [notebooks with threadpool](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/microsoft-spark-utilities?pivots=programming-language-python#notebook-utilities)

Code repository: [Synapse Genie GitHub](https://github.com/microsoft/SynapseGenie)

Steps to [Onboard Genie](https://github.com/microsoft/SynapseGenie/blob/main/Onboarding.md)
