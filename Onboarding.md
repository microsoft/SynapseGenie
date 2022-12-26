**Genie Onboarding :**

**Pre-Requisites:**

   1) Access to Synapse Workspace
   2) Access to Data Lake Storage Account, with Blob Storage Contributor access or above
   3) Download below files from the Repo location (https://github.com/microsoft/SynapseGenie/tree/main/utilities)
   
      i) Genie Package – Contains code for the framework
      
      ii) Genie Wrapper file - Template that contains the required code to run the framework
      
      iii) Schema files for tables required by the framework
      
      iv) SQL scripts for creating views based on logs (Requires serverless SQL)
    
    
**Steps to Onboard:**

 i) Files from the repo are required (https://github.com/microsoft/SynapseGenie/tree/main/utilities) .
 
 ii) Upload the genie package (.whl file) in the spark pool/workspace.
 <img width="718" alt="image" src="https://user-images.githubusercontent.com/99250812/209457623-b2ff8b2a-4f4d-45dd-b070-693eb34098bf.png">
 
 iii) Run one-time Genie metadata table creation script (**genie_schema.ipynb**). Store the metadata for pipelines in this table.
 
 iv) Also, run one-time scripts for creating tables and views for Genie logging (GenieLogViewCreate / GenieLogSP_create/ GenieLogSP).
 <img width="311" alt="image" src="https://user-images.githubusercontent.com/99250812/209457705-80342daa-8039-4f19-a850-add070e9ee53.png">
 
 v) Set the Spark configuration for the Wrapper notebook. (optional)
 
 <img width="459" alt="image" src="https://user-images.githubusercontent.com/99250812/209457735-d71f8d00-e07f-4b9b-aa48-a5abb7285fbc.png">

 vi) Configure the parameters for the framework in the Wrapper notebook.
 
 <img width="444" alt="image" src="https://user-images.githubusercontent.com/99250812/209457970-a8cf7a3c-5ab4-4e04-8838-274bcd9bad44.png">

 vii) Execute the framework with below command. Wrapper notebook containingthis execute command can be scheduled for daily through a Synapse pipeline. This Notebook executes all the other notebooks through the Genie framework
 
 <img width="379" alt="image" src="https://user-images.githubusercontent.com/99250812/209457975-5b867760-41ad-4f41-8229-b0c3e3bdc019.png">

    
**Metadata Onboard:**

Metadata can be loaded manually into the Genie Metadata table through insert scripts. Else, we can convert an existing pipeline into genie metadata through below steps

    i) Download all Json files of the Master pipeline (including Sub pipeline activities)
    ii) Place Json files in a Container of Data Lake Storage Account.
    iii) Modify the Values in the Script (metadata_generator.ipynb) accordingly and execute, 
                which will automatically publish the metadata into Genie Metadata Table.

**Genie Metadata Fields’ description**

<img width="554" alt="image" src="https://user-images.githubusercontent.com/99250812/203220807-10502672-27c9-44ec-b390-eda822ecf6c8.png">

**Wrapper notebook**

The framework execution happens by the Wrapper notebook. It consists of:

     i) Setting Spark configuration (optional)
     ii) Setting the parameters for the framework
     iii) Calling the framework
     iv) Visualizing the pipeline (optional)
     v) Finally, executing the framework.

**Guidance on Pool Usage**

   **Use Genie small pool (1 driver + 3 executors = 4 nodes):**

      If all notebooks can run individually on small nodes (dataset < 32 gb) and there are up to 8 notebooks that need to be run in parallel

   **Use Genie medium pool (1 driver + 3 executors = 4 nodes):**

      If all notebooks can run individually on small nodes (dataset < 32 gb) & there are up to 16 notebooks that need to be run in parallel

      If any of the notebook needs at least medium nodes (dataset > 32 gb) & there are up to 8 notebooks (small + Medium) that need to be run in parallel

   **Use Genie large pool (1 driver + 3 executors = 4 nodes):**

      If all notebooks can run individually on small\medium nodes (dataset > 32 gb) & there are up to 16-24 notebooks that need to be run in parallel.

      If any of the notebook needs at least large nodes (dataset > 64 gb) & there are up to 8-16 notebooks (small + Medium + Large) that need to be run in parallel

   **Important Note:** The above is an approximate of average workloads in our test. The actual notebooks that can be executed in parallel depends on the workload in the notebooks. And it would be beneficial to monitor metrics such as executor utilization and Executor JVM memory to tune the spark pool node size and counts.

**Advanced Tuning**

Two additional parameters are given for further customization – threads and cellTimeoutInSeconds.

**threads** – By default, threads is set equal to the number of executor cores. “threads” parameter can be tuned to a particular number depending on the requirements. <Integer>

**cellTimeoutInSeconds** – If any cell in a notebook in the pipeline takes more time than this value, that notebook will timeout.

**Points to Consider:**

   **i)** Delta tables Concurrent Update/Delete/Merge may result in exceptions when running from multiple notebooks.

   **ii)** Partition a table according to the conditions commonly used on the command can reduce conflicts significantly.

   **iii)** Ensure Sequential runs, move all Update statements into single notebook.

   **iv)** Using Retry mechanism

   **v)** Isolation – Spark session is shared across the Synapse notebooks. To prevent spark session sharing in a notebook and keep it isolated, one can add the following command at the beginning of the notebook –

   <img width="326" alt="image" src="https://user-images.githubusercontent.com/99250812/207006575-3803a496-b504-4e52-bc75-dcbe82b0e62e.png">

   **vi)** Ensure unique view name across all notebooks

   **vii)** Unique notebook ID

   **viii)** Only one Magic command is allowed in each cell of a notebook

   **ix)** It is advised to keep the language of caller notebook same as the notebook being called (callee) in %run cell.
