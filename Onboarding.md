**Genie Onboarding :**

**Pre-Requisites:**

   1) Access to Synapse Workspace
   2) Access to Data Lake Storage Account, with Blob Storage Contributor access or above
   3) Download below files from the Repo location
      i) Genie Package – Contains code for the framework
      ii) Genie Wrapper file - Template that contains the required code to run the framework
      iii) Schema files for tables required by the framework
      iv) SQL scripts for creating views based on logs (Requires serverless SQL)
    
**Steps to Onboard:**

    i) Download the above-mentioned files from the repo.
    ii) Upload the genie package (.whl file) in the spark pool/workspace.
    iii) Run one-time Genie metadata table creation script (Schema.txt). Store the metadata for pipelines in this table.
    iv) Also, run one-time scripts for creating tables and views for Genie logging (GenieLogViewCreate / GenieLogSP_create/ GenieLogSP).
    v) Set the Spark configuration for the Wrapper notebook. (optional)
    vi) Configure the parameters for the framework in the Wrapper notebook.
    vii) Execute the framework.
    
**Metadata Onboard:**

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
