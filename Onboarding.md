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

    Download the above-mentioned files from the repo.
    Upload the genie package (.whl file) in the spark pool/workspace.
    Run one-time Genie metadata table creation script (Schema.txt). Store the metadata for pipelines in this table.
    Also, run one-time scripts for creating tables and views for Genie logging (GenieLogViewCreate / GenieLogSP_create/ GenieLogSP).
    Set the Spark configuration for the Wrapper notebook. (optional)
    Configure the parameters for the framework in the Wrapper notebook.
    Execute the framework.
    
**Metadata Onboard:**

    Download all Json files of the Master pipeline (including Sub pipeline activities)
    Place Json files in a Container of Data Lake Storage Account.
    Modify the Values in the Script (metadata_generator.ipynb) accordingly and execute, 
                which will automatically publish the metadata into Genie Metadata Table.

**Genie Metadata Fields’ description**

<img width="554" alt="image" src="https://user-images.githubusercontent.com/99250812/203220807-10502672-27c9-44ec-b390-eda822ecf6c8.png">
