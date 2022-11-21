from dataclasses import dataclass

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

