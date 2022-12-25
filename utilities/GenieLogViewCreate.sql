-- create DATABASE parallel_dedicated;
USE Parallel_dedicated;
Go

create or alter view log
AS  
with logCTE(PipelineRunID,Id,"Status","Start","End",ExecutionMsg,"Type",rank_no) 
as 
(
select top(20000) PipelineRunID,ID,"Status","Start","End",ExecutionMsg,"Type",
row_number() over (PARTITION by pipelinerunid,Id,start order by "end" desc) as rank_no
from openrowset(
    BULK 'https://hrdatalakeprod.blob.core.windows.net/hrsisynapsefs/data/common/genie/log/',             
    FORMAT='DELTA'         
    ) as log
order by "start" desc
) 
select a.*, metadata.Pipeline,metadata.Dependencies,  CONVERT(TIME(0), DATEADD(SS,DATEDIFF( second,"start",ISNULL( "end",GETDATE()) ),0), 108) AS "Duration" from
(
    select PipelineRunId,ID,Status,"Start","End",ExecutionMsg,"Type"
    from logCTE
    WHERE rank_no = 1
) a
left join 
openrowset(
    BULK 'https://hrdatalakeprod.blob.core.windows.net/hrsisynapsefs/data/common/genie/metadata/',             
    FORMAT='DELTA'         
) as metadata
on a.Id = metadata.id; 

GO