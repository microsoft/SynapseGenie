

USE Parallel_dedicated;
Go

create or alter procedure quickGenieLog  @runID nvarchar(50), @pipeline NVARCHAR(50)
AS  
    select TOP(2500) * from log
    where (@runID is null or @runID='' or Pipelinerunid = @runID)
    and (@pipeline is null or @pipeline='' or (@pipeline=pipeline or @pipeline = id) )
    order by start desc; 

GO