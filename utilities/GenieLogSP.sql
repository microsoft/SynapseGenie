-- Searches from the latest 2500 records. Can search by either runID or pipeline name. 
-- If no parameter is passed, it will return all records.
Exec quickGenieLog @runID='', @pipeline=''

-- Additional Info
-- The original logs can be accessed at parallel.dbo.log table
-- The view built on the logs and metadata is *parallel_dedicated.dbo.log*. The above stored procedure uses this view. (Recommended)