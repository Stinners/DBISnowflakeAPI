library(R6)
library(httr2)

# This should only get constructed when a query is successful, any error handling should 
# be handled in the in th SnowflakeResultHandle class wrapping this

# Tracks the state associated with an in-flight Snowflake Query
SnowflakeCursor <- R6Class("SnowflakeCursor",
    public = list(
        statement_handle = NA_character_,

        # The partition info data returned with the first response
        partitions = NULL

        # The number of partitions which have actually been retrieved by this cursor
        # Note Snowflake uses 0 indexes for partitions
        n_partitions_retreived = 0 

        # Points to the next row to return the the current partition 
        partition_cursor = 1

        # The raw data returned by the most recent partition
        buffer = NULL

        metadata <- NULL

        initialize = function(resp) {
            json <- resp_body_json(resp)
            self$raw_resp <- resp

            self$statement_handle <- resp$statementHandle
            self$partitions <- json$resultSetMetaData$partitionInfo
            self$n_partitions_retreived <- 1 
            self$buffer <- json$data 
            self$metadata <- json$
        }

        get_rows = function(row_request) {
            # Figure out what subset of the data we want
            target_end_row <- row_request - partition_cursor + 1 
            max_end_row <- self$partitions[[self$n_partitions_retreived]]$rowCount
            end_row <- max(target_end_row, max_end_row)

            # get the raw rows we need to return 
            rows <- self$buffer[partition_cursor:end_row]

            fetch_more <- length(rows) != row_request 
            # Get the next partition 
            # reset the partition_cursor 
            # repeate get_rows with the new remaining row request
        }
    )
)
