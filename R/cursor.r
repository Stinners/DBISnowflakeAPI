library(R6)
library(httr2)
library(chron)
library(rlang)

# This should only get constructed when a query is successful, any error handling should 
# be handled in the in th SnowflakeResultHandle class wrapping this

# Tracks the state associated with an in-flight Snowflake Query
SnowflakeCursor <- R6Class("SnowflakeCursor",
    public = list(
        host = NA_character_,

        # The partition info data returned with the first response
        # This is a list which includes the number of entries for each partition
        partitions = list(),

        # The number of partitions which have actually been retrieved by this cursor
        # Note Snowflake uses 0 indexes for partitions
        n_partitions_retreived = 0,

        # Points to the next row to return the the current partition 
        partition_cursor = 1,

        # The raw data returned by the most recent partition
        buffer = NULL,

        # The metadata returned in the first reponse
        # Includes: numRows, format, partionInfo and rowType fields
        metadata = list(),

        initialize = function(query, resp) {
            json <- resp_body_json(resp)

            self$host <- query@conn@host
            self$partitions <- json$resultSetMetaData$partitionInfo
            self$n_partitions_retreived <- 1 
            self$buffer <- json$data 
            self$metadata <- json$resultSetMetaData$rowType
        },

        # use -1 to indicate fetching all rows
        get_rows = function(n_rows = -1) {
            raw_rows <- list()
           
            # Fetch as many partitons as we need to satisy the row request
            repeat {
                # Update how many rows we still need to fetch 
                if (n_rows != -1) {
                    to_fetch <- n_rows - length(raw_rows) 
                }
                else {
                    to_fetch <- -1
                }

                partition_rows <- private$get_raw_rows_from_partition(to_fetch)
                raw_rows <- c(raw_rows, partition_rows)

                # Check if we need to fetch the next partition
                last_partition <- self$n_partitions_retreived == length(self$partitonsInfo)
                done_reading <- n_rows != -1 && length(raw_rows) >= n_rows

                if (last_partition || done_reading) {
                    break
                }
            }
            # Loop and fetch more partitions until we've satisified the row request
            # each time we get a partition add the rows the raw_rows list

            private$make_dataframe(raw_rows)
        }
    ),

    private = list(

        get_raw_rows_from_partition = function(n_rows) {
            max_end_row <- length(self$buffer)

            # Get Everything
            if (is.na(n_rows) || n_rows == -1) {
                end_row <- max_end_row
            }
            else {
                target_end_row <- n_rows - self$partition_cursor + 1 
                end_row <- min(target_end_row, max_end_row)
            }

            raw_rows <- self$buffer[self$partition_cursor:end_row]
            self$partition_cursor <- self$partition_cursor + length(raw_rows)

            raw_rows
        },


        # TODO: add proxy information
        get_next_partition = function() {
            resp <- request(self$host) |>
                req_url_path(paste("api/v2/statements/", self$statementHandle)) |> 
                req_url_query(partition=self$n_partitions_retreived) |>
                req_headers(!!!headers) |>
                req_perform()

            self$partition_cursor <- 1
            self$n_partitions_retreived <- self$n_partitions_retreived + 1 
            self$buffer <- resp_body_json(resp)$data
        },


        # TODO: make sure this covers the full range of Snowflake types
        get_type_converter = function(row_metadata) {
            convert <- switch(row_metadata$type,
                "text" = as.character,
                "fixed" = as.integer,
                "date" = function(x) as.Date(as.integer(x)),
                # This hasn't been tested with real data
                "timestamp_ntz" = as.chron,
                abort(paste("Unknown type ", row_metadata$type))
            )

            # There must be a better way to handle nulls
            function(val) {
                if (is.null(val[[1]])) {
                    NA 
                } else {
                    convert(val)
                }
            }
        },


        make_dataframe = function(rows) {
            df <- as.data.frame(do.call(rbind, rows), stringsAsFactors = FALSE)
            colnames(df) <- lapply(self$metadata, function(row) row$name )

            # type conversions 
            for (i in seq_along(self$metadata)) {
                type_converter <- private$get_type_converter(self$metadata[[i]])
                df[[i]] = type_converter(df[[i]])
            }
            df
        }

    )
)
