library("methods")
library("DBI")
library(httr)
library(jsonlite)

source("auth.R")

setClass("Snowflake",
    contains = "DBIDriver",
)

setClass("SnowflakeConnection",
    contains = "DBIConnection",
    slots = c(
        auth = "ANY",
        host = "character"
    )
)

# This will need to handle returning results from Snowflake including
# things like pagination and long running responses

setClass("SnowflakeResult",
    contains = "DBIResult",
    slots = c(
        conn = "SnowflakeConnection",
        statement = "character",
        params = "ANY",
        url = "character",

        # Keys track of how many rows have actually been returned by snowflake
        # will be -1 if the query has not actally been sent
        rows_fetched = "numeric",

        # Keeps track of how many rows have been requested through the DBI interface
        # Will be -1 if all rows have been requested
        cursor_requested = "numeric",

        # Queried to get the next part of the result set
        next_result_set_url = "character"
    ),

    prototype = list(
        params = NULL,
        rows_fetched = -1,
        cursor_requested = 0,
        next_result_set_url = NA_character_
    )
)

############### Driver Methods ###################

# We don't actally do anything for keypair auth
# Possibly we should run a SELECT 1; to verify that we've connected
setMethod("dbConnect", "Snowflake", function(drv, host, auth) {
    new("SnowflakeConnection",
        host = host,
        auth = auth
    )
})

############### Connection Mehods ################

# DBI separates sending queries and retreiving data, but Snowflake does not,
# so this methods does nothing but construct an empty SnowflakeResult object

init_connection <- function(
    conn, statement, params = NULL, immediate = NULL,
    warehouse = NULL, database = NULL, schema = NULL, role = NULL,
    timeout = NULL)
{
    new("SnowflakeResult", conn = conn, statement = statement, params = params)
}

############### Result Mehods ################


# TODO handle making repeated requests
# Make the request
setMethod("dbFetch", "SnowflakeResult",
    function(res, n = -1) {

        base_headers <- c(
          "Accept" = "application/json",
          "Content-Type" = "application/json"
        )

        headers <- set_auth_headers(res@conn@auth, base_headers)

        body <- list(
            statement = res@statement
        )

        response <- VERB("POST",
            url = res@conn@host,
            body = toJSON(body, auto_unbox = TRUE),
            add_headers(headers)
        )
    }
)
