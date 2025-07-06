library(methods)
library(DBI)
library(R6)
library(httr)
library(jsonlite)


# This is purely just a marker class to implimentent DbiDriver
# and drive the dbConnect function
setClass("DBISnowflakeAPI",
         contains = "DBIDriver"
)

##======================================================================

# SnowflakeConnection holds the auth object and handles authenticating
# With Snowflake, the classes which actually handling creating and running
# queries will all host a reference to a SnowflakeConnection
setClass("SnowflakeConnection",
     contains = "DBIConnection",
     slots = c(

        # auth is an R6 class, so all queries crated by a connection share a reference
        # to a single auth object and we can refresh tokens in a single central place
        auth = "ANY",
        host = "character"
    )
)

setGeneric("host<-", function(x, value) standardGeneric("host<-"))
setMethod("host<-", "SnowflakeConnection", function(x, value) {
    x@host <- value
    x
})

##======================================================================

# Holds information about a query while it being built
setClass("SnowflakeQuery",
    slots = c(
        conn = "SnowflakeConnection",
        statement = "character",
        params = "list"
    ),
    prototype = list(
        params = list()
    )
)

##======================================================================

# Represents the results for a query which has been sent, can be used to
# actually fetch results from Snowflake
# Handles things like long running queries and result paging
setClass("SnowflakeResultHandle",
    slots = c(
        query = "SnowflakeQuery",
        cursor = "ANY"               # A SnowflakeCursor R6 object
    )
)

##======================================================================

# Tracks the state associated with an in-flight Snowflake Query
SnowflakeCursor <- R6Class("SnowflakeCursor", list(
        next_result_url = NA_character_,
        rows_fetched = 0,
        rows_requested = 0,
        buffer = list()
    )
)
