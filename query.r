library(R6)

source("connection.r")

# This class backs the DBIResult object - we need a way to transition between 
# SnowflakeQuery and SnowflakeResultHandle depending on the state 
# of the query - while still exposing all the DBI methods

# We need to do this because DBI makes no distinction between a query before 
# and after submitting - it expects that we can write code like:

# query <- dbSendQuery(conn, statement)
# dbBind(query, bindParams) 
# dbFetch(query)

# Where the db backend manages the mutable state of the query. But the Snowflake 
# API doesn't support seting bind paramerts after submitting the query, so 
# we need to manage the mutable state on the client side

DBIQueryInner <- R6Class("DBIQueryInner",
    public = list(
        initialize = function(query) {
            private$query <- query
        },

        submitQuery = function() {
            cursor <- submitQuery(private$query)

            private$cursor <- cursor 
            private$submitted <- TRUE
        },

        fetch = function(nRows) {
            if (!private$submitted) {
                self$submitQuery()
            }

            private$cursor$get_rows(nRows)
        },

        bind = function(res, params) {
            if (private$submitted) {
                warning("Cannot add bind variables to a query after it has been submitted")
                return 
            }

            for (param in params) {
                bindParam(private$query) <- value
            }
        }
    ),

    private = list(
        query = NULL,
        cursor = NULL,
        submitted = FALSE
    )
)
