
library(methods)
library(DBI)
library(rlang)

# This is purely a marker class to implimentent DbiDriver
# and drive the dbConnect function
setClass("DBISnowflakeAPI",
         contains = "DBIDriver"
)


# SnowflakeConnection holds the auth object and handles authenticating
# With Snowflake, the classes which actually handling creating and running
# queries will all host a reference to a SnowflakeConnection
setClass("SnowflakeConnection",
     contains = "DBIConnection",
     slots = c(

        # auth is an R6 class, so all queries crated by a connection share a reference
        # to a single auth object and we can refresh tokens in a single central place
        auth = "ANY",
        host = "character",
        proxy = "ANY",

        warehouse = "character",
        database = "character",
        schema = "character",
        role = "character"
    ),

    prototype = list(
        proxy = nullProxy,
        warehouse = NA_character_,
        database = NA_character_,
        schema = NA_character_,
        role = NA_character_
    )
)


# TODO: impliment the httr2 response inspection methods for this class

# Represents the results for a query which has been sent, can be used to
# actually fetch results from Snowflake
# Handles things like long running queries and result paging
setClass("DBISnowflakeResult",
    contains = "DBIResult",
    slots = c(
        inner = "ANY"   # A DBIQueryInner R6 object
    ) 
)
