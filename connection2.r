library(methods)
library(DBI)
library(R6)
library(httr2)
library(jsonlite)
library(rlang)

source('cursor.r')

# Headers which should be included in every request
# regardless of authentication method
BASE_HEADERS <- c(
  "Accept" = "application/json",
  "Content-Type" = "application/json"
)

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
        host = "character",

        warehouse = "character",
        database = "character",
        schema = "character",
        role = "character"
    ),

    prototype = list(
        warehouse = NA_character_,
        database = NA_character_,
        schema = NA_character_,
        role = NA_character_
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
        params = "list",

        # these can optionally be provided, otherwise we use the values
        # from the connection
        warehouse = "character",
        database = "character",
        schema = "character",
        role = "character"
    ),

    prototype = list(
        params = list(),
        warehouse = NA_character_,
        database = NA_character_,
        schema = NA_character_,
        role = NA_character_
    )
)

##======================================================================

# TODO: impliment the httr2 response inspection methods for this class

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

setGeneric("initConnection", function(dvr, host, auth, ...) standardGeneric("initConnection"))
setMethod("initConnection", "DBISnowflakeAPI", function(
    dvr,
    host,
    auth,
    test = TRUE,
    database = NA_character_,
    schema = NA_character_,
    role = NA_character_,
    warehouse = NA_character_)
{
    conn <- new("SnowflakeConnection",
        auth = auth,
        host = host,

        warehouse = warehouse,
        database = schema,
        schema = schema,
        role = role
    )

    # TODO: Add proper error handling
    if (test) {
        resp <- initQuery(conn, "select 1") |> submitQuery()
        if (resp_is_error(resp@cursor$raw_resp)) {
            message <- cat("Failed to connect to Snowflake ", resp_status, " ", resp_status_desc(resp))
            stop(message)
        }
    }

    conn
})

# Create a query, but don't send it yet
setGeneric("initQuery", function(conn, statement, ...) standardGeneric("initQuery"))
setMethod("initQuery", "SnowflakeConnection", function(
        conn,
        statement,
        params = list(),
        database = NA_character_,
        schema = NA_character_,
        role = NA_character_,
        warehouse = NA_character_)
{
        new("SnowflakeQuery",
            conn = conn, statement = statement, params = params,

            warehouse = warehouse,
            database = schema,
            schema = schema,
            role = role
        )
})

setGeneric("bindParam<-", function(query, name, value) standardGeneric("bindParam<-"))
setMethod("bindParam<-", "SnowflakeQuery", function(query, name, value) {
    query@params[[name]] <- value
    query
})

if_not_null <- function(lst, name, ...) {
    for (value in list(...)) {
        if (!is.na(value)) {
            lst[[name]] <- value
            return(lst)
        }
    }
    lst
}

# TODO: handle refreshing
# TODO: handle parameters
setGeneric("submitQuery", function(query) standardGeneric("submitQuery"))
setMethod("submitQuery", "SnowflakeQuery", function(query) {

    # Check if auth has expired and updated the R6 auth object if necessary
    query@conn@auth$refresh()

    body <- list(statement = query@statement)
    headers <- query@conn@auth$set_auth_headers(BASE_HEADERS)

    # Set the Snowflake context parmameters if they are non-null
    # Prioritizing values on the query to values on the connection
    body <- body |>
        if_not_null("warehouse", query@warehouse, query@conn@warehouse) |>
        if_not_null("role", query@role, query@conn@role) |>
        if_not_null("database", query@database, query@conn@database) |>
        if_not_null("schema", query@schema, query@conn@schema)

    resp <- request(query@conn@host) |>
        req_headers(!!!headers) |>
        req_body_json(body) |>
        req_perform()

    # TODO: make the query data on this reflect the actually sent context
    # we can get this from the response
    handle <- new("SnowflakeResultHandle",
        query = query,
        cursor = SnowflakeCursor$new(resp)
    )

    handle
})
