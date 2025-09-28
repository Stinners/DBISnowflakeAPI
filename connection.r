library(methods)
library(DBI)
library(R6)
library(httr2)
library(jsonlite)
library(rlang)

source('cursor.r')
source('proxy.r')
source('dbi_types.r')


# Headers which should be included in every request
# regardless of authentication method
BASE_HEADERS <- c(
  "Accept" = "application/json",
  "Content-Type" = "application/json"
)

## =============================== Creating and Submitting Queries ==================

setClass("SnowflakeQuery",
    slots = c(
        conn = "SnowflakeConnection",
        statement = "character",
        bindVars = "list",

        # these can optionally be provided, otherwise we use the values
        # from the connection
        warehouse = "character",
        database = "character",
        schema = "character",
        role = "character"
    ),

    prototype = list(
        bindVars = list(),
        warehouse = NA_character_,
        database = NA_character_,
        schema = NA_character_,
        role = NA_character_
    )
)

# Create a query, but don't send it yet
# Only creates the query, not the SnowflakeHandle Wrapper
setGeneric("initQuery", function(conn, statement, ...) standardGeneric("initQuery"))
setMethod("initQuery", "SnowflakeConnection", function(
        conn,
        statement,
        database = NA_character_,
        schema = NA_character_,
        role = NA_character_,
        warehouse = NA_character_)
{
        new("SnowflakeQuery",
            conn = conn,
            statement = statement,

            warehouse = warehouse,
            database = schema,
            schema = schema,
            role = role
        )
})


# Snowflake does not support named bind variables
# So for now we need to just supply them in order
setGeneric("bindParam<-", function(query, value) standardGeneric("bindParam<-"))
setMethod("bindParam<-", "SnowflakeQuery", function(query, value) {
    i <- length(query@bindVars) + 1
    query@bindVars[[i]] <- value
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

# Actually send the query to Snowflake and return the cursor
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


    if (length(query@bindVars) > 0) {
        body[["bindings"]] <- makeBindBody(query) # TODO: impliment this
    }

    req <- request(query@conn@host) |>
        req_url_path("api/v2/statements/") |>
        req_headers(!!!headers) |>
        req_body_json(body) |>
        setProxy(query@conn@proxy)

    resp <- req_perform(req)
    cursor <- SnowflakeCursor$new(query, resp)
    cursor
})

## =============================== Creating Connections ==================

setGeneric("initConnection", function(dvr, host, auth, ...) standardGeneric("initConnection"))
setMethod("initConnection", "DBISnowflakeAPI", function(
    dvr,
    host,
    auth,
    testConnection = TRUE,
    database = NA_character_,
    schema = NA_character_,
    role = NA_character_,
    warehouse = NA_character_,
    proxy = NULL)

{
    conn <- new("SnowflakeConnection",
        auth = auth,
        host = host,

        proxy = proxy,
        warehouse = warehouse,
        database = schema,
        schema = schema,
        role = role
    )

    # TODO: Add proper error handling
    if (testConnection) {
        resp <- initQuery(conn, "select 1") |> submitQuery()
    }

    conn
})

