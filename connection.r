library(methods)
library(DBI)
library(R6)
library(httr2)
library(jsonlite)
library(rlang)

source('cursor.r')
source('proxy.r')

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

setClassUnion("ProxyOrNull", members = c("SnowflakeProxy", "NULL"))


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
        proxy = "ProxyOrNull",

        warehouse = "character",
        database = "character",
        schema = "character",
        role = "character"
    ),

    prototype = list(
        proxy = NULL,
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

    # TODO validate how proxy variables are complete

    # TODO: Add proper error handling
    if (test) {
        resp <- initQuery(conn, "select 1") |> submitQuery()
        #if (resp_is_error(resp@cursor$raw_resp)) {
        #    message <- cat("Failed to connect to Snowflake ", resp_status, " ", resp_status_desc(resp))
        #    stop(message)
        #}
    }

    conn
})

# Create a query, but don't send it yet
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

getBindVarType <- function(var) {
    if (is.character(var)) {
        return("TEXT")
    } 
    else if (inherits(var, 'Date')) {   # TODO: figure out how the different datetime types work
        return('TIMESTAMP_TZ')
    } 
    else if (is.numeric(var)) {
        if (as.integer(var) == var) {
            return("FIXED")
        } 
        else {
            return("REAL")
        }
    } 
    else if (is.logical(var)) {
        return("BOOLEAN")
    } 
    else {
        stop(cat("Unknown bind variable type ", var))
    }
}

# Take the bind variables set in the SnowflakeQuery object and construct the 
# json body to send to Snowflake

setGeneric("makeBindBody", function(query) standardGeneric("makeBindBody"))
setMethod("makeBindBody", "SnowflakeQuery", function(query) {
    # TODO make this an apply call 
    body <- lapply(query@bindVars, function(variable) 
        list(
            type = getBindVarType(variable),
            value = variable
        )
    )
})

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
        body[["bindings"]] <- makeBindBody(query) 
    }

    req <- request(query@conn@host) |>
        req_url_path("api/v2/statements/") |>
        req_headers(!!!headers) |>
        req_body_json(body) |> 
        setProxy(query@conn@proxy) |> 
        req_perform()

    # TODO: make the query data on this reflect the actually sent context
    # we can get this from the response
    handle <- new("SnowflakeResultHandle",
        query = query,
        cursor = SnowflakeCursor$new(query, resp)
    )

    handle
})
