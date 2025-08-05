library(methods)
library(DBI)

source("dbi_types.r")
source("query.r")
source("connection.r")

setMethod("dbConnect", "DBISnowflakeAPI", function(drv, ...) initConnection(drv, ...))

# Currently disconnecting does nothing 
setMethod("dbDisconnect", "SnowflakeConnection", function(conn, ...) {})


# This creates the query object, but doesn't actually submit anything yet
setMethod(
    "dbSendQuery", 
    "SnowflakeConnection", 
    function(conn, statement, ...) {
        query <- initQuery(conn, statement, ...)
        inner <- DBIQueryInner$new(query)
        new("DBISnowflakeResult", inner = inner)
    }
)

setMethod(
    "dbFetch", 
    "DBISnowflakeResult", 
    function(res, n = -1, ...) res@inner$fetch(n)
)

setMethod(
    "dbClearResult", 
    "DBISnowflakeResult", 
    function(res, ...) TRUE
)

setMethod(
    "dbBind",
    "DBISnowflakeResult",
    function(res, params, ...) res@inner$bind(params)
)

# dbGetQuery - we use the default implimentation
# dbSendStatement - we use the default implimentation
# dbExecute - we use the default implimentation

# TODO: dbQuoteString
# TODO: dbQuoteIdentifier

# TODO: test this
setMethod("dbQuoteIdentifier", signature(conn = "SnowflakeConnection", x = "Id"),
    function(conn, x, ...) {
        quoteFunc <- function(val)  dbQuoteIdentifier(conn, val)
        parts <- x@name
        id <- paste0(lapply(parts, quoteFunc), collapse = ".")
        return(SQL(id))
    }
)

setMethod("dbQuoteIdentifier", signature(conn = "SnowflakeConnection", x = "character"),
    function(conn, x, ...) {
        quoted <- cat("\"", x, "\"")
        return(SQL(toupper(quoted)))
    }
)
