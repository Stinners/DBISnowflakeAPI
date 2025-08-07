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

# dbQuoteString - use the default implimentation

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
        quoted <- paste0("\"", x, "\"")
        return(SQL(toupper(quoted)))
    }
)

# TODO: test this 
# TODO: impliment the check.names parameter
setMethod("dbReadTable", "SnowflakeConnection", function(conn, name, ...) {
    quoted_name <- dbQuoteIdentifier(name)
    query_text <- paste("SELECT * FROM", quoted_name, ";")
    query <- dbSendQuery(conn, query_text)
    df <- dfFetch(query)

    opt <- list(...)
    row.names <- opt[["row.names"]]
    if (isTRUE(row.names)) {
        row.names(df) <- df[["row_names"]]
    }
    else if (is.na(row.names)) {
        if ("row_names" %in% df) {
            row.names(df) <- df[["row_names"]]
        }
    }
    else if (is.character(row.names)) {
        row.names(df) <- df[[row.names]]
    }
    else if (!(isFALSE(row.names) && is.null(row.names))) {
        abort(paste("row.name argument of dbReadTable must be a boolean or a character, but was:", row.names))
    }

    return(df)
})
