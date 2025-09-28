library(methods)
library(DBI)

source("dbi_types.r")
source("query.r")
source("connection.r")

setMethod("dbConnect", "DBISnowflakeAPI", function(drv, ...) initConnection(drv, ...))

# Currently disconnecting does nothing 
# TODO: This should probably displose of the data in the auth object
setMethod("dbDisconnect", "SnowflakeConnection", function(conn, ...) {})


# Despite the name: this creates the query object, but doesn't actually submit anything yet
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

# dbGetQuery - default
# dbSendStatement - default
# dbExecute - default

# dbQuoteString - default

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

# TODO: this doesn't do anything as yet 
# We have to figure out how to handle special characters in quoted column names
makeRidentifier <- function(SfIdentifier) {
    return(SfIdentifier)
}

# TODO: support bind variables
setGeneric("runQuery", function(conn, queryText) standardGeneric("runQuery")) 
setMethod("runQuery", "SnowflakeConnection", function(conn, queryText) {
    query_text <- paste(queryText)
    query <- dbSendQuery(conn, query_text)
    df <- dbFetch(query)
    return(df)
})

# TODO: test this 
# TODO: impliment the check.names parameter
setMethod("dbReadTable", "SnowflakeConnection", function(conn, name, ...) {
    quoted_name <- dbQuoteIdentifier(name)
    query_text <- paste("SELECT * FROM", quoted_name, ";")
    df <- runQuery(conn, query_text)

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

    if(check.names) {
        colNames <- names(df) |> makeRidentifier
    }

    return(df)
})

# TODO: dbtWriteTable - check that everything else is working before we start on this


# TODO: support getting tables in specific schema, and returning infromation 
# about schema and catalog
setMethod("dbExistsTables", "SnowflakeConnection", function(conn, ...) {
    query_text <- "
      SELECT table_name from SNOWFLAKE.ACCOUNT_USAGE.TABLES
      UNION ALL
      SELECT table_name from SNOWFLAKE.ACCOUNT_USAGE.VIEWS"
    df <- runQuery(conn, query_text)
    return(as.list(df["TABLE_NAME"]))
})

setMethod("dbExistsTable", "SnowflakeConnection", function(conn, name, ...) {
    query_text <- "
        with all_tables as (
          SELECT table_name from SNOWFLAKE.ACCOUNT_USAGE.TABLES
          UNION ALL
          SELECT table_name from SNOWFLAKE.ACCOUNT_USAGE.VIEWS
        )
        select ? in (SELECT TABLE_NAME FROM all_tables) AS TABLE_EXISTS;"
    query <- dbSendQuery(conn, query_text)
    bindParam(query) <- name
    df <- dbFetch(query)
    return(any(df["TABLE_EXISTS"]))
})
