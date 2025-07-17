library(methods)
library(httr2)


setClass("SnowflakeProxy",
    slots = c(
        host = "character",
        port = "numeric",
        username = "character",
        password = "character",
        auth = "character"
    ),
    prototype = list(
        username = NA_character_,
        password = NA_character_,
        auth = "basic"
    )
)


initProxy <- function(
    host, 
    port,
    username = NA_character_,
    password = NA_character_,
    auth = "basic")
{
    allowed_auth_types <- c("basic", "digest", "digest_ie", "gssnegotiate", "ntlm", "any")

    if (! auth %in% allowed_auth_types) {
        stop(cat("Auth method '", auth, "' is invalid - allowed types are: ", allowed_auth_types))
    }

    new("SnowflakeProxy", 
        port = port, 
        username = username, 
        password = password, 
        auth = auth
    )
}

setProxy <- function(req, proxy) {
    if (is.null(proxy)) {
        return(req)
    }

    req_proxy(req,
        url = proxy@host,
        port = proxy@port,
        username = proxy@username,
        password = proxy@password,
        auth = proxy@auth
    )
}
