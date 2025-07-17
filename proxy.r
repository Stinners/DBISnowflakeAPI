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
        host = host,
        port = port, 
        username = username, 
        password = password, 
        auth = auth
    )
}

# An empty proxy to use a a default - will do nothing 
# when passed to setProxy
nullProxy <- new("SnowflakeProxy", host = NA_character_, port = NA_integer_)

setProxy <- function(req, proxy) {
    if (is.null(proxy) || is.na(proxy@host)) {
        return(req)
    }
    else if (!is.na(proxy@username)) {
        req_proxy(req,
            url = proxy@host,
            port = proxy@port,
            username = proxy@username,
            password = proxy@password,
            auth = proxy@auth
        )
    }
    else {
        req_proxy(req,
            url = proxy@host,
            port = proxy@port,
            auth = proxy@auth
        )
    }
}
