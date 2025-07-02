suppressMessages(library(openssl)) # linking messages
library(jose)
suppressMessages(library(digest))  # masks the sha1 function from openssl

# ======= Auth Interface =======

set_auth_headers <- function(auth, headers) {
    UseMethod("set_auth_headers")
}

set_auth_headers.default <- function(auth, headers) {
    stop(paste(class(auth), "is not a valid authorization provider",
        "it does not impliment the method 'auth_headers'"))
}

refresh <- function(auth) {
    UseMethod("refresh")
}

refresh.default <- function(auth) auth


# ======= Public Key Auth =======

JWT_VALID_FOR <- 60 * 60 # One Hour

set_auth_headers.key_pair_auth <- function(auth, headers) {
    auth_headers <- c(
      "X-Snowflake-Authorization-Token-Type" = "KEYPAIR_JWT",
      "Authorization" = paste("Bearer", auth$token)
    )

    c(headers, auth_headers)
}

refresh.key_pair_auth <- function(auth) {
    tolerance <- 5 * 60 # 5 minutes
    refresh_at <- auth$expires_at - tolerance
    now <- as.integer(Sys.time())

    if (now > refresh_at) {
        refresh_jwt(auth)
    } else {
        auth
    }
}

get_key_fingerprint <- function(private_key) {
    public_key <- as.list(private_key)$pubkey
    public_key_hash <- digest(as.raw(public_key), algo = "sha256", serialize = FALSE, raw = TRUE)
    paste("SHA256:", openssl::base64_encode(public_key_hash), sep = "")
}


make_claim <- function(fingerprint, username, account_identifier) {
    qualified_account_name <- toupper(paste(account_identifier, username, sep = "."))
    issuer <- paste(qualified_account_name, fingerprint, sep = ".")
    issued_at <- as.integer(Sys.time())

    # We can"t use jose::jwt_claim since it validates each of the parameters and Snowflake expects an invalid
    # iss field. In particular Snowflake requires iss contains ":" which is not allowed unless the iss is a URI
    raw_claim <- list(
          iss = issuer,
          sub = qualified_account_name,
          iat = issued_at,
          exp = issued_at + JWT_VALID_FOR
    )

    structure(raw_claim, class = c("jwt_claim", "list"))
}

# TODO handle private keys with passwords
key_pair_auth <- function(username, account_identifier, private_key_path) {
    private_key_raw <- read_key(private_key_path, der = is.raw(private_key_path))
    fingerprint <- get_key_fingerprint(private_key_raw)
    claim <- make_claim(fingerprint, username, account_identifier)

    encoded_clain <- jwt_encode_sig(claim, key = as.raw(private_key_raw), size = 256)

    key_pair <- list(
        token = encoded_clain,
        expires_at = claim$exp,
        fingerprint = fingerprint,
        username = username,
        account_identifier = account_identifier
    )

    structure(key_pair, class = c("key_pair_auth", "auth", "list"))
}

refresh_jwt <- function(auth) {
    key_pair_auth(auth$username, auth$account_identifier, auth$private_key_path)
}

# ======= OAuth Auth =======

set_auth_headers.oauth_auth <- function(auth, headers) {
    auth_headers <- c(
      "X-Snowflake-Authorization-Token-Type" = "OAUTH",
      "Authorization" = paste("Bearer", auth$token)
    )

    c(headers, auth_headers)
}

oauth <- function(token) {
    token <- list(
        token = token
    )
    structure(token, class = c("ouath_auth"))
}
