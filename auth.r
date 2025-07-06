library(R6)
library(jose)
suppressMessages(library(openssl)) # linking messages
suppressMessages(library(digest))  # masks the sha1 function from openssl

# An abstract base class for different kinds of authetication
AbstractSnowflakeAuth <- R6Class("AbstractSnowflakeAuth", list(
    initialize = function(...) stop("AbstractSnowflakeAuth cannot be directly inititialized"),

    set_auth_headers = function(headers) {
        stop(cat("set_auth_headers is not implimented for ", class(self)[1]))
    },

    refresh = function() {
        stop(cat("refresh is not implimented for ", class(self)[1]))
    }
))

##======================================================================

JWT_VALID_FOR <- 60 * 60        # One Hour
JWT_REFRESH_TOLERANCE <- 5 * 60 # 5 minutes

KeyPairAuth <- R6Class("KeyPairAuth",
    public = list(

        # TODO: think about which of these should be private
        # and how they should be accessed by methods
        username = NA_character_,
        account_identifier = NA_character_,
        private_key_path = NA_character_,

        token = NA_character_,
        issued_at = NA_character_,
        expires_at = NA_integer_,

        initialize = function(username, account_identifier, private_key_path) {

            self$username <- username
            self$account_identifier <- account_identifier
            self$private_key_path <- private_key_path

            self$set_expiry_time()
            self$make_token(username, account_identifier, private_key_path)
        },

        set_auth_headers = function(headers) {
            auth_headers <- c(
              "X-Snowflake-Authorization-Token-Type" = "OAUTH",
              "Authorization" = paste("Bearer", auth$token)
            )

            c(headers, auth_headers)
        },

        refresh = function() {
            current_time <- as.integer(Sys.time())
            if (current_time > self$expires_at - JWT_REFRESH_TOLERANCE) {
                self$set_expiry_time()
                self$make_token(self$username, self$account_identifier, self$private_key_path)
            }
        }
    ),

    private = list(

        set_expiry_time = function() {
            self$issued_at <- as.integer(Sys.time())
            self$expires_at <- self$issued_at + JWT_VALID_FOR
        },

        get_key_fingerprint = function(private_key) {
            public_key <- as.list(private_key)$pubkey
            public_key_hash <- digest(as.raw(public_key), algo = "sha256", serialize = FALSE, raw = TRUE)
            paste("SHA256:", openssl::base64_encode(public_key_hash), sep = "")
        },

        make_raw_claim = function(fingerprint, username, account_identifier) {
            qualified_account_name <- toupper(paste(account_identifier, username, sep = "."))
            issuer <- paste(qualified_account_name, fingerprint, sep = ".")

            # We can"t use jose::jwt_claim since it validates each of the parameters and Snowflake expects an invalid
            # iss field. In particular Snowflake requires iss contains ":" which is not allowed unless the iss is a URI
            raw_claim <- list(
                  iss = issuer,
                  sub = qualified_account_name,
                  iat = self$issued_at,
                  exp = self$expires_at
            )

            structure(raw_claim, class = c("jwt_claim", "list"))
        },

        make_token = function(username, account_identifier, private_key_path) {
            private_key_raw <- read_key(private_key_path, der = is.raw(private_key_path))
            fingerprint <- get_key_fingerprint(private_key_raw)
            claim <- make_claim(fingerprint, username, account_identifier)

            encoded_claim <- jwt_encode_sig(claim, key = as.raw(private_key_raw), size = 256)
            self$token <- encoded_claim
        }
   )
)

# TODO: this should probably be called inside the constructor for
# SnowflakeConnection to ensure the R6 class is fully encapuslated
# in the S4 class
key_pair_auth <- function(username, account_identifier, private_key_path) {
    KeyPairAuth$new(username, account_identifier, private_key_path)
}
