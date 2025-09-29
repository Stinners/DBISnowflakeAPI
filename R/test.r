source("auth.r")
source("connection.r")
source("dbi.r")

host <- "https://esrnz-data.snowflakecomputing.com/api/v2/statements/"
private_key_path <- "/home/cstinson/.ssh/snowflake_training/rsa_key.p8"
keypair <- key_pair_auth("chris.stinson@esr.cri.nz", "esrnz-data", private_key_path)
driver <- new("DBISnowflakeAPI")
proxy <- initProxy(host = "proxy.esr.cri.nz", port = 3128)

conn <- dbConnect(driver,
    host, 
    keypair,
    testConnection = FALSE,
)

name <- "Robert'); DROP TABLE Students;--"

test <- function(...) {
    opt <- list(...)
    row.names <- opt[["row.names"]]
    if (isFALSE(row.names)) {
        print("False")
    }
    else if (is.null(row.names)) {
        print("NULL")
    }
}

test(row.names=TRUE)
