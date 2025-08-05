# Fundamental DBI Classes 

1. DBIDriver -> DBIsnowflakeAPI 
2. DBIConnection -> SnowflakeConnection 
3. DBIResult -> DBISnowflakeResult 

DBISnowflakeResult is just a wrapper around the `DBIQueryInner` class.
DBIQueryInner manages the mutable state which DBI assumes will be handled 
by the database backed. It contains two more classes:
    1. SnowflakeQuery - holds the information about the query itself 
    2. SowflakeCursor - holds the state of the response once a query has been sent

# File Structure 

dbi_types  - just contains the definition of the 3 core classes
connection - contains an implimentation of the logic for connection to 
              Snowflake independent of the DBI protocol
dbi - contains implimenations of dbi specifc logic
