#!/bin/sh
# Load data into the database using psql
# Usage: ./load_data.sh -h <host> -p <port> -d <dbname> -U <user>

# Default values
host="localhost"
port="5432"
dbname="postgres"
user="postgres"
sslmode="prefer"

# Process command line arguments
while getopts "h:p:d:U:" opt; do
  case $opt in
    h) host="$OPTARG" ;;
    p) port="$OPTARG" ;;
    d) dbname="$OPTARG" ;;
    U) user="$OPTARG" ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

connection_string="host=$host port=$port dbname=$dbname user=$user sslmode=$sslmode target_session_attrs=read-write"

# Create the database if it doesn't exist
psql "$connection_string" -f $(pwd)/create_tables.sql

psql "$connection_string" -c "TRUNCATE customer, region, nation, part, supplier, partsupp;"

# Copy data into the tables from files
cat $(pwd)/_data/customer.txt | psql "$connection_string" -c "copy customer from stdin with (DELIMITER '|');"
cat $(pwd)/_data/region.txt | psql "$connection_string" -c "copy region from stdin with (DELIMITER '|');"
cat $(pwd)/_data/nation.txt | psql "$connection_string" -c "copy nation from stdin with (DELIMITER '|');"
cat $(pwd)/_data/part.txt | psql "$connection_string" -c "copy part from stdin with (DELIMITER '|');"
cat $(pwd)/_data/supplier.txt | psql "$connection_string" -c "copy supplier from stdin with (DELIMITER '|');"
cat $(pwd)/_data/partsupp.txt | psql "$connection_string" -c "copy partsupp from stdin with (DELIMITER '|');"
