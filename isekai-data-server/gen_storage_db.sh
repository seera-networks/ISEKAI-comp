#!/bin/bash

DB="./storage.db"

if [ -f $DB ]; then
    echo "Database already exists"
    exit 0
fi

sqlite3 $DB <<EOF
CREATE TABLE policy (
    table_name TEXT NOT NULL,
    json TEXT NOT NULL
);

CREATE TABLE storage_schema (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    arrow_type TEXT NOT NULL,
    PRIMARY KEY (table_name, column_name)
);
EOF