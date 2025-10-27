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
EOF