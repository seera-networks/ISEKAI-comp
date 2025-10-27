#!/bin/bash

DB="./policy.db"

if [ -f $DB ]; then
    echo "Database already exists"
    exit 0
fi

sqlite3 $DB <<EOF
CREATE TABLE policy (
    subject TEXT NOT NULL,
    json TEXT NOT NULL
);
EOF

sqlite3 $DB <<EOF
CREATE TABLE policy_by_item (
    item TEXT NOT NULL,
    json TEXT NOT NULL
);
EOF