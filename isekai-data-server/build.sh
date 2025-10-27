#!/bin/bash

set -eux

cd $(dirname $0)

cargo build --package isekai-data-server --release --target x86_64-unknown-linux-gnu

./gen_certs.sh
./gen_policy_db.sh
./gen_storage_db.sh
