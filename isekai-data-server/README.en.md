ISEKAI Data Server Guide
--
<!-- TOC -->
- [Overview](#overview)
- [Usage](#usage)
  - [Prerequisites](#prerequisites)
  - [Install Rust](#install-rust)
  - [Build](#build)
  - [Prepare Data](#prepare-data)
  - [Start the Server](#start-the-server)
  - [Start ngrok (when running on a client machine)](#start-ngrok-when-running-on-a-client-machine)
  - [Restrict Allowed Users](#restrict-allowed-users)
- [Let's Encrypt Certificate Setup with certbot](#lets-encrypt-certificate-setup-with-certbot)
  - [Prerequisites](#prerequisites-1)
  - [Install (recommended: snap)](#install-recommended-snap)
  - [Method 1: Nginx automatic setup (easy)](#method-1-nginx-automatic-setup-easy)
  - [Method 2: Webroot (use static file hosting)](#method-2-webroot-use-static-file-hosting)
  - [Method 3: Standalone (temporarily use port 80)](#method-3-standalone-temporarily-use-port-80)
  - [Change Certificate Owner (to the user running isekai-data-server)](#change-certificate-owner-to-the-user-running-isekai-data-server)
  - [Certificate Paths](#certificate-paths)
  - [Automatic Renewal](#automatic-renewal)
  - [Troubleshooting](#troubleshooting)
<!-- /TOC -->

# Overview
- ISEKAI Data Server (isekai-data-server) is server software that provides data to ISEKAI computation.
- It is written in Rust. You can use CSV files as a data source (SQLite is used when providing Edinet data).
- There are two deployment options for ISEKAI Data Server: a client machine (using ngrok) or your own server.
  - When running on a client machine, use [ngrok](https://ngrok.com/) so it is accessible from the internet.
  - When running on your own server, certificates are required because communication between ISEKAI computation and ISEKAI Data Server is encrypted with TLS.
  You can create your own certificate authority (CA), issue certificates, and include the CA certificate in the data provider module running on ISEKAI computation,
  but you can also connect without bundling your own CA certificate by using certificates issued by Let's Encrypt
  (Let's Encrypt is free to use).

# Usage
## Prerequisites
- Operating environment: Ubuntu 24.04 (you can also use Windows with WSL2)
- Domain name used by ISEKAI Data Server (for your own server): isekai-data.example.com
- Domain name used by ISEKAI Data Server (via ngrok): xxx.ngrok-free.dev (after logging in to ngrok, you can find it from the left menu under Domains)

## Install Rust
- The current MSRV (Minimum Supported Rust Version) for ISEKAI Data Server is 1.90.0. Building with earlier versions is not supported.

1. Install required packages (Ubuntu 24.04):
    ```
    sudo apt update
    sudo apt install -y build-essential curl pkg-config libssl-dev ca-certificates git
    ```

2. Install Rust using rustup:
    ```
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
    ```

## Build
1. Clone the repository:
    ```
    git clone https://github.com/seera-networks/ISEKAI-comp.git
    ```

2. Build by running the build script:
    ```
    cd ISEKAI-comp/isekai-data-server
    ./build.sh
    ```

3. When running the build script above, `gen_certs.sh` is executed to set up a certificate authority and issue certificates.
By default, the domain is set to `localhost`. This is not for production use, but if you do not use Let's Encrypt certificates,
replace the domain in `gen_certs.sh` from `localhost` to `isekai-data.example.com` for production use.
The updated `gen_certs.sh` is as follows:
    ```
    set -eux

    mkdir -p ./certs

    echo "subjectAltName = DNS:isekai-data.example.com" > certs/v3.txt

    # Create CA key and certificate
    openssl req -x509 -newkey rsa:4096 -keyout ./certs/ca.key -out ./certs/ca.crt -nodes -days 365 -subj "/CN=MyCA"

    # Create server key and CSR
    openssl req -newkey rsa:4096 -keyout ./certs/server.key -out ./certs/server.csr -nodes -subj "/CN=isekai-data.example.com"

    # Sign server certificate with CA
    openssl x509 -req -in ./certs/server.csr -CA ./certs/ca.crt -CAkey ./certs/ca.key -CAcreateserial -out ./certs/server.crt -days 365 -extfile ./certs/v3.txt
    ```

## Prepare Data
- Prepare the data to be provided from ISEKAI Data Server to ISEKAI computation. Here, we use the Wooldridge dataset as an example.
    ```
    git clone https://github.com/spring-haru/wooldridge.git
    ```

## Start the Server
- Run the following in the isekai-data-server directory (when using Let's Encrypt certificates):
    ```
    ../target/x86_64-unknown-linux-gnu/release/isekai-data-server --csv-file wooldridge/raw_data/data_csv/wage1.csv --policy-db ./policy.db --key /etc/letsencrypt/live/isekai-data.example.com/privkey.pem --cert /etc/letsencrypt/live/isekai-data.example.com/fullchain.pem
    ```
- Run the following in the isekai-data-server directory (when not using Let's Encrypt certificates):
    ```
    ../target/x86_64-unknown-linux-gnu/release/isekai-data-server --csv-file wooldridge/raw_data/data_csv/wage1.csv --policy-db ./policy.db --key ./certs/server.key --cert ./certs/server.crt
    ```
- Run the following in the isekai-data-server directory (when making it accessible via ngrok):
    ```
    ../target/x86_64-unknown-linux-gnu/release/isekai-data-server --csv-file wooldridge/raw_data/data_csv/wage1.csv --policy-db ./policy.db --no-tls
    ```

## Start ngrok (when running on a client machine)
1. Create an [ngrok](https://ngrok.com/) account and install the ngrok command (setup instructions are shown after account creation and login).

2. Start ngrok like this:
   ```
   ngrok http --domain=xxx.ngrok-free.dev --upstream-protocol=http2 50053
   ```
   Note: If you allow access via ngrok, data may be requested by parties other than ISEKAI computation (authentication exists, but because the authentication method is public, it is not perfect).
   To reduce the risk of unexpected data leakage, it is recommended to apply access control using the method described in [Restrict Allowed Users](#restrict-allowed-users).

## Restrict Allowed Users
- In ISEKAI computation, users are authenticated using tokens (JWT) issued by [YakAuth](https://seera-networks.github.io/YakAuth/). In ISEKAI Data Server, you can configure it to provide data only when specific users use ISEKAI computation.
1. Obtain the token for the user you want to allow in YakAuth.

2. Check the token `sub` using [JWT Debugger](https://www.jwt.io/). For example:
    ```
    "sub": "auth0|683eda78562794c7c574c4dc",
    ```

3. Replace `|` with `_` in the `sub` value and set it as an argument to isekai-data-server. For example:
    ```
    ../target/x86_64-unknown-linux-gnu/release/isekai-data-server --csv-file wooldridge/raw_data/data_csv/wage1.csv --authorized-subject auth0_683eda78562794c7c574c4dc --policy-db ./policy.db --key /etc/letsencrypt/live/isekai-data.example.com/privkey.pem --cert /etc/letsencrypt/live/isekai-data.example.com/fullchain.pem
    ```

# Let's Encrypt Certificate Setup with certbot

## Prerequisites
- Server: Ubuntu 24.04
- Domain name used by ISEKAI Data Server: isekai-data.example.com
- The domain points to your server correctly in DNS (A/AAAA records)
- Root privileges or sudo access

---

## Install (recommended: snap)
1. Update snap and install certbot:
    ```
    sudo snap install core
    sudo snap refresh core
    sudo snap install --classic certbot
    sudo ln -s /snap/bin/certbot /usr/bin/certbot   # if needed
    ```

2. Via apt (snap is recommended due to minor differences):
    ```
    sudo apt update
    sudo apt install certbot python3-certbot-nginx python3-certbot-apache
    ```

---

## Method 1: Nginx automatic setup (easy)
- If Nginx is running, certbot can update settings automatically:
  ```
  sudo certbot --nginx -d isekai-data.example.com
  ```
- If asked about HTTP -> HTTPS redirection, choose `2` (redirect).

---

## Method 2: Webroot (use static file hosting)
- Obtain a certificate by specifying the web server document root:
  ```
  sudo certbot certonly --webroot -w /var/www/html -d isekai-data.example.com
  ```

---

## Method 3: Standalone (temporarily use port 80)
- If you can temporarily stop the server:
  ```
  sudo systemctl stop nginx   # or apache2
  sudo certbot certonly --standalone -d isekai-data.example.com
  sudo systemctl start nginx
  ```

---

## Change Certificate Owner (to the user running isekai-data-server)
- Save the following as `/etc/letsencrypt/renewal-hooks/post/certbot-post-hook_isekai-data.sh`:
  ```
  #!/bin/bash
  # certbot post-hook script for changing certificate ownership

  CERT_PATH="/etc/letsencrypt/live/isekai-data.example.com"

  OWNER="ubuntu:ubuntu"

  chown $OWNER $CERT_PATH/privkey.pem
  chown $OWNER $CERT_PATH/fullchain.pem
  chown $OWNER $CERT_PATH/cert.pem
  chown $OWNER $CERT_PATH/chain.pem

  chmod 600 $CERT_PATH/privkey.pem
  chmod 644 $CERT_PATH/fullchain.pem
  chmod 644 $CERT_PATH/cert.pem
  chmod 644 $CERT_PATH/chain.pem

  echo "Certificate ownership changed to $OWNER"
  ```
- Add execute permission to certbot-post-hook_isekai-data.sh:
  ```
  sudo chmod +x /etc/letsencrypt/renewal-hooks/post/certbot-post-hook_isekai-data.sh
  ```
- Add the following line to the end of `/etc/letsencrypt/renewal/isekai-data.example.com.conf`:
  ```
  post_hook = /etc/letsencrypt/renewal-hooks/post/certbot-post-hook_isekai-data.sh
  ```
- Run a renewal test to execute the hook:
  ```
  sudo certbot renew --dry-run --run-deploy-hooks
  ```

---

## Certificate Paths
- Issued files:
  - /etc/letsencrypt/live/isekai-data.example.com/fullchain.pem
  - /etc/letsencrypt/live/isekai-data.example.com/privkey.pem

---

## Automatic Renewal
- Verify with a dry run:
  ```
  sudo certbot renew --dry-run
  ```
- With snap installation, a systemd timer is set automatically. If needed, add a deploy hook to reload:
  ```
  sudo certbot renew --deploy-hook "systemctl reload nginx"
  ```

---

## Troubleshooting
- Check whether ports 80/443 are blocked (firewall, cloud settings).
- Check whether DNS propagation is complete (`nslookup`/`dig`).
- On errors, check `/var/log/letsencrypt/`.

---
