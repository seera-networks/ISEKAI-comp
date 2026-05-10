ISEKAI Data Provider Module Guide
--
<!-- TOC -->
- [Overview](#overview)
- [How to Build the ISEKAI Data Provider Module](#how-to-build-the-isekai-data-provider-module)
  - [Prerequisites](#prerequisites)
  - [Install Rust](#install-rust)
  - [Build](#build)
<!-- /TOC -->

# Overview
- The ISEKAI Data Provider Module runs on ISEKAI computation and retrieves data by connecting to ISEKAI Data Server.
- It includes a Rust-based WASM module and configuration files that contain connection settings.
- Upload it to ISEKAI computation to use it.

# How to Build the ISEKAI Data Provider Module
## Prerequisites
- Build environment: Ubuntu 24.04
- Domain and port: isekai-data.example.com:50053 (when running ISEKAI Data Server on your own server)
- Domain: xxx.ngrok-free.dev (when running ISEKAI Data Server on a client machine via ngrok)

## Install Rust
- The current MSRV (Minimum Supported Rust Version) for the ISEKAI Data Provider Module is 1.90.0. Building with earlier versions is not supported.

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

3. Install the build target for WASI Preview 0.2:
    ```
    rustup target add wasm32-wasip2
    ```

## Build
1. Clone the repository:
    ```
    git clone https://github.com/seera-networks/ISEKAI-comp.git
    ```

2. Edit `ISEKAI-comp/mods/mod-http-data-flight/yak.toml` and set connection information (for your own server using Let's Encrypt certificates):
    ```
    name = "mod-http-data-flight"
    module_type = "Wasm"
    files = []

    [external_com]
    endpoint = "https://isekai-data.example.com:50053"
    use_tls = true
    use_mtls = true
    ```

3. Edit `ISEKAI-comp/mods/mod-http-data-flight/yak.toml` and set connection information (for your own server not using Let's Encrypt certificates; you need to finish building ISEKAI Data Server first):
    ```
    name = "mod-http-data-flight"
    module_type = "Wasm"
    files = []

    [external_com]
    endpoint = "https://isekai-data.example.com:50053"
    use_tls = true
    use_mtls = true
    ca_cert = "../../isekai-data-server/certs/ca.crt"
    ```

4. Edit `ISEKAI-comp/mods/mod-http-data-flight/yak.toml` and set connection information (when running ISEKAI Data Server on a client machine via ngrok):
    ```
    name = "mod-http-data-flight"
    module_type = "Wasm"
    files = []

    [external_com]
    endpoint = "https://xxx.ngrok-free.dev"
    use_tls = true
    ```

5. Build the module using the `yakcli` command included in the repository:
    ```
    cd ISEKAI-comp/mods/mod-http-data-flight
    ../../bin/yakcli build
    ```

6. The file `mod-http-data-flight.yak` is generated in the current directory. Upload it using [ISEKAI Data Provider Module Uploader](https://yakserv.seera-networks.com/).

7. After the upload completes, a token is output. Specify this token as `data_token` in jupyterlite.
