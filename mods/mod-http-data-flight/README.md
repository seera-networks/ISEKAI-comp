ISEKAIデータ提供モジュール説明書
--
<!-- TOC -->
- [概要](#概要)
- [ISEKAIデータ提供モジュールのビルド方法](#isekaiデータ提供モジュールのビルド方法)
  - [前提](#前提)
  - [Rust のインストール](#rust-のインストール)
  - [ビルド](#ビルド)
<!-- /TOC -->

# 概要
- ISEKAIデータ提供モジュールは、ISEKAI計算上で動作し、ISEKAIデータサーバーに接続してデータを取得するモジュールです。
- Rustで書かれたWASMモジュールを含み、接続先情報などが書き込まれたファイルです。
- ISEKAI計算にアップロードして使用します。

# ISEKAIデータ提供モジュールのビルド方法
## 前提
- ビルド環境: Ubuntu 24.04
- ドメイン名とポート: isekai-data.example.com:50053（独自サーバーでISEKAIデータサーバーを動作させる場合）
- ドメイン名: xxx.ngrok-free.dev（ngrokを利用してクライアント端末でISEKAIデータサーバーを動作させる場合）

## Rust のインストール
- ISEKAIデータ提供モジュールのMSRV (Minimum Supported Rust Version)は、現在、1.90.0です。これ以前のバージョンでのビルドはサポートしていません。

1. 必要パッケージをインストール（Ubuntu 24.04）:
    ```
    sudo apt update
    sudo apt install -y build-essential curl pkg-config libssl-dev ca-certificates git
    ```

2. rustup を使って Rust をインストール:
    ```
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
    ```

3. WASI Preview 0.2のビルド環境をインストール
    ```
    rustup target add wasm32-wasip2
    ```

## ビルド
1. レポジトリを複製する:
    ```
    git clone https://github.com/seera-networks/ISEKAI-comp.git
    ```

2. ISEKAI-comp/mods/mod-http-data-flight/yak.tomlを編集して、接続先情報を設定する（Let's Encryptの証明書を使用した独自サーバーの場合）:
    ```
    name = "mod-http-data-flight"
    module_type = "Wasm"
    files = []

    [external_com]
    endpoint = "https://isekai-data.example.com:50053"
    use_tls = true
    use_mtls = true
    ```

2. ISEKAI-comp/mods/mod-http-data-flight/yak.tomlを編集して、接続先情報を設定する（Let's Encryptの証明書を使用しない独自サーバーの場合。先にISEKAIデータサーバーのビルドを終えておく必要があります）:
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

3. ISEKAI-comp/mods/mod-http-data-flight/yak.tomlを編集して、接続先情報を設定する（ngrokを利用してクライアント端末でISEKAIデータサーバーを動作させる場合）:
    ```
    name = "mod-http-data-flight"
    module_type = "Wasm"
    files = []

    [external_com]
    endpoint = "https://xxx.ngrok-free.dev"
    use_tls = true
    ```

4. レポジトリに付属しているyakcliコマンドを使用してモジュールをビルドする:
    ```
    cd ISEKAI-comp/mods/mod-http-data-flight
    ../../bin/yakcli build
    ```

5. カレントディレクトリにmod-http-data-flight.yakが生成されているので、[ISEKAIデータ提供モジュール Uploader](https://yakserv.seera-networks.com/)を用いてアップロードする。

6. アップロードが完了したらトークンが出力されます。このトークンをjupyterliteでdata_tokenに指定します。
