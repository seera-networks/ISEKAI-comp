# ISEKAIデータサーバーについて
- ISEKAIデータサーバー（isekai-data-server）は、ISEKAI計算にデータを提供するためのサーバーソフトウェアです。
- Rustで書かれており、データソースとしては、CSVファイルを用いることができます（Edinetのデータを提供する場合にはsqliteを使います）。
- ISEKAI計算とISEKAIデータサーバーの間の通信はTLSで暗号化するため証明書が必要です。
独自の認証局を作成し、証明書を発行し、ISEKAI計算の上で動かすデータ提供モジュールに認証局の証明書を含めることもできますが、
Let's Encryptで発行できる証明書を利用することで、認証局の証明書を含めることなく接続を行うことができます
（Let's Encryptの利用は無料です）。

# ISEKAIデータサーバーの使い方
## 前提
- サーバー: Ubuntu 24.04
- ISEKAIデータサーバーが用いるドメイン名: isekai-data.example.com

## Rust のインストール
- ISEKAIデータサーバーのMSRV (Minimum Supported Rust Version)は、現在、1.90.0です。これ以前のバージョンでのビルドはサポートしていません。

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

## ビルド
1. レポジトリを複製する:
    ```
    git clone https://github.com/seera-networks/ISEKAI-comp.git
    ```

2. ビルドスクリプトを実行してビルドする:
    ```
    cd ISEKAI-comp/isekai-data-server
    ./build.sh
    ```

3. 上のビルドスクリプトの実行の際に、gen_certs.shが実行され、認証局のセットアップと証明書の発行が行われます。
デフォルトではドメインがlocalhostとなっており、本番では使用しませんが、Let's Encryptの証明書を使わない場合は
gen_certs.shで指定しているドメイン名をlocalhostからisekai-data.example.comに書き換えて本番で使用します。
書き換え後のgen_certs.shは次の通りです:
    ```
    set -eux

    mkdir -p ./certs

    echo "subjectAltName = DNS:isekai-data.example.com" > certs/v3.txt

    # Create CA key and certificate
    openssl req -x509 -newkey rsa:4096 -keyout ./certs/ca.key -out ./certs/ca.crt -nodes -days 365 -subj "/CN=MyCA"

    # Create server key and CSR
    openssl req -newkey rsa:4096 -keyout ./certs/server.key -out ./certs/server.csr -nodes -subj "/CN=isekai-data.example.com"

    # Sign server certificate with CA
    openssl x509 -req -in ./certs/server.csr -CA ./certs/ca.crt -CAkey ./certs/ca.key -CAcreateserial -out ./certs/server.crt -days 365 -extfile ./certs/v3.txt    ```
    ```

## データの用意
- ISEKAIデータサーバーからISEKAI計算に提供するデータを用意します。ここでは、Wooldridgeのデータセットを例に用います
    ```
    git clone https://github.com/spring-haru/wooldridge.git
    ```
## 起動
- isekai-data-serverのディレクトリで以下を実行（Let's Encryptの証明書を使用する場合）:
    ```
    ../target/x86_64-unknown-linux-gnu/release/isekai-data-server --csv-file wooldridge/raw_data/data_csv/wage1.csv --policy-db ./policy.db --key /etc/letsencrypt/live/isekai-data.example.com/privkey.pem --cert /etc/letsencrypt/live/isekai-data.example.com/fullchain.pem
    ```
- isekai-data-serverのディレクトリで以下を実行（Let's Encryptの証明書を使用しない場合）:
    ```
    ../target/x86_64-unknown-linux-gnu/release/isekai-data-server --csv-file wooldridge/raw_data/data_csv/wage1.csv --policy-db ./policy.db --key ./certs/server.key --cert ./certs/server.crt
    ```
## 利用できるユーザの制限
- ISEKAI計算では、[YakAuth](https://seera-networks.github.io/YakAuth/)で発行したトークン（JWT）を使用してユーザの認証を行います。ISEKAIデータサーバーでは、特定のユーザーがISEKAI計算を利用する場合のみデータを提供するように設定できます。
1. YakAuthで利用したいユーザのトークンを取得する。

2. [JWT Debugger](https://www.jwt.io/)を用いてトークンのsubを調べる。subは例えば次のように設定されています:
    ```
    "sub": "auth0|683eda78562794c7c574c4dc",
    ```

3. subの内容のうち、|を_に置き換えてisekai-data-serverの引数に設定する。例えば、次のようになります:
    ```
    ../target/x86_64-unknown-linux-gnu/release/isekai-data-server --csv-file wooldridge/raw_data/data_csv/wage1.csv --authorized-subject auth0_683eda78562794c7c574c4dc --policy-db ./policy.db --key /etc/letsencrypt/live/isekai-data.example.com/privkey.pem --cert /etc/letsencrypt/live/isekai-data.example.com/fullchain.pem
    ```

# certbot を使ったLet's Encryptの証明書設定手順

## 前提
- サーバー: Ubuntu 24.04
- ISEKAIデータサーバーが用いるドメイン名: isekai-data.example.com
- ドメインが DNS に正しく向いていること（A/AAAA レコード）
- root 権限または sudo が使えること

---

## インストール（推奨: snap）
1. snap を更新して certbot を入れる:
    ```
    sudo snap install core
    sudo snap refresh core
    sudo snap install --classic certbot
    sudo ln -s /snap/bin/certbot /usr/bin/certbot   # 必要なら
    ```

2. apt 経由（軽微な差異があるため snap を推奨）:
    ```
    sudo apt update
    sudo apt install certbot python3-certbot-nginx python3-certbot-apache
    ```

---

## 方法1: Nginx 自動設定（簡単）
- Nginx が稼働している場合、certbot が自動で設定を変更してくれる:
  ```
  sudo certbot --nginx -d isekai-data.example.com
  ```
- HTTP → HTTPS リダイレクトを聞かれたら `2`（リダイレクト）を選択。

---

## 方法2: Webroot（静的ファイル配信を使う）
- Web サーバのドキュメントルートを指定して証明書を取得:
  ```
  sudo certbot certonly --webroot -w /var/www/html -d isekai-data.example.com
  ```

---

## 方法3: Standalone（ポート 80 を一時的に使用）
- サーバーを一時停止できる場合:
  ```
  sudo systemctl stop nginx   # または apache2
  sudo certbot certonly --standalone -d isekai-data.example.com
  sudo systemctl start nginx
  ```

---

## 証明書の所有者の変更（isekai-data-serverを動かすユーザへの変更）
- /etc/letsencrypt/renewal-hooks/post/certbot-post-hook_isekai-data.sh として次の内容を保存する
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
- certbot-post-hook_isekai-data.shに実行パーミッションを追加する
  ```
  sudo chmod +x /etc/letsencrypt/renewal-hooks/post/certbot-post-hook_isekai-data.sh
  ```
- /etc/letsencrypt/renewal/isekai-data.example.com.conf の末尾に次の行を追加する:
  ```
  post_hook = /etc/letsencrypt/renewal-hooks/post/certbot-post-hook_isekai-data.sh
  ```
- 更新テストでhookを実行する
  ```
  sudo certbot renew --dry-run --run-deploy-hooks
  ```
---

## 証明書の場所
- 発行されたファイル:
  - /etc/letsencrypt/live/isekai-data.example.com/fullchain.pem
  - /etc/letsencrypt/live/isekai-data.example.com/privkey.pem

---

## 自動更新
- 動作確認（ドライラン）:
  ```
  sudo certbot renew --dry-run
  ```
- snap インストールだと systemd タイマーが自動でセットされる。必要なら deploy hook を追加して reload:
  ```
  sudo certbot renew --deploy-hook "systemctl reload nginx"
  ```

---

## トラブルシューティング
- ポート 80/443 がブロックされていないか確認（ファイアウォール、クラウド設定）。
- DNS の反映が完了しているか確認（nslookup/dig）。
- エラー時は /var/log/letsencrypt/ を参照。

---