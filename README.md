# MicroK8s Setup Instructions for Bitcoin Indexer

```bash
sudo apt update && sudo apt upgrade -y

sudo snap install microk8s --classic --channel=1.28/stable

sudo microk8s enable dns hostpath-storage  metrics-server

# get config
sudo microk8s config > ./.kube/config

# bashrc
vim .bashrc
```

Bashrc config here:

```bash
# Config for K9s
export KUBECONFIG=~/.kube/config
# Use nano instead of vi as default editor
export KUBE_EDITOR="vim"
# Autocomplete kubectl
source <(microk8s kubectl completion bash)
```

```bash
source .bashrc

sudo snap install kubectl --classic

# k9s
wget https://github.com/derailed/k9s/releases/download/v0.27.3/k9s_Linux_amd64.tar.gz

tar -xvzf k9s_Linux_amd64.tar.gz

sudo mv k9s /bin

rm LICENSE README.md k9s_Linux_amd64.tar.gz
```

Apply and Create SC and PVC

```bash
kubectl apply -f bitcoin-sc.yaml

kubectl patch storageclass microk8s-hostpath -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
kubectl patch storageclass nvme-hostpath -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

kubectl apply -f bitcoin-pvc.yaml
kubectl apply -f bitcoin-service.yaml
```

The RPC should now be available on port 30032 of your localhost.

```bash
python ./indexer/quantum-at-risk.py
```

### BLK direct seeks (optional, faster)

To use Bitcoin Core’s block index and seek directly to blocks instead of scanning blk files, install `plyvel`. On systems that don’t allow global `pip install`:

```bash
sudo apt install python3.12-venv   # if needed
cd /root/btcq-indexer
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m indexer.main
```

Run the indexer with `.venv/bin/python` so `plyvel` is used and you see direct seeks instead of “height N not in blk….dat, trying next file”.

## Database roles (docker-compose)

Postgres is published on `127.0.0.1:5432` only. Don't change that to `5432:5432`: Docker-published
ports bypass host firewalls such as `ufw`.

There are no default passwords. Copy `.env.example` to `.env` and set `POSTGRES_PASSWORD`, `DB_PASS`
and `API_DB_PASS` (for example `openssl rand -hex 32` each). `docker compose` refuses to start without them,
and so do the indexer and the API.

| Role       | Used by                         | Rights                                   |
|------------|---------------------------------|------------------------------------------|
| `postgres` | administration only             | superuser                                |
| `indexer`  | `indexer.main`, `indexer.address` (`DB_USER`/`DB_PASS`) | owns the schema, not a superuser |
| `api_ro`   | `indexer.api` (`API_DB_USER`/`API_DB_PASS`) | `SELECT` only, read-only transactions |

`db-init/01-roles.sh` creates these roles the first time the `pg` volume is initialised.

### Migrating an existing volume

A volume created before this change has `indexer` as its only superuser with the password `password`.
With `.env` filled in and the new image pulled:

```bash
docker compose up -d pg
# 1. Upgrade the TimescaleDB extension to the new image's version (must be the first command in the session).
docker compose exec pg psql -X -U indexer -d postgres -c "ALTER EXTENSION timescaledb UPDATE"
# 2. Create the admin superuser with POSTGRES_PASSWORD from .env.
docker compose exec pg sh -c 'echo "CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD :'"'"'pw'"'"';" | psql -v ON_ERROR_STOP=1 -U indexer -d postgres -v pw="$POSTGRES_PASSWORD"'
# 3. Demote indexer, set the new DB_PASS, and create api_ro.
docker compose exec pg /docker-entrypoint-initdb.d/01-roles.sh
```

After step 3 the old `password` no longer works. If the old port was reachable from outside, also check
the database and the container for changes the indexer didn't make.
