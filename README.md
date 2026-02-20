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