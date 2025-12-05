k3d cluster create ks --api-port "0.0.0.0:6443" --port "8080:80@loadbalancer" --port "30880:30880@loadbalancer" --image rancher/k3s:v1.25.16-k3s4 --registry-config ./k3d-registries.yaml
k3d cluster create ks --api-port "0.0.0.0:6443" --port "8080:80@loadbalancer" --port "30880:30880@loadbalancer" --image rancher/k3s:v1.23.17-k3s1 --registry-config ./k3d-registries.yaml


# https://github.com/kit101/ks-installer2/blob/v3.4.1-k1/deploy
kubectl apply -f kubesphere-installer.yaml
kubectl apply -f cluster-configuration.yaml
