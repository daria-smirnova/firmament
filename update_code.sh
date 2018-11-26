sudo git add --all
sudo git commit -m "commit."
sudo git push origin1
sudo ./contrib/docker-build.sh
sudo docker tag firmament dariasmirnova/firmament:latest3
sudo docker push dariasmirnova/latest3
kubectl delete deployment firmament-scheduler -n kube-system
kubectl delete deployment poseidon -n kube-system
kubectl delete job cpuspin5
kubectl create -f ./../firmament-deployment.yaml
kubectl create -f https://raw.githubusercontent.com/kubernetes-sigs/poseidon/master/deploy/poseidon-deployment.yaml
kubectl create -f https://raw.githubusercontent.com/kubernetes-sigs/poseidon/master/deploy/configs/cpu_spin.yaml
