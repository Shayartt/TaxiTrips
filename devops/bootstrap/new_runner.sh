sudo mkdir -p /efs
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport *********:/ /efs

python3 -m pip install -r /efs/traffic/code/requirements.txt
echo "Python requirements installed"