#!/bin/bash

aws ecr get-login-password --region {{aws_region}} | docker login --username AWS --password-stdin {{aws_id}}.dkr.ecr.{{aws_region}}.amazonaws.com

echo "Connected to AWS"

sudo chown -R ubuntu:ubuntu .
git pull

docker build -t $1:$2 .
docker tag $1:$2 {{aws_id}}.dkr.ecr.{{aws_region}}.amazonaws.com/$1:$2
docker push {{aws_id}}.dkr.ecr.{{aws_region}}.amazonaws.com/$1:$2
