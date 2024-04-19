FROM python:3.7-slim

WORKDIR /opt/camunda_api
RUN apt-get update -y
RUN apt-get install -y libmariadb-dev
RUN apt-get install -y gcc 

#RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip 
COPY webapp_requirements.txt requirements.txt
COPY . .
RUN ls /opt/camunda_api
RUN pip3 install -r requirements.txt


RUN pip3 install pyOpenSSL --upgrade
RUN python3 -m pip install --upgrade pandas
RUN python3 -m pip freeze


CMD [ "python3", "app.py"]