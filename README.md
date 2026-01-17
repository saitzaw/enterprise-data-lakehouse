#  data engineering in Scala, PySpark 

  Enterprise Data engineering needs complex infrastrucre setup and install huge dependencies to run a single pipeline. However, thanks to containerization, we can build similar setup for the DEV, QUA and PRD environment. This setup is test to run the Scala, Pyspark in low cost method. This repo also accept to use the ad-hoc query to run in juypter notebook. 
  
  Besides of Data Engineering, this repo can give the business value to the users (dataset, busienss term, lineage etc.)

  All the scripts and code are easy to test and run in local before submit to cloud server with zero cost. 

# Data Pipeline Architecture
![System Architecture](asserts/dataArch.png)

# SYSTEM Requirement 
- GNU Linux or WSL 2
- Docker must be installed 
- docker compose must be installed 
- RAM 16 GB at least 
- CPU core i5 at least 

# Languages 
- Python, PySpark 
- Scala 
- SQL (for Postgres,MySQL)

# Framework 
- Apache Spark 
- Apache Airflow 
- Elastic Search
- Jupyter notebook / Lab for analysis
- dbt (data build tool) for SQL transformations
- ClickHouse for OLAP analytics

# Database 
- Postgresql 15
- Mysql 
- MongoDB
- ClickHouse (gold layer analytics)

# Additional requirements
- Vscode with Metal extension 
- DBeaver 
- Docker for Desktop 
- Git 
- Postman for API test (for Elastic Search)

# Version alignment 

| # | language | version | 
|:-:|:-------:|:------------| 
| 1 | python  | 3.12.9 | 
| 2 | Java | open-jdk-11 | 
| 3 | Scala | 2.12.18 | 

Remark: Don't use different version of Scala and check the stable scala version if you want to change the 3.5.x to 4.x version

Note: All the spark server and worker use spark-3.5.5 version 

# Using make command 
This is only for data plotform to run and all this commands run for various environment and related services.  

| # | command | description | 
|:-:|:-------:|:------------| 
| 1 | make build | build docker images for customized spark, airflow, elastic and jupyter | 
| 2 | make dev-jupyter-up | up the spark master, jupyter and minio server |
| 3 | make dev-streaming-up | up the kafka, debezium, spark and postgres |  
| 4 | make dev-scheduler-up | up the airflow ,spark, postgres and minio | 
| 5 | make dev-cdc-up | up the CDC workspace, kafka, debezium and postgres | 
| 6 | make dev-dbt-up | up the dbt server using dbt core, postgres -> data analysis and transformation |
| 7 | make dev-elk-up | up the elastic search | 
| 7 | make dev-elk-down | down the elastic search |
| 8 | make dev-up-all-services | run all service exclude elastic stack | 
| 9 | make dev-down-all-services | down all service | 
| 10| make dev-clean-all-services | clean docker images | 
| 11| make dev-rebuild-all-services | rebuild all services | 
| 12| make dev-spark-shell | enter the spark master shell | 
| 13| make dev-jupyter-shell | enter the jupyter server | 
| 14| make dev-postgres-shell | enter the postgres shell | 
| 16| make dev-airflow-webserver| enter the airflow webserver shell |
| 17| make dev-airflow-scheduler| enter the airflow scheduler shell | 
| 18| make dev-redis | enter the redis shell | 
| 19| make dev-dbt-shell | enter the dbt shell |
| 20| make dev-airflow-dags-list | show the dags lists in the system | 
| 21| make dev-kafka-logs | show the kakfa log | 
| 22| make dev-spark-logs | show the spark master log | 
| 23| make dev-spark-worker-logs | show the spark worker log | 
| 24| make dev-airflow-logs | show the airflow webserver log | 
| 25| make dev-dbt-debug | debug the dbt after init | 
| 26| make dev-airflow-init-db | init the airflow database | 
| 27| make dev-airflow-migrate | migrate the database from SQLite to  | 
| 28| make dev-airflow-create-user | create the airflow user in dev | 

## Stop the required services 
docker compose down  


For other environment such as QUA, UAT, PRD please change as 
- qua-cdc-up
- uat-cdc-up
- prd-cdc-up 

# Environment file 
Remove sample from the file and edit the reqiured file such as 
user name, password etc 

| # | environment file | use file name |description | 
|:-:|:----------------:|:-------------:|:-----------:|
| 1 | sample.env | .env |  environment file for jupyter notebook and postgresql | 
| 2 | sample.env.spark | .env.spark | spark environment file | 
| 3 | sample.env.airflow | .env.airflow |airflow environment file |  
| 4 | sample.env.minio | .env.minio | minio enviroment file |

REMARK: edit and make sure the parameters before image  build and docker compose up 

# Folders Description 
| # |  Folder |description | 
|:-:|:-------:|:-----------:|
| 1 | asserts | pictures folder | 
| 2 | confs | spark configuration file | 
| 3 | dags | apache airflow dags file folder | 
| 4 | data | data folder for spark and pyspark script | 
| 5 | data-modeling | for data modeling |
| 6 | data-platfrom | next comming IaC | 
| 7 | dbt | data build tool for tranformation and analytics |  
| 8 | docker | docker files location | 
| 9 | init-sql | sql related folders | 
| 10 | kafka-config | kakfa configuration file |
| 11 | kafka-data | presistance kafka data store | 
| 12 | logs* | auto generate from apache airflow| 
| 13 | notebooks | location for data analysis and data scientist | 
| 14 | pg_data* | auto generate and volumn mount for postgresql | 
| 15 | plugins | Apache Airflow plugins file here | 
| 16 | reports | report generators | 
| 17 | result-jar| jar files collection and use in spark-submit | 
| 18 | sap-data | sample sap data from Kaggel | 
| 19 | spark-apps| scala and python development folder | 
| 20 | spark-logs| external log folder for spark| 
| 21 | lakehouse | data lakehouse using Minio and Delta table, this is and exter folder  | 


REMARK: Don't run make rebuild or make clean. It will take more tike to build a docker image and repulling the required images. 

# Buinses Domain (logistic, Finance)

- SAP (SD, FICO, COPA) and 
- Data catalog and data lineage 


# BUILD and Setup 
  
  This section is only for 1st time installation and using of this repo. After that, you can use all make command. This is the building of data paltform command for data lakehouse. 

## BUILD 
Run those command in GNU Linux or WSL terminal  
```shell 
make build 
```
it needs to wait couple of minutes to build and settle the images. 

## Setup 
1. postgresl for data and  
2. apache airflow

### postgresql setup
Enter the postgresql image by `make pg` to create airflow user name and password

### psql command to enter the postresql 
```
psql -U sparkuser -d sparkdb
```
### load data to SQL
```sql 
\i /path/to/07_dml_seed_crm_orders.sql
```
### create and insert all seed values in pg using init.sh 
Remark: init.sh is in the sql_scripts folder
```shell 
./init.sh 
```
### Airflow setup

```sql
-- Create a new database for Airflow
CREATE DATABASE airflowdb;

-- Create a new user with a secure password
CREATE USER airflow WITH PASSWORD 'airflowpass';

-- Grant privileges to the new user
GRANT ALL PRIVILEGES ON DATABASE airflowdb TO airflow;

-- change to airflowdb 
\c airflowdb

-- Grant privileges to public schema to airflow 
GRANT ALL ON SCHEMA public TO airflow;
```

- Run this command to initialize apache airflow webserver `make airflow-init-db` and then run other left commands 
```Shell 
make airflow-migrate 
make airflow-create-user
```
Airflow is ready to use, but all the example dags are not shown as development environment. 

### Airflow build for new operator 
```shell 
docker build -t custom-airflow:2.9.2 -f docker/airflow.Dockerfile .
```

### check for the docker run 
```sh
docker run -it --rm custom-airflow:2.9.2 airflow version
```

### Airflow is using DockerOperator 
```
DOCKER_GID=$(getent group docker | cut -d: -f3) 
```


### Airflow UI Admin and password 
```sh 
# Create a new admin user
airflow users create \
    --username airflowAdmin \
    --firstname airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password airflow1234
```

### Airflow CLI commands 

| # | cli | description | 
|:-:|:---:|:-----------:|
|1| airflow dags list | list all dags | 
|2| airflow dags list-runs --dag-id <dag_id> | show the running dags status | 

### Airflow operator use in this project

- Spark 
- Kakfa 
- Bash 
- dbt 

### Airflow variables setup 
![System Architecture](asserts/sparkVariable.png)

# Login pages 
This project supports to use UI
1. Spark master UI 
2. Spark work 
3. Spark history 
4. Jupyter notebook 
5. airflow web UI 
6. Minio UI 
7. Elastic UI

## Check the spark master UI in web
After building the docker images and up the process, check the spark UI

| # | url | Description | 
|:-:|:---------------------:|:------------:|
| 1 | http://localhost:8080 | spark master | 
| 2 | http://localhost:8081 | spark worker | 
| 3 | http://localhost:18080| spark histoircal server | 
| 4 | http://localhost:8088 | airflow web ui | 
| 5 | http://localhost:4040 | spark application | 
| 6 | http://localhost:8888 | Jupyter UI | 
| 7 | http://localhost:9021 | Kafka control UI | 
| 8 | http://localhost:9000 | Minio UI | 
| 9 | http://localhost:5601 | Elasitc UI | 


REMARK: Jupyter needs to use access token each time 
TODO: username and password based login 
TODO: add SSL for for secure in production deployment  

e.g. of Spark UI 
![Spark Architecture](asserts/spark-master.png)


## Apache airflow integration 
This project use the Apache airflow as a data orchestrator. It supports to run spark-submit and others jobs.  
e.g. of airflow UI 
![Airflow UI](asserts/airflowUI.png)

## Kafka Control center
For streaming data 
![Kafka Control center](asserts/confluenct.png)

## Debezium connection 
- To run connect CDC postgresql 

```shell 
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "youruser",
      "database.password": "yourpass",
      "database.dbname": "yourdb",
      "database.server.name": "pgserver",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_slot",
      "publication.name": "debezium_pub",
      "table.include.list": "public.your_table"
    }
  }'
```

## DBT (data build tool)
The folder is using underscore, because this should be managed by python script

### init the shell 
```shell 
make dbt-shell 
```
### create sbt scaffold 
in the shell start this 
```shell 
dbt init sap_landing
```

### host machine 
For docker to read profile.yml from host machine 
```shell 
cp sap_dbt 
mkdir .dbt
cp profiles/profiles.yml .dbt/
sudo chmod $USER:$USER .dbt/profiles.yml
```

DBT debug cannot run for the init so, use some tweak to initailize 
```shell 
cp sap_dbt 
mv sap_landing/* . 
sudo chmod $USER:$USER -R sap_dbt
```

add the data to seeds folder [get data from Kaggel SAP]
```shell 
curl -L -o ~/Downloads/sap-dataset-bigquery-dataset.zip\
  https://www.kaggle.com/api/v1/datasets/download/mustafakeser4/sap-dataset-bigquery-dataset
```
REMARK TESTING PURPOSE I reduce bseg and big csv file to smaller chunk 5000 records 


## Sample ER diagarm
This is the sample ER diagram of Old ORM system without having a proper columns name. 
![ERD diagram](asserts/ERD.png)

### create the sigle view in postgresql 
This is the sample of single view in postgresql before deploy on the Scala code. 
It can only check for Monolithic application. 
SQL pipeline make sure the micro services into a single landing like a monolithic structure. 
![Single View](asserts/postsqlDBeaver.png)

# Scala and Python 
This section is about how to build the scala based application and submit the jar to spark server. In this project, we can build our jar file on server instead of using host machine. But, in real production environment we should build on provided machine for safety. 

### Software lifecycle 
- DEV, create the jar file and run on this dockers 
- QUA QA test, This is very difficult to check the Data As A Product 
- UAT, This is also difficult to do in DAAP 
- PRD, Use the Jar file and integrate with Airflow 

### Data lifecycle 
- DEV, get the sample dataset from production [Note: not use dummy data]
- QUA, check the data quality, data drift, schema evolution, and report dashboard 
- UAT, internal check and run with production data and check on the clone dashboard 
- PRD, Use the pass Jar file or python script to run on server 


### Installation in development environment 
  - Install Java [open-jdk-11]
  - Install cs 
  - Install Scala using cs [scala 2.12.18]
  - Install Python 3.12.9 
  - install sbt 
Check the sbt version 

### Scala apps [spark-apps]
  - Create a folder structure as follow in your local machine 
  - create a spark folder in `/opt/spark/job/scala`

#### create required folders 
```shell 
mkdir -p spark-apps/pyspark
mkdir -p spark-apps/scala 
mkdir -p spark-apps/scala/lib spark-apps/scala/src
```
#### create a build file 
- add the required dependencies in build.sbt
```shell 
name := "oldSingleViewCRM"

version := "0.1"

scalaVersion := "2.12.18"

// libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "3.5.5" % Provided,
  "org.apache.spark" %% "spark-sql"  % "3.5.5" % Provided,
  "org.scalatest"    %% "scalatest"  % "3.2.17" % Test ,
  "org.postgresql" % "postgresql" % "42.7.1" % Runtime
)
```

#### Folder structure 
```Shell 
  .
├── pyspark
│   ├── README.md
│   ├── src
│   │   └── read_txt_log.py
│   └── test
└── scala
    ├── build.sbt
    ├── lib
    │   └── postgresql-42.7.1.jar    
    └── src
        ├── main
        │   └── scala
        │       ├── oldSingleViewCRM.scala
        │       ├── readPg.scala
        │       └── readTxtLog.scala
        └── test
            └── readTxtLogTest.scala
```

###  Build the jar file 
using make dev and do the following steps 
```shell 
cd /opt/spark/jobs/scala
sbt package 
```

### output jar location 
The path contains the output of sbt compiler 
```
/opt/spark/jobs/scala/target/scala-2.12
```
### copy jar file to spark server
if the jar file build in host or local machine copy jar file and submit to spark  server. 
Change the Jar file location in spark-submit, Following spark-submit is only for creating a jar file in the spark server. 

### Jar file submit 
    This process needs to do in the master node and run this command 
```shell 
/opt/spark/bin
```
Then run the spark-submit to get the result
```shell 
spark-submit \
  --class readTxtLog \
  --master spark://spark-master:7077 \
  --conf spark.input.path=/opt/spark/data/logs.txt \
  --conf spark.log.type=INFO \
  --conf spark.interface.name=VTLINK \
  /opt/spark/jobs/scala/target/scala-2.12/sparkrdd_2.12-0.1.jar
```
### Pyspark submit
To test with PySpark 
```shell 
PYSPARK_DRIVER_PYTHON=python3
 spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.input.path=/opt/spark/data/logs.txt \
  --conf spark.log.type=INFO \
  --conf spark.interface.name=VTLINK \
  /opt/spark/jobs/pyspark/src/read_txt_log.py
```

# Troubleshooting 

### Docker network 

##### list all docker network 
```sh 
docker network ls 
```

##### check the docker image own network 

 - check for the airflow webserver
```sh 
docker inspect airflow-webserver --format '{{json .NetworkSettings.Networks}}'
```


### mount permission 
#### command to check the docker sock
```sh 
docker compose exec -T airflow-scheduler ls -l /var/run/docker.sock
```
#### check the docker group 

```sh 
getent group docker | cut -d: -f3
```
Note: put the docker group id to env file -> .env.airflow 

### GID to .env
```
echo "DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)" > .env.docker-gid
```

### Check the defautl route to interactive with DBeaver 
```shell 
ip route | grep default 
```
The Sample output is -> default via 172.23.224.1 dev eth0 proto kernel 

### Postgresal connection to DBeaver and check for single view
Add the requried data to connection and use the default IP addess get from the ip route, in this case the jdbc connecting is using 172.23.224.1 this ip address

### Spark submit fail 
- Build Scala version and Spark server Scala version mismatch 
- File path 
- Postgresql user name and password 
- postgresql jdbc connector location issue 
- with extra python module, create a zip file first create a zip in that folder 
```
cd ~/BigData/enterprise-data-lakehouse/spark-apps/pyspark/src
zip -r utilities.zip utilities
```
- using direct connection to s3 instead of using a single utilities script 

### Debezium connector checking 
```bash
curl http://localhost:8083/connector-plugins
```
Remark: it needs to run double to get the response from server 

if the above command is not reply anything check with this command 
```bash 
docker exec -it debezium curl http://localhost:8083/connector-plugins
```
### DBT 
- cannot run debug command, check the profile 
- parse error in VScode check the DBT operator can read the profile or not [if dbt debug can run ignore it]

### Kafka cannot write log 
```bash 
sudo chown -R 1000:1000 ./kafka-data
sudo chmod -R 755 ./kafka-data
```

### Debezuim connector type 
- Avro 
- Json 


# check the process in UI
we can check the spark process in UI and it is also a dag.  

![Spark Architecture](asserts/spark-application.png)


# Log retention 
- delete the log after 15 days 

# Data compliance
- General Data Protection Regulation (GDPR) 
- Health Insurance Portability and Accountability Act (HIPAA) 
- Payment Card Industry Data Security Standards (PCI-DSS)

# Business values 
- Same report for OLD CRM system is monolithic and some are running in Mirco service 
- Real time data analysis support
- Data As A Product 
- Compliant checking using AI
- report generation in pdf and in excel 
- ON DEMANd ad-hoc report supprot 
- Master data management 
- SAP and Salesfoce data as simple [SD, FICO, MW and MM ]
- SAP in lakehouse sample and lineage  

# Next plan 
- Add Openlineage for data lineage 
- Add Openmetadata for Metadaa 
- Full ETL pipeline in Scala 
- DataVault 2.0 and SCD type 2 for address and historical tracking 
- CI/CD 
- Data profiler 


