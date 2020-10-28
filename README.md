# NUS-CS5424-Project

## Setup

### Setup Cassandra & Datastax Python Driver
https://cassandra.apache.org/doc/latest/getting_started/index.html <br>
https://docs.datastax.com/en/developer/python-driver/3.24/

### Setup Python Environment
```
pip install pipenv
pipenv shell
pipenv install
```

### Download Data
```
wget http://www.comp.nus.edu.sg/~cs4224/project-files.zip
unzip project-files.zip
```

## Preparation
Start Cassandra.
```
cd /path/to/cassandra (in the cluster: cd /temp/apache-cassandra-4.0-beta2/)
bin/cassandra
```
Create tables & insert initial data.
```
python create_tables.py
python insert_records.py
```
