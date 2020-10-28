# NUS-CS5424-Project

## Setup

### Setup Cassandra
https://cassandra.apache.org/doc/latest/getting_started/index.html

### Setup Python Environment
You may use pipenv,
```
pip install pipenv
pipenv install
pipenv shell
```
Or manually install
```
pip install cassandra-driver
pip install tqdm
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

## Run Experiments
syntax: bash run_experiment \[experiment number] \[node number]
example:
```
bash run_experiment 1 1
```
