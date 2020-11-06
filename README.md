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

### Download Experiment Data
```
wget http://www.comp.nus.edu.sg/~cs4224/project-files.zip
unzip project-files.zip
```

## Preparation
To run in a single machine: modify ```IP_ADDRESS``` in ```create_tables.py``` to '127.0.0.1'  

To run in a cluster: follow https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/initialize/initSingleDS.html to configure cassandra environment, then modify ```IP_ADDRESS``` in ```create_tables.py``` to a seed address. 

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

## Run Experiments & Collect Statistics
Test a single transaction:
```
vim queries.py
uncomment the query you want to try out, save & exit
python queries.py
```
Run a client:
```
python run_xact_file.py [experiment number] [client number]
```
Run experiment: 
```
bash run_experiment [experiment number] [node number]
```
Collect statistics: 
```
python collect_database_state.py
```
