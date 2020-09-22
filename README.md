# NUS-CS5424-Project

## Setup

### Setup Cassandra & Datastax Python Driver
https://cassandra.apache.org/doc/latest/getting_started/index.html
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
```

## Run
First, start Cassandra.
Then, create tables & insert rows:
```
python create_tables.py
python insert_records.py
```
