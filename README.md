# dgen
generate python source code to access database(mysql) like select, update, insert, delete of sql.
I just need to access database source code simply. 
I do not need the huge o/r mapper.

## usage
```
usage: dgen.py [-h] [--host HOST] [--db_name DB_NAME] [--user USER]
               [--passwd PASSWD]

generate source code of database

optional arguments:
  -h, --help         show this help message and exit
  --host HOST        enter database hostname
  --db_name DB_NAME  enter database name
  --user USER        enter user name
  --passwd PASSWD    enter password
```


## prerequisites
- python3.x
- virtualenv

## installation
```
cd ~/git
git clone git@github.com:tmy310/dgen.git
cd dgen/

virtualenv env
source env/bin/activate
pip install -r requirements.txt
```
virtualenv directory name must be `env` .

## sample
```
# create sample env
mysql -u root  < create_test_env.sql

# just input hostname, database_name, user_name, password
python dgen.py --host localhost --db_name test_database --user root --passwd ''

# execute sample file. you can use select, update, insert, delete.
python sample.py
```
