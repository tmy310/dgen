# dgen
generate source code of database

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
- python2.7 or later
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
mysql -u root  < create_test_env.sql
python dgen.py --host localhost --db_name test_database --user root --passwd ''
python sample.py
```
