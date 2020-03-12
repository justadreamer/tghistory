# tghistory

Import Telegram message history into a PostgreSQL DB

## Setup 

1\. Register a Telegram app at [my.telegram.org/apps](https://my.telegram.org/apps), obtain `API_ID`, `API_HASH`  
2\. Build and install [TDLib](https://github.com/tdlib/td#building)  
3\. Setup a Postgres DB in an arbitrary location.    
4\. Create `config.py` 
```sh
cp config_example.py config.py
```
5\. Set values in the `config.py`:
 - your DB connection string in the form suitable for psycopg initalization: `dbname={DBNAME} user={user} password={password} host={host}`
 - API_ID, API_HASH
 - your phone number to connect to your Telegram account
 - list of chat titles for which you would like to fetch history 

6\. Also install [pipenv](https://github.com/pypa/pipenv) (on a Mac: `brew install pipenv`)  
7\. To make it correctly install `psycopg` on a Mac package export this before running `pipenv install`:
```sh
export LDFLAGS="-L/usr/local/opt/openssl/lib" export CPPFLAGS="-I/usr/local/opt/openssl/include"
```
8\. Run `pipenv install` to install dependencies and create a venv.

## Run
With pipenv you have to switch to your virtualenv using: 
`pipenv shell`

Then just `python extract_thistory.py`

Enter your code to authenticate with Telegram.

DB can be empty schema will be initialized on the first run of the script.

## Queries

`queries.sql` contains sample queries and comments.   