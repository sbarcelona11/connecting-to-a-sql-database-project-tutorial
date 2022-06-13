'''
Instalar:
- pip install PyMySQL
- pip install sqlparse
'''
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import sqlparse

load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
def connect():
    connection_string = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}?autocommit=true"
    print("url", connection_string)
    print("Starting the connection...")
    engine = create_engine(connection_string)
    engine.connect()
    return engine

def execute_action_on_db(db_file, connection):
    with open(db_file) as f_sql:
        sql_raw = f_sql.read()
        
    sql_queries = sqlparse.split(
        sqlparse.format(sql_raw, strip_comments=True)
    )

    for query in sql_queries:
        result = connection.execute(text(query))
        print(f"{result.rowcount} rows have been updated/selected.")

engine = connect()
if(engine):
    try:
        # drop data
        execute_action_on_db("./sql/drop.sql", engine)
        # 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
        execute_action_on_db("./sql/create.sql", engine)
        # 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function
        execute_action_on_db("./sql/insert.sql", engine)
        # 4) Use pandas to print one of the tables as dataframes using read_sql function
        print(pd.read_sql("SELECT * FROM books;", engine))
    except Exception as error:
        print('Error', error)
else:
    print('Don`t have connection to DB')