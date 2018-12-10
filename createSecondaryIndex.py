import pandas as pd
from configparser import ConfigParser
from sqlalchemy import create_engine
import argparse

import logging
logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

conf = ConfigParser()
conf.read('conf.ini')

# make connection
def mysql_engine(type):

    if type == "index":
        mysql = conf["sec-index"]
    elif type == "data":
        mysql = conf["mysql"]

    user_name = mysql["USER_NAME"]
    pwd = mysql.get("PASSWORD", '')
    host = mysql.get("HOST_NAME", "localhost")
    port = mysql["PORT"]
    db = mysql["DB_NAME"]
    
    engine = create_engine(f"mysql+pymysql://{user_name}:{pwd}@{host}:{port}/{db}?charset=utf8mb4")
    return engine


def parse_table(table):

    # connect to data source
    try:
        engine = mysql_engine("data")
        conn = engine.connect()

    except Exception as e:
        logger.debug(f"Unable to connect to database.\nError: {e}")
        return

    # delete index if exists for the table and re-create it
    try:
        engine.execute(f"delete from sec_index where dataset='{table}'")
    except Exception as e:
        logger.debug(e)

    # read data
    df = pd.read_sql(f"select * from {table} limit 10", conn);
    # df.to_csv("product.csv", index=False)
    # df = pd.read_csv("product.csv")
    # df.info()

    # Secondary Index schema
    secondary_index = pd.DataFrame([], columns=["key", "completion", "column", "dataset"])
    i = 0 # idx locator
    
    # Put All Columns, Infer data types
    for x in df.columns:
        secondary_index.loc[i] = [x, x, "", table]
        i+=1

    # iterate over categorical columns and add each value to index dataframe
    catCols = df.select_dtypes(include=object).columns
    for column in catCols:
        distinct_values = df[column].unique()
        for value in distinct_values:
            keys = value.split()
            for key in keys:
                secondary_index.loc[i] = [key, value, column, table]
                i+=1

    # store secornday index in mysql
    try:
        index_engine = mysql_engine("index")
        index_conn = index_engine.connect()
        secondary_index.to_sql("sec_index", index_conn, if_exists="append")
    except Exception as e:
        logger.debug(f"Unable to store index due to: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("table", help="name of table")
    args = parser.parse_args()
    parse_table(args.table)
    