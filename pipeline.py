import pandas as pd
import mysql.connector
import logging
import json
import os
from logging.handlers import RotatingFileHandler

def get_config():
    with open("config.json","r") as f:
        config=json.load(f)
        return config
    
config=get_config()
table_name=config["table_name"]
req_columns=config["req_columns"]
    
logger=logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

file_handler=RotatingFileHandler("pipeline.log",maxBytes=1000000,backupCount=3)
formatter=logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(file_handler)


def extract():
    try:
        data=pd.read_csv(config["file_name"])
        logger.info("Data Extracted successfully.")
        return data
    except Exception as e:
        logger.error(f"Data Extraction failed: {e}")
        raise

def transform(data):
    try:
        data["marks"]=data["marks"].fillna(0)
        data["marks"]=data["marks"].astype(int)

        data["age"]=data["age"].fillna(round(data["age"].mean()))
        data["age"]=data["age"].astype(int)
        logger.info("Data Transformed successfully.")
        return data

    except Exception as e:
        logger.error(f"Data tranformation failed: {e}")
        raise

def validate(data):
    try:
        #check for required columns
        for col in req_columns:
            if col not in data.columns:
                logger.error(f"{col} is missing.")
                raise ValueError(f"{col} is missing.")
            
        #check for nulls 
        if data.isnull().sum().sum()>0:
            logger.error("Null Values still exist.")
            raise ValueError("Null Values still exist.")

        #check for data type
        if not pd.api.types.is_integer_dtype(data["marks"]):
            logger.error("marks should be integer.")
            raise ValueError("marks should be integer.")
        
        if not pd.api.types.is_integer_dtype(data["age"]):
            logger.error("age should be integer.")
            raise ValueError("age should be integer.")
        
        logger.info("Data Validation Passed.")
        return data

    except Exception as e:
        logger.error(f"Data Validation Failed: {e}")
        raise

def load(data,table_name):
    allowed=["students","team","staff"]
    if table_name not in allowed:
        logger.error(f"{table_name} is not a valid table  name.")
        raise ValueError(f"{table_name} is not a valid table  name.")

    db_pass=os.getenv("DB_PASS")
    
    conn=None
    cursor=None

    try:
        conn=mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            database=config["database"],
            password=db_pass
        )

        cursor=conn.cursor()

        sql=f"insert into {table_name}({','.join(req_columns)}) values({','.join(['%s']*len(req_columns))})"
        values=[tuple(x) for x in data[req_columns].to_numpy()]
#        cursor.execute(f"truncate {table_name}")
        cursor.executemany(sql,values)
        conn.commit()
        logger.info("Data loaded Successfully.")

    except Exception as e:
        logger.error(f"Data Loading Failed: {e}")
        raise

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

if __name__=="__main__":
    data=extract()
    data=transform(data)
    data=validate(data)
    load(data,table_name)