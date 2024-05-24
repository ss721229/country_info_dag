from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import json
import logging
from time import sleep
from datetime import datetime

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_country_info():
    logging.info("extract and transform started")
    try:
        response = requests.get("https://restcountries.com/v3/all")
        data = json.loads(response.text)

        country = [data[i]['name']['official'] for i in range(len(data))]
        population = [data[i]['population'] for i in range(len(data))]
        area = [data[i]['area'] for i in range(len(data))]

        logging.info("extract and transform done")
        return [[c, p, a] for c, p, a in zip(country, population, area)]
    except Exception as e:
        logging.error("error during getting country info", exc_info=True)
        raise
    

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
                    CREATE TABLE {schema}.{table} (
                        country varchar(100),
                        population int,
                        area int );
                    """)
        for r in records:
            cur.execute(f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s)", r)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error("error during load", exc_info=True)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("load done")

with DAG(
    dag_id = 'hw_country_info',
    start_date = datetime(2024, 5, 23),
    catchup = False,
    tags = ['API'],
    schedule = '30 6 * * 6'
) as dag:

    results = get_country_info()
    load("ss721229", "country_info", results)