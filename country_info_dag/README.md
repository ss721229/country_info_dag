# 세계 나라 정보 사용 DAG

## &nbsp;&nbsp;&nbsp;개요
- <a href="https://restcountries.com/v3/all">세계 나라 정보 페이지</a>
- 나라 별 정보를 얻을 수 있으며, 별도의 API Key가 필요 없음
- 해당 API를 사용하여 나라 별 official name(공식 이름), population(인구), area(면적) 데이터 수집
``` json
{"name": {"common": "South Korea", "official": "Republic of Korea", …
"area": 100210.0,
"population": 51780579, …}
```

## &nbsp;&nbsp;&nbsp;DAG
- **Redshift 연결** : Airflow의 Connection 기능과 PostgresHook을 통해 민감한 정보 표시 X
``` python
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()
```

- **Task (1) - Extarct, Transform** : API 호출을 통한 Extract, country/population/area Transform
``` python
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
```

- **Task(2) - Load** : 추출한 데이터를 "schema.table" 테이블에 적재
``` python
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
```

- **DAG** : DAG 정보 입력 및 Task 실행
``` python
with DAG(
    dag_id = 'hw_country_info',
    start_date = datetime(2024, 5, 23),
    catchup = False,
    tags = ['API'],
    schedule = '30 6 * * 6'
) as dag:

    results = get_country_info()
    load("ss721229", "country_info", results)
```

## &nbsp;&nbsp;&nbsp;특징
- Task Decorator를 활용한 코드 간략화
- 트랜잭션을 활용한 멱등성, 정합성 보장
- Full Refresh
- UTC 기준 매주 토요일 오전 6시 30분에 실행
- Backfill(catchup) 적용 X

## &nbsp;&nbsp;&nbsp;테스트 with Colab
- Airflow에서 DAG를 실행 후 테이블이 제대로 생성됐는지 확인
``` sql
%%sql
SELECT * FROM ss721229.country_info LIMIT 10;
```
<div align='center'>
  <img src="https://github.com/ss721229/first-airflow-dag/assets/53392184/cd587ab1-d555-48bb-a74f-b5b39d727448">
  <img src="https://github.com/ss721229/first-airflow-dag/assets/53392184/17a4d92d-7f6e-487d-8934-ebc47c275fbc">
</div>
<br>

- DAG를 여러 번 실행해도 테이블이 똑같은지 확인 -> 250 동일
``` sql
%%sql
SELECT COUNT(*) FROM ss721229.country_info;
```
