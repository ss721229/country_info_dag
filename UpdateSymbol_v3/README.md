# AAPL 주식 정보 DAG

## &nbsp;&nbsp;&nbsp;개요
- python의 yfinance 라이브러리 사용
- 해당 라이브러리를 사용하여 애플(AAPL)의 한 달 간 주식 데이터 수집

## &nbsp;&nbsp;&nbsp;DAG
- **Redshift 연결** : Airflow의 Connection 기능과 PostgresHook을 통해 민감한 정보 표시 X
``` python
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()
```

- **테이블 생성** : 첫 실행 시 테이블 생성, 원본 테이블 생성
``` python
def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date date PRIMARY KEY,
    "open" float,
    high float,
    low float,
    close float,
    volume bigint,
    created_date timestamp default GETDATE()
);""")
```

- **Task (1) - Extarct, Transform** : 라이브러리를 활용한 Extract, date/open/high/low/close/volume Transform
``` python
@task
def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])

    return records
```

- **Task(2) - Load** : 추출한 데이터를 "schema.table" 테이블에 적재
``` python
@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, schema, table, False)
        # 임시 테이블로 원본 테이블을 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO t VALUES ('{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]});"
            print(sql)
            cur.execute(sql)

        # 원본 테이블 생성
        _create_table(cur, schema, table, True)
        # 임시 테이블 내용을 원본 테이블로 복사
        cur.execute(f"""
                    INSERT INTO {schema}.{table}
                    SELECT date, "open", high, low, close, volume
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (partition by date ORDER BY created_date DESC) seq
                        FROM t 
                    )
                    WHERE seq = 1;
                    """)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise
    logging.info("load done")
```

- **DAG** : DAG 정보 입력 및 Task 실행
``` python
with DAG(
    dag_id = 'UpdateSymbol_v3',
    start_date = datetime(2024, 5, 23),
    catchup=False,
    tags=['API'],
    schedule = '0 10 * * *'
) as dag:

    results = get_historical_prices("AAPL")
    load("ss721229", "stock_info_v3", results)
```

## &nbsp;&nbsp;&nbsp;특징
- Task Decorator를 활용한 코드 간략화
- 트랜잭션을 활용한 멱등성, 정합성 보장
- Incremental Update : ROW_NUMBER()를 사용해 PK Uniqueness와 최신 데이터 보장
- created_date(데이터 생성 날짜) 컬럼을 추가해 최신 데이터 보장
- UTC 기준 매일 오전 10시에 실행
- Backfill(catchup) 적용 X

## &nbsp;&nbsp;&nbsp;테스트 with Colab
- Airflow에서 DAG를 실행 후 테이블이 제대로 생성됐는지 확인
``` sql
%%sql
SELECT * FROM ss721229.stock_info_v3 ORDER BY date;
```
<div align='center'>
  <img src="https://github.com/ss721229/country_info_dag/assets/53392184/cbd7914d-79e1-43e0-88c5-8c4421992087">
</div>
<br>

- DAG를 여러 번 실행해도 테이블이 똑같은지 확인 -> 22 동일
``` sql
%%sql
SELECT COUNT(*) FROM ss721229.stock_info_v3;
```
