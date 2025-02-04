from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import json
import boto3
from datetime import datetime

# 전역 상수 설정
S3_BUCKET_NAME = "yongsun-test-t2"
AREA_DATA_KEY = "final/area_data.json"
CATE_DATA_KEY = "final/cate_data.json"
TOTAL_DATA_KEY = "final/total_data.json"
REDSHIFT_CONN_ID = 'redshift_conn'


# Redshift 연결 및 테이블 생성 함수 - area_data
def create_area_data_table():
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # 테이블이 없으면 생성하는 SQL
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_data.area_data (
        month INT,
        area VARCHAR(100),
        performances INT,
        total_tickets_sold INT,
        sale_amount BIGINT,
        tickets_sold INT,
        tickets_cancled INT,
        play_counts INT,
        seats INT,
        venues INT,
        locations INT,
        openings_counts INT
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Redshift 연결 및 테이블 생성 함수 - cate_data
def create_cate_data_table():
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # cate_data 테이블 생성 SQL
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_data.cate_data (
        month INT,
        cate VARCHAR(100),
        tickets_sold INT,
        sale_amount BIGINT,
        audience_ratio FLOAT,
        sale_amount_ratio FLOAT,
        play_counts INT,
        openings_counts INT
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Redshift 연결 및 테이블 생성 함수 - total_data
def create_total_data_table():
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_data.total_data (
        month INT,
        date DATE,
        performances INT,
        total_tickets_sold INT,
        sale_amount BIGINT,
        tickets_sold INT,
        tickets_cancled INT,
        play_counts INT,
        openings_counts INT
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# S3에서 area_data.json 파일을 읽어와서 Redshift에 적재하는 함수
def load_area_data_to_redshift():
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # S3에서 area_data.json 파일 가져오기
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=AREA_DATA_KEY)
    data = json.loads(obj['Body'].read().decode('utf-8'))

    # JSON 데이터에서 필요한 정보 추출
    for month_data in data:
        month = month_data['month']
        for performance in month_data['prfst']:
            area = performance['area']
            prfcnt = int(performance['prfcnt'])
            totnmrs = int(performance['totnmrs'])
            amount = int(performance['amount'])
            nmrs = int(performance['nmrs'])
            nmrcancl = int(performance['nmrcancl'])
            prfdtcnt = int(performance['prfdtcnt'])
            seatcnt = int(performance['seatcnt'])
            fcltycnt = int(performance['fcltycnt'])
            prfplccnt = int(performance['prfplccnt'])
            prfprocnt = int(performance['prfprocnt'])

            # Redshift에 데이터 삽입
            insert_sql = """
            INSERT INTO raw_data.area_data (
                month, area, performances, total_tickets_sold, sale_amount, tickets_sold, tickets_cancled, 
                play_counts, seats, venues, locations, openings_counts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_sql, (month, area, prfcnt, totnmrs, amount, nmrs, nmrcancl, prfdtcnt, seatcnt, fcltycnt, prfplccnt, prfprocnt))
    
    conn.commit()
    cursor.close()
    conn.close()

# S3에서 cate_data.json 파일을 읽어와서 Redshift에 적재하는 함수
def load_cate_data_to_redshift():
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # S3에서 cate_data.json 파일 가져오기
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=CATE_DATA_KEY)
    data = json.loads(obj['Body'].read().decode('utf-8'))

    # JSON 데이터에서 필요한 정보 추출
    for month_data in data:
        month = month_data['month']
        for performance in month_data['prfst']:
            cate = performance['cate']
            amount = int(performance['amount'])
            nmrs = int(performance['nmrs'])
            prfdtcnt = int(performance['prfdtcnt'])
            nmrsshr = float(performance['nmrsshr'])
            prfprocnt = int(performance['prfprocnt'])
            amountshr = float(performance['amountshr'])

            # Redshift에 데이터 삽입
            insert_sql = """
            INSERT INTO raw_data.cate_data (
                month, cate, tickets_sold, sale_amount, audience_ratio, 
                sale_amount_ratio, play_counts, openings_counts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_sql, (month, cate, nmrs, amount, nmrsshr, amountshr, prfdtcnt, prfprocnt))
    
    conn.commit()
    cursor.close()
    conn.close()

# S3에서 total_data.json 파일을 읽어와서 Redshift에 적재하는 함수
def load_total_data_to_redshift():
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    # S3에서 total_data.json 파일 가져오기
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=TOTAL_DATA_KEY)
    data = json.loads(obj['Body'].read().decode('utf-8'))

    for month_data in data:
        month = int(month_data['month'])
        for performance in month_data['prfst']:
            date = datetime.strptime(performance['prfdt'], "%Y%m%d").date()
            prfcnt = int(performance['prfcnt'])
            ntssnmrs = int(performance['ntssnmrs'])
            cancelnmrs = int(performance['cancelnmrs'])
            amount = int(performance['amount'])
            nmrs = int(performance['nmrs'])
            prfdtcnt = int(performance['prfdtcnt'])
            prfprocnt = int(performance['prfprocnt'])

            insert_sql = """
            INSERT INTO raw_data.total_data (
                month, date, performances, total_tickets_sold, sale_amount, tickets_sold, tickets_cancled, play_counts, openings_counts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_sql, (month, date, prfcnt, ntssnmrs, amount, nmrs, cancelnmrs, prfdtcnt, prfprocnt))
    
    conn.commit()
    cursor.close()
    conn.close()


# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

dag = DAG(
    "load_s3_to_redshift",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False
)

# 테이블 생성 태스크 - area_data
create_area_table_task = PythonOperator(
    task_id="create_area_data_table",
    python_callable=create_area_data_table,
    dag=dag,
)

# 테이블 생성 태스크 - cate_data
create_cate_table_task = PythonOperator(
    task_id="create_cate_data_table",
    python_callable=create_cate_data_table,
    dag=dag,
)

# 테이블 생성 태스크 - total_data
create_total_table_task = PythonOperator(
    task_id="create_total_data_table",
    python_callable=create_total_data_table,
    dag=dag,
)

# 데이터 적재 태스크 - area_data
load_area_data_task = PythonOperator(
    task_id="load_area_data_to_redshift",
    python_callable=load_area_data_to_redshift,
    dag=dag,
)

# 데이터 적재 태스크 - cate_data
load_cate_data_task = PythonOperator(
    task_id="load_cate_data_to_redshift",
    python_callable=load_cate_data_to_redshift,
    dag=dag,
)

# 데이터 적재 태스크 - total_data
load_total_data_task = PythonOperator(
    task_id="load_total_data_to_redshift",
    python_callable=load_total_data_to_redshift,
    dag=dag,
)

# 테이블 생성 후 데이터 적재
create_area_table_task >> load_area_data_task
create_cate_table_task >> load_cate_data_task
create_total_table_task >> load_total_data_task