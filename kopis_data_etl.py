import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 전역 변수 설정
API_SERVICE_KEY = '31ae519c31784b11ae0bb632c4d018f7'
REDSHIFT_CONN_ID = 'redshift_conn'

# KOPIS API의 지역 코드 리스트
AREAS = [
    "11", "28", "30", "27", "29", "26", "31", "36", "41", "43",
    "44", "47", "48", "45", "46", "51", "50", "UNI"
]

# 테이블 생성 SQL
CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS raw_data.ticket_sales (
        execution_date DATE,
        mt20id VARCHAR(100),
        name VARCHAR(255),
        cate VARCHAR(100),
        rank INT,
        area VARCHAR(100),
        venue VARCHAR(255),
        period VARCHAR(100),
        seats INT,
        play_counts INT,
        created_at TIMESTAMP DEFAULT GETDATE()
    );
"""

# API에서 데이터 가져오기
def fetch_kopis_data(**kwargs):
    execution_date = kwargs['ds']  # YYYY-MM-DD 형식
    yesterday = (datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y%m%d')

    all_data = []

    for area in AREAS:
        url = f'http://kopis.or.kr/openApi/restful/boxoffice?service={API_SERVICE_KEY}&stdate={yesterday}&eddate={yesterday}&area={area}'
        response = requests.get(url)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data for area {area}: {response.status_code}, Response: {response.text}")

        root = ET.fromstring(response.text)
        for boxof in root.findall('boxof'):
            item = {
                'mt20id': boxof.find('mt20id').text,
                'prfnm': boxof.find('prfnm').text,
                'cate': boxof.find('cate').text,
                'rnum': int(boxof.find('rnum').text),
                'area': boxof.find('area').text,
                'prfplcnm': boxof.find('prfplcnm').text,
                'prfpd': boxof.find('prfpd').text,
                'seatcnt': int(boxof.find('seatcnt').text),
                'prfdtcnt': int(boxof.find('prfdtcnt').text),
            }
            all_data.append(item)

    return {"execution_date": execution_date, "data": all_data}

# Redshift에 데이터 적재
def load_data_to_redshift(**kwargs):
    ti = kwargs['ti']
    fetched_data = ti.xcom_pull(task_ids='fetch_kopis_data')
    
    if not fetched_data:
        raise ValueError("No data received from fetch_kopis_data")

    execution_date = fetched_data['execution_date']
    data = fetched_data['data']

    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()  # 커넥션 유지
    cur = conn.cursor()

    try:
        # 임시 테이블 생성
        cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS temp_ticket_sales (
            execution_date DATE,
            mt20id VARCHAR(100),
            name VARCHAR(255),
            cate VARCHAR(100),
            rank INT,
            area VARCHAR(100),
            venue VARCHAR(255),
            period VARCHAR(100),
            seats INT,
            play_counts INT,
            created_at TIMESTAMP DEFAULT GETDATE()
        );
        """)

        # 데이터 삽입
        insert_temp_data_query = """
        INSERT INTO temp_ticket_sales (
            execution_date, mt20id, name, cate, rank, area, venue, period, seats, play_counts, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE());
        """
        
        for item in data:
            cur.execute(insert_temp_data_query, (
                execution_date, item['mt20id'], item['prfnm'], item['cate'],
                item['rnum'], item['area'], item['prfplcnm'], item['prfpd'],
                item['seatcnt'], item['prfdtcnt']
            ))

        # 중복 제거: 최신 데이터만 남기기 (ROW_NUMBER 사용)
        deduplicate_query = """
        DELETE FROM raw_data.ticket_sales;

        INSERT INTO raw_data.ticket_sales
        SELECT execution_date, mt20id, name, cate, rank, area, venue, period, seats, play_counts, created_at
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY mt20id ORDER BY created_at DESC) AS rn
            FROM temp_ticket_sales
        ) t WHERE rn = 1;
        """
        
        cur.execute(deduplicate_query)  # 중복 제거 및 데이터 삽입
        conn.commit()  # 트랜잭션 커밋

    except Exception as e:
        conn.rollback()  # 오류 발생 시 롤백
        raise Exception(f"Error occurred while loading data to Redshift: {str(e)}")
    finally:
        cur.close()
        conn.close()

# DAG 정의
with DAG(
    'kopis_data_etl',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 2, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=lambda: PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID).run(CREATE_TABLE_SQL)
    )
    
    fetch_data_task = PythonOperator(
        task_id='fetch_kopis_data',
        python_callable=fetch_kopis_data
    )
    
    load_data_task = PythonOperator(
        task_id='load_data_to_redshift',
        python_callable=load_data_to_redshift
    )
    
    create_table_task >> fetch_data_task >> load_data_task