from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import requests
import xml.etree.ElementTree as ET
import json
from io import BytesIO
import calendar
from datetime import datetime

# 전역 상수 설정
BUCKET_NAME = "yongsun-test-t2"
SERVICE_KEY = "31ae519c31784b11ae0bb632c4d018f7"

# XML 데이터를 딕셔너리로 변환하는 함수
def xml_to_dict(element):
    result = {}
    for child in element:
        child_data = xml_to_dict(child) if len(child) else child.text
        # 중복 태그를 처리하기 위해 리스트로 묶기
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_data)
            else:
                result[child.tag] = [result[child.tag], child_data]
        else:
            result[child.tag] = child_data
    return result

# S3 클라이언트 설정
s3_client = boto3.client('s3', region_name='us-east-1')

# S3 업로드 함수
def upload_to_s3(data_list, object_key):
    json_str = json.dumps(data_list, indent=4, ensure_ascii=False)
    json_bytes = BytesIO(json_str.encode('utf-8'))
    s3_client.upload_fileobj(json_bytes, BUCKET_NAME, object_key)

# Kopis API 호출 및 데이터 수집 (prfstsArea)
def fetch_kopis_area_data():
    url = "http://www.kopis.or.kr/openApi/restful/prfstsArea"
    year = 2024
    month_ranges = [(f"{year}{month:02}01", f"{year}{month:02}{calendar.monthrange(year, month)[1]}", month) for month in range(1, 13)]
    data_list = []
    object_key = "final/area_data.json"

    for stdate, eddate, month in month_ranges:
        params = {"service": SERVICE_KEY, "stdate": stdate, "eddate": eddate}
        response = requests.get(url, params=params)
            
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            json_data = xml_to_dict(root)
            json_data = {"month": f"{year}{month:02}", **json_data}
            data_list.append(json_data)
        else:
            print(f"Failed to retrieve data for {stdate} - {eddate}. Status code: {response.status_code}")
    
    return data_list, object_key

# Kopis API 호출 및 데이터 수집 (prfstsCate)
def fetch_kopis_cate_data():
    url = "http://www.kopis.or.kr/openApi/restful/prfstsCate"
    year = 2024
    month_ranges = [(f"{year}{month:02}01", f"{year}{month:02}{calendar.monthrange(year, month)[1]}", month) for month in range(1, 13)]
    data_list = []
    object_key = "final/cate_data.json"

    for stdate, eddate, month in month_ranges:
        params = {"service": SERVICE_KEY, "stdate": stdate, "eddate": eddate}
        response = requests.get(url, params=params)
            
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            json_data = xml_to_dict(root)
            json_data = {"month": f"{year}{month:02}", **json_data}
            data_list.append(json_data)
        else:
            print(f"Failed to retrieve data for {stdate} - {eddate}. Status code: {response.status_code}")
    
    return data_list, object_key

# Kopis API 호출 및 데이터 수집 (prfstsTotal)
def fetch_kopis_total_data():
    url = "http://www.kopis.or.kr/openApi/restful/prfstsTotal"
    start_year = 2018
    end_year = 2024
    month_ranges = [
        (f"{year}{month:02}01", f"{year}{month:02}{calendar.monthrange(year, month)[1]}", month)
        for year in range(start_year, end_year + 1)
        for month in range(1, 13)
    ]
    data_list = []
    object_key = "final/total_data.json"

    for stdate, eddate, month in month_ranges:
        params = {
            "service": SERVICE_KEY,
            "stdate": stdate,
            "eddate": eddate,
            "ststype": "day"
        }
        response = requests.get(url, params=params)
            
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            json_data = xml_to_dict(root)
            json_data = {"month": f"{stdate[:4]}{stdate[4:6]}", **json_data}
            data_list.append(json_data)
        else:
            print(f"Failed to retrieve data for {stdate} - {eddate}. Status code: {response.status_code}")
    
    return data_list, object_key

# S3에 데이터 업로드
def upload_all_data_to_s3(ti):
    # XCom을 통해 데이터와 OBJECT_KEY 가져오기
    data_list_area, object_key_area = ti.xcom_pull(task_ids="fetch_kopis_area_data")
    data_list_cate, object_key_cate = ti.xcom_pull(task_ids="fetch_kopis_cate_data")
    data_list_total, object_key_total = ti.xcom_pull(task_ids="fetch_kopis_total_data")
    
    # 세 가지 데이터 업로드
    upload_to_s3(data_list_area, object_key_area)
    upload_to_s3(data_list_cate, object_key_cate)
    upload_to_s3(data_list_total, object_key_total)

# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1}

dag = DAG(
    "kopis_basic_data",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False)

# 데이터 수집 태스크 생성 (prfstsArea API)
fetch_area_task = PythonOperator(
    task_id="fetch_kopis_area_data",
    python_callable=fetch_kopis_area_data,
    dag=dag,
)

# 데이터 수집 태스크 생성 (prfstsCate API)
fetch_cate_task = PythonOperator(
    task_id="fetch_kopis_cate_data",
    python_callable=fetch_kopis_cate_data,
    dag=dag,
)

# 데이터 수집 태스크 생성 (prfstsTotal API)
fetch_total_task = PythonOperator(
    task_id="fetch_kopis_total_data",
    python_callable=fetch_kopis_total_data,
    dag=dag,
)

# 데이터 업로드 태스크 생성
upload_task = PythonOperator(
    task_id="upload_kopis_data_to_s3",
    python_callable=upload_all_data_to_s3,
    dag=dag,
)

# 데이터 수집 후 업로드 태스크 실행
fetch_area_task >> fetch_cate_task >> fetch_total_task >> upload_task