
import uuid
import sys
import os


from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

from src import config
from src.postprocess import postprocess_data
from src.uploader import upload_to_mysql
from src.crawler.crawler import crawl_and_save_csv
from src.extractor import process_csv_with_openai

def generate_batch_id():
    # 날짜 + UUID 조합 예: 20250802-98fhd93hdf8
    return f"{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"


def main():
    # 1. 고유 배치 ID 생성한 후 환경 변수 불러오기
    batch_id = generate_batch_id()
    print(f"🔍 Batch ID: {batch_id}")
    load_dotenv()
    print(" MAIN PROCESSING...")
    
    
    print("🔍 Crawling and Saving CSV...")
    #2. 크롤링 및 CSV 저장 (크롤링 결과 파일 경로 반환)
    raw_csv_path = crawl_and_save_csv(batch_id)
    
    #3. OpenAI API로 정보 추출 후 새로운 CSV 저장
    print("Extracting Information with OpenAI...")
    processed_df = process_csv_with_openai(raw_csv_path, batch_id)
    
    
    # #1. 데이터 불러오기
    # DATA_PATH = "data/gpt40_data.csv"
    # if not os.path.exists(DATA_PATH):
    #     raise FileNotFoundError(f"{DATA_PATH} 파일이 존재하지 않습니다.")

    # print("📥 데이터 로드 중...")
    # processed_df = pd.read_csv(DATA_PATH)

    # 4. 전처리
    print("🧹 데이터 전처리 중...")
    processed_df = postprocess_data(processed_df, batch_id)

    # 5. 업로드
    print("🛫 MySQL 업로드 중...")
    # upload_to_mysql(processed_df)

    print("✅ 작업 완료")


if __name__ == "__main__":
    main()
