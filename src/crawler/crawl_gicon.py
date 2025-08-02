#### driver에 어떤 작업을 한 후에는 충분한 시간이 필요하다.
# Selenium을 확인하려면 다음과 같이 사용해야 합니다.
import selenium
#print(selenium.__version__) # 이전에 사용했던 버전 : 4.15.2

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from pathlib import Path
from os.path import join as opj
import requests
from bs4 import BeautifulSoup
import PyPDF2

from urllib.parse import urljoin
from io import BytesIO
import os
import time
import glob
import random
from tqdm.auto import tqdm
from collections import defaultdict
import pandas as pd


pd.set_option('display.max_columns', 25)
def crawl_gicon(batch_id):
    
    ############# 광주 정보 문화 산업 진흥원 #####################33
    # https://www.gicon.or.kr/board.es?mid=a10204000000&bid=0003

    # 설정
    BASE_URL = "https://www.gicon.or.kr"
    START_PAGE = 1
    END_PAGE = 10
    SAVE_DIR = "data/raw/gicon"
    CSV_PATH = f"data/collected_data/gicon_{batch_id}.csv"
    WAIT_TIME = 5

    # os.makedirs(SAVE_DIR, exist_ok=True)
    data = []

    # 셀레니움 옵션
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    def extract_pdf_text(pdf_bytes):
        try:
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            text = ""
            for page in reader.pages:
                text += page.extract_text() or ""
            return text.strip()
        except Exception as e:
            return f"PDF 읽기 실패: {e}"

    # gicon특화 코드 title_set
    # 각 페이지마다 중복적인 공지가 있어 중복 미포함하도록 사전 처리 진행
    title_set = set()

    for page in tqdm(range(START_PAGE, END_PAGE + 1), desc="페이지 탐색 중"):
        url = f"{BASE_URL}/board.es?mid=a10204000000&bid=0003&act=list&nPage={page}"
        driver.get(url)
        time.sleep(WAIT_TIME)

        soup = BeautifulSoup(driver.page_source, "lxml")
        rows = soup.select("table.tstyle_list tbody tr")

        for row in rows:
            try:
                title_tag = row.select_one("td.txt_left a")
                if not title_tag:
                    continue

                title = title_tag.text.strip()
                relative_link = title_tag.get("href")
                full_link = BASE_URL + relative_link
                date = row.select("td")[2].text.strip()
                status = row.select("td")[3].text.strip()

                # gcon 특화 코드
                if title in title_set:
                    continue
                else:
                    title_set.add(title)
                    # title 어떤 것 진행 중인지 log 남기기
                    print("진행 중인 공고문 제목 : ", title)

                # 접수마감 및 결과 제외
                if status not in ['접수중', '접수예정']:
                    continue
                # 상세 페이지 진입
                driver.get(full_link)
                WebDriverWait(driver, WAIT_TIME*2)#.until(
                #     EC.presence_of_element_located((By.CSS_SELECTOR, "td"))
                # )
                detail_soup = BeautifulSoup(driver.page_source, "lxml")
                file_items = detail_soup.select("div.file li")


                # pdf 없을 경우, "" 공백 입력
                pdf_text = ""
                pdf_filename = ""
                for item in file_items:
                    # 텍스트에 'pdf' 포함 여부 판단
                    if "pdf" in item.text.lower():
                        pdf_tag = item.select_one("a.btn_line")
                        if pdf_tag and pdf_tag.get("href"):
                            pdf_url = BASE_URL + pdf_tag["href"]
                            pdf_filename = pdf_tag.get("href").split("list_no=")[-1] + ".pdf"

                            try:
                                res = requests.get(pdf_url)
                                pdf_text = extract_pdf_text(res.content)

                                with open(os.path.join(SAVE_DIR, pdf_filename), "wb") as f:
                                    f.write(res.content)

                            except Exception as e:
                                print(f"❌ PDF 다운로드 실패 - [{title}]: {e}")
                                pdf_text = f"PDF 다운로드 실패: {e}"
                        break  # 첫 번째 PDF만 추출하고 루프 종료

                # 데이터 저장
                data.append({
                    "제목": title,
                    "링크": full_link,
                    "접수기간": date,
                    "상태": status,
                    "PDF 파일명": pdf_filename,
                    "PDF 내용": pdf_text
                })

            except Exception as e:
                print(f"❌ [{title}] 처리 중 오류 발생: {e}")
                continue

    # 저장
    df = pd.DataFrame(data)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    driver.quit()
    print(f"✅ 완료! 결과 CSV 저장: {CSV_PATH}")