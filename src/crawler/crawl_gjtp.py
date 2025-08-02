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


import time
import random
import urllib.parse
from pathlib import Path
import os

from urllib.parse import urlparse


def crawl_gjtp(batch_id):
    
    ############# 광주 테크노파크 #####################33
    # https://www.gjtp.or.kr/home/business.cs?m=8&pageIndex=1

    # 설정
    BASE_URL = "https://www.gjtp.or.kr/home/business.cs"
    START_PAGE = 1
    END_PAGE = 2
    SAVE_DIR = "data/raw/gjtp"
    CSV_PATH = f"data/collected_data/gjtp_{batch_id}.csv"
    WAIT_TIME = 5

    os.makedirs(SAVE_DIR, exist_ok=True)
    data = []

    # 셀레니움 옵션
    options = Options()
    prefs = {
        "download.default_directory": SAVE_DIR,
        "download.prompt_for_download": False, # PDF 뷰어 대신 다운로드
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", prefs)

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)



    def wait_for_pdf_download(filename, timeout=15):
        start_time = time.time()
        while time.time() - start_time < timeout:
            # .pdf 파일이 완전히 다운로드되었는지 확인 (.crdownload 없을 것)
            matching_files = glob.glob(os.path.join(SAVE_DIR, filename))
            if matching_files and not any(f.endswith(".crdownload") for f in matching_files):
                return matching_files[0]
            time.sleep(1)
        # 다운로드된 파일 경로 지정
        pdf_path = os.path.join(SAVE_DIR, filename)
        return pdf_path


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
        url = f"{BASE_URL}?m=8&pageIndex={page}"
        driver.get(url)
        # ✅ table 요소가 로드될 때까지 대기 (최대 10초)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.list-table tbody tr"))
        )

        soup = BeautifulSoup(driver.page_source, "lxml")
        rows = soup.select("table.list-table tbody tr")

        for row in rows:
            try:
                title_tag = row.select_one("td.tal a")
                if not title_tag:
                    continue

                title = title_tag.text.strip()
                relative_link = title_tag.get("href")

                # 만약 href가 '/' 없이 '?act=view'부터 시작되면, '/'를 붙여줘야 urljoin이 제대로 작동함
                if relative_link.startswith("?"):
                    relative_link = "/" + relative_link

                parsed_base = urlparse(BASE_URL)
                base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"

                full_link = urljoin(BASE_URL, relative_link)
                status = row.select("td")[-1].text.strip()
                dates = list(row.select("td")[1].stripped_strings)
                date = f"{dates[0]} ~ {dates[1]}" if len(dates) == 2 else dates[0] if dates else ""

                # 접수마감 및 결과 제외
                if status not in ['접수중', '접수전']:
                    continue

                # gcon 특화 코드
                if title in title_set:
                    continue
                else:
                    title_set.add(title)
                    # title 어떤 것 진행 중인지 log 남기기
                    print("진행 중인 공고문 제목 : ", title)

                # 상세 페이지 진입
                driver.get(full_link)
                WebDriverWait(driver, WAIT_TIME*2)#.until(
                #     EC.presence_of_element_located((By.CSS_SELECTOR, "td"))
                # )
                detail_soup = BeautifulSoup(driver.page_source, "lxml")

                file_items = []

                # 상세 페이지 전체에서 모든 <a> 태그를 가져와서 파일 확장자로 끝나는 텍스트만 필터링
                for a in detail_soup.find_all("a", href=True):
                    text = a.get_text(strip=True).lower()
                    break
                    if any(text.endswith(ext) for ext in [".pdf", ".hwp", ".doc", ".docx", ".xls", ".xlsx"]):
                        print(f"text : {text}")
                        file_items.append(a)
                # pdf 없을 경우, "" 공백 입력 ( nan 아닌 )
                pdf_text = ""
                pdf_filename = ""

                for item in file_items:
                    try:
                        # ✅ 요청 사이에 지연 추가 (2~5초 랜덤)
                        time.sleep(random.uniform(2, 5))

                        file_name = item.get_text(strip=True)
                        href = item["href"]

                        # ✅ 조건: '공고' 포함 + '.pdf'로 끝나는 텍스트
                        if "공고" not in file_name.lower():
                            continue
                        if not file_name.lower().endswith(".pdf"):
                            continue

                        # ✅ 상대 경로인 경우 절대경로로 변환
                        if not href.startswith("http"):
                            href = urllib.parse.urljoin(driver.current_url, href)

                        # ✅ 해당 링크 텍스트로 element 다시 찾기 (클릭)
                        pdf_element = driver.find_element(By.LINK_TEXT, file_name)
                        pdf_element.click()

                        # ✅ 다운로드 완료 대기 (3초)
                        time.sleep(3)

                        # ✅ 다운로드 디렉토리에서 최신 파일 탐색
                        downloaded_files = list(Path(SAVE_DIR).glob("*"))
                        latest_file = max(downloaded_files, key=lambda f: f.stat().st_mtime)

                        # ✅ 원하는 이름으로 저장
                        pdf_filename = f"{title}.pdf"
                        pdf_path = Path(SAVE_DIR) / pdf_filename
                        latest_file.rename(pdf_path)

                        # ✅ PDF 열어서 텍스트 추출
                        if os.path.exists(pdf_path):
                            with open(pdf_path, "rb") as f:
                                pdf_text = extract_pdf_text(f.read())
                        else:
                            pdf_text = f"❌ 다운로드된 파일이 존재하지 않음: {pdf_filename}"

                        break  # 첫 번째 공고문 PDF만 처리하고 종료

                    except Exception as e:
                        print(f"❌ PDF 다운로드 실패 - [{title}]: {e}")
                        pdf_text = f"PDF 다운로드 실패: {e}"
                        break

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