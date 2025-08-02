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


def crawl_gisec(batch_id):

    ############# 광주 사회적 경제 지원센터 #####################33
    # https://www.gjsec.kr/bbs/board.php?bo_table=csupport&sst=wr_datetime&sod=desc&type=&page=1

    # 설정
    BASE_URL = "https://www.gjsec.kr"
    START_PAGE = 1
    END_PAGE = 15  # 현재 상황에서 15p 정도까지 하면 모집 중인 건 다 뽑아올 수 있을 것으로 보임
    SAVE_DIR = "data/raw/gjsec"
    CSV_PATH = f"data/collected_data/gjsec_{batch_id}.csv"
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
        url = f"https://www.gjsec.kr/bbs/board.php?bo_table=csupport&sst=wr_datetime&sod=desc&type=&page={page}" #변경
        driver.get(url)
        time.sleep(WAIT_TIME)

        soup = BeautifulSoup(driver.page_source, "lxml")
        rows = soup.select("tbody > tr") # 변경

        for row in rows:
            try:
                title_tag = row.select_one("td.td_subject a")
                if not title_tag:
                    continue

                title = title_tag.text.strip()
                relative_link = title_tag.get("href")
                full_link = relative_link if relative_link.startswith("http") else BASE_URL + relative_link
                date = row.select_one("td.td_datetime").text.strip()
                status = row.select_one("td:nth-of-type(2) span").text.strip()

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
                file_items = detail_soup.select("section#bo_v_file li")


                # pdf 없을 경우, "" 공백 입력 ( nan 아닌 )
                pdf_text = ""
                pdf_filename = ""
                for item in file_items:
                    # ✅ 요청 사이에 지연 추가 (2~5초 랜덤 추천)
                    time.sleep(random.uniform(2, 5))  # 2초~5초 랜덤으로 쉬기
                    # 텍스트에 'pdf' 포함 여부 판단
                    if "pdf" in item.text.lower():
                        # if not pdf_url.startswith("http"):
                        #     pdf_url = BASE_URL + pdf_tag["href"]
                        # else:
                        #     pdf_url = pdf_tag["href"]
                        try:
                            # PDF 다운로드 버튼 클릭
                            pdf_tag = driver.find_element(By.CSS_SELECTOR, "a.view_file_download")

                            pdf_url = pdf_tag.get_attribute("href")  # 실제 다운로드 URL 추출
                            # pdf_filename = os.path.basename(pdf_url).split("?")[0]

                            # 클릭으로 다운로드 시작
                            pdf_tag.click()

                            # ✅ 다운로드 완료를 기다리기 (예: 3초)
                            time.sleep(3)

                            # 글자 깨짐 문제 (인코딩 문제) 해결
                            # 다운로드 폴더에서 가장 최근 파일 찾기
                            downloaded_files = list(Path(SAVE_DIR).glob("*"))
                            latest_file = max(downloaded_files, key=lambda f: f.stat().st_mtime)

                            # 원하는 이름으로 변경
                            pdf_filename = f"{title}.pdf"
                            pdf_path = Path(SAVE_DIR) / pdf_filename
                            latest_file.rename(pdf_path)

                            # 다운로드 완료 대기
                            # pdf_path = wait_for_pdf_download(pdf_filename)

                            # ✅ 다운로드된 파일 열어서 텍스트 추출
                            if os.path.exists(pdf_path):
                                with open(pdf_path, "rb") as f:
                                    pdf_text = extract_pdf_text(f.read())
                            else:
                                pdf_text = f"❌ 다운로드된 파일이 존재하지 않음: {pdf_filename}"

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