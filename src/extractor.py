import json
import re
import pandas as pd
from openai import OpenAI
from collections import defaultdict
from tqdm.auto import tqdm
from src.config import OPENAI_API_KEY

# 프롬프트 설계
prompt = """
아래는 정부 지원사업 공고문 전문입니다. 다음 항목별로 JSON 형식으로 정확히 추출하세요.
총 19개 항목 : 사업명, 지역, 지원분야, 사업 종류, 주최기관, 신청 시작일, 신청 마감일, 사업 시작일, 사업 종료일, 지원대상, 지원대상 상세, targetyears, 지원제외 대상, 신청방법, 심사방법, 지원항목, 최소 지원금액, 최대 지원금액, 전체 공고문 내용 요약
공고문 전문:
\"\"\"{pdf_text}\"\"\"

항목별 지침:
- 사업명: PDF 내 사업명을 그대로 출력
- 지역: 광역지자체명에서 '광역시', '특별시', '도' 제거 (예: 광주광역시 → 광주)
- 지원분야: 문맥에 따라 '문화예술', '콘텐츠', '기술개발', '제조업', '농업', '요식업', '사회문제해결', '기타', '상관 없음' 중 다중 선택
- 사업 종류: '공모전및경진대회', '창업지원사업', '기타' 중 택 1 (기존 기업 지원 사업이어도 지원 사업이면 창업 지원 사업에 포함시키기 )
- 주최기관: 명시된 기관명
- 신청 시작일 / 마감일 / 사업 시작일 / 종료일: 명시된 날짜 ( YYYY.MM.DD. HH:MM 형식 반드시 맞추기 )
- 지원대상: '소상공인', '예비창업자', '여성기업', '대학생', '사회적기업', '1인창업', '재창업', '기타' 중 다중 배열
- 지원대상 상세: 문장형으로 요약
- targetyears: 몇 년차 미만에 해당하는 숫자 넣기 '1년 미만'-> 1, '7년 미만'->7 (정수형 데이터)
- 지원제외 대상: 문장형으로 요약
- 신청방법: '이메일', '온라인', '방문' 등 다중 배열
- 심사방법: '단순신청', '사업계획서', '인터뷰', '발표심사', '현장심사', '기타' 중 다중 배열
- 지원항목: '재정지원', '시제품제작', '공간지원', '교육', '네트워킹', '멘토링', '창업팀빌딩', '행정지원', '투자유치', '마케팅/판로개척', '인력/고용', '글로벌', '기타' 중 다중 배열
- 최소 지원금액 / 최대 지원금액: '000,000,000원' 형식
- 전체 공고문 내용 요약: 핵심만 2~3줄로 개조식으로 요약

※ 공고에 없는 내용은 공백으로 표시 (Null)
※ 문장형과 요약 항목 제외하고는 나머지 항목에서 선택지에 없는 사항은 절대 넣지 말 것
※ 다중 선택인 경우 List 형태가 아닌 콤마(,)로 구분된 문자열로 출력할 것
※ 기타 항목을 최소화할 것. 최대한 기타를 제외한 다른 항목에 포함시킬 것
반드시 19개 항목만을 포함하여 json형식으로 추출하세요.

"""

def process_csv_with_openai(raw_csv_path, batch_id):


    api_key = OPENAI_API_KEY
    client = OpenAI(api_key=api_key)

    df_filtered = pd.read_csv(raw_csv_path)

    data = defaultdict(list)
    for i, row in tqdm(df_filtered.iterrows(), total=df_filtered.shape[0]):
        # PDF 추출 텍스트 관련 파이프라인 작성할 곳
        # with open("extracted_text.txt", "r", encoding="utf-8") as f:
        #     pdf_text = f.read()

        pdf_text = row['PDF 내용']

        # API 호출
        response = client.chat.completions.create(
            model="gpt-4o-mini", #"gpt-4", "gpt-4o-mini" # 또는 gpt-4o
            messages=[
                {"role": "user", "content":  prompt.format(pdf_text=pdf_text)}
            ],
            temperature=0.2,
        )

        # ✅ 1. 응답 content를 문자열로 추출
        content_str = response.choices[0].message.content

        # ✅ 2. 코드 블록(````json ... ````) 제거
        # 이 정규식은 ```json\n{...}\n``` 형식을 제거하고 JSON 본문만 남깁니다.
        json_str = re.sub(r"^```json\n(.*?)\n```$", r"\1", content_str.strip(), flags=re.DOTALL)

        # ✅ 3. JSON 파싱
        structured_data = json.loads(json_str)

        data['PDF 파일명'].append(row['PDF 파일명'])
        data['상태'].append(row['상태'])
        data['링크'].append(row['링크'])

        for key, value in structured_data.items():
            data[key].append(value)
    # ✅ 4. DataFrame 변환
    df = pd.DataFrame(data)

    # ✅ 5. CSV 저장
    df.to_csv("data/preprocess/{batch_id}.csv", index=False, encoding="utf-8-sig")
    
    return df
    
