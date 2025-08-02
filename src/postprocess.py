
import re
import pandas as pd

def postprocess_data(data, batch_id):
        
    # 2. 컬럼명 매핑
    dict_mapping = {
    'PDF 파일명' : 'file_name', # # varchar(64)
    '상태' : 'status', # varchar(50)
    '링크' : 'url', # varchar(255)
    '사업명' : 'title', # varchar(255) 
    '지역' : 'regions', # varchar(255)
    '지원분야' : 'support_fields', # varchar(255)
    '사업 종류' : 'support_types', # varchar(50)
    '주최기관' : 'organization', # varchar(255)
    '신청 시작일' : 'apply_end_at', # datetime(6)
    '신청 마감일' : 'apply_start_at', # datetime(6)
    '사업 시작일' : 'start_at', # datetime(6)
    '사업 종료일' : 'end_at', # datetime(6)
    '지원대상' : 'targets', # varchar(255)
    '지원대상 상세' : 'target_detail', # varchar(255)
    'targetyears' : 'years', # int
    '지원제외 대상' : 'ex_target_detail', # varchar(255)
    '신청방법' : 'apply_methods', # varchar(255)
    '심사방법' : 'evaluation_methods', # varchar(255)
    '지원항목' : 'support_category', # varchar(255)
    '최소 지원금액' : 'min_amount', # bigint
    '최대 지원금액' : 'max_amount', # bigint
    '전체 공고문 내용 요약' : 'summary', # varchar(255)
    }



    #### Pre 사전 정의해야할 목록

    # 사전 정의: 들어갈 수 있는 값의 리스트 정의
    valid_support_fields = ['상관 없음','문화예술','콘텐츠','기술개발','제조업','농업','요식업','사회문제해결','기타']       # '지원분야'
    valid_support_types = ['공모전및경진대회', '창업지원사업', '기타']        # '사업 종류'
    valid_targets = ['재창업','여성기업','대학생','사회적기업','1인창업','소상공인','예비창업자', '기타']              # '지원대상'
    valid_apply_methods = ['이메일', '온라인', '방문', '기타']        # '신청방법'
    valid_evaluation_methods = ['단순신청', '사업계획서', '인터뷰', '발표심사', '현장심사', '기타']   # '심사방법'
    valid_support_category = ['재정지원','시제품제작','공간지원','교육','네트워킹','멘토링','창업팀빌딩','행정지원','투자유치','마케팅/판로개척','인력/고용','글로벌','기타']     # '지원항목'


    def correct_spacing(value, valid_list):
        """
        value가 valid_list 안에 없다면, 붙어있는 문자열을 공백 단위로 나누고
        valid_list와 일치하는 항목이 있는지 확인해 교체합니다.
        """
        if pd.isna(value):
            return value

        if value in valid_list:
            return value

        for valid_value in valid_list:
            # 정규식으로 공백 제거한 것과 비교
            if re.sub(r"\s+", "", valid_value) == re.sub(r"\s+", "", value):
                return valid_value

        return value  # 못 찾으면 원래 값 반환


    column_valid_map = {
        '지원분야': valid_support_fields,
        '사업 종류': valid_support_types,
        '지원대상': valid_targets,
        '신청방법': valid_apply_methods,
        '심사방법': valid_evaluation_methods,
        '지원항목': valid_support_category,
    }

    for col, valid_list in column_valid_map.items():
        data[col] = data[col].apply(lambda x: correct_spacing(x, valid_list))


    # 띄어쓰기 그냥 다 없애
    data['지원분야'] = data['지원분야'].str.replace('상관 없음', '문화예술,콘텐츠,기술개발,제조업,농업,요식업,사회문제해결,기타')
    data['지원분야'] = data['지원분야'].str.replace(r'\s+', '', regex=True)
    data['사업 종류'] = data['사업 종류'].str.replace(r'\s+', '', regex=True)
    data['신청방법'] = data['신청방법'].str.replace(r'\s+', '', regex=True)
    data['지원항목'] = data['지원항목'].str.replace(r'\s+', '', regex=True)
    data['지원대상'] = data['지원대상'].str.replace(r'\s+', '', regex=True)
    data['심사방법'] = data['심사방법'].str.replace(r'\s+', '', regex=True)
    
    # 컬럼명 바꾸기
    data = data.rename(columns=dict_mapping)


    ###### 잠시 있는 코드
    # ######## 예비창업자 없애는 코드
    # import pandas as pd
    # import re

    # # 예시용 DataFrame
    # # df = pd.DataFrame({"targets": [...]})

    # # 1단계: 예비창업자만 있는 경우 → 기타로 변경
    # data["targets"] = data["targets"].replace(r"^\s*예비창업자\s*$", "기타", regex=True)

    # # 2단계: 나열된 값 중에서 '예비창업자'만 제거
    # data["targets"] = data["targets"].str.replace(r"(?:^|,)\s*예비창업자\s*(?=,|$)", "", regex=True)

    # # 3단계: 남은 쉼표 정리 및 앞뒤 공백 제거
    # data["targets"] = data["targets"].str.replace(r",\s*,", ",", regex=True)  # 연속 쉼표 정리
    # data["targets"] = data["targets"].str.strip(" ,")  # 앞뒤 쉼표 및 공백 제거

    # # 4단계: 빈 문자열은 기타로 처리
    # data["targets"] = data["targets"].replace("", "기타")


    # 3. 누락된 컬럼 추가: condition_detail은 NULL로 추가
    data['condition_detail'] = None

    # 4. datetime 컬럼 변환
    datetime_cols = ['apply_end_at', 'apply_start_at', 'start_at', 'end_at']
    for col in datetime_cols:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], errors='coerce')

    # ✅ 5-1. 지역값 정제
    valid_regions = ["서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
                    "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]

    def clean_regions(region_value):
        if pd.isna(region_value):
            return []

        # 쉼표로 여러 지역이 들어오는 경우 분리
        regions = [r.strip() for r in str(region_value).split(',')]

        if "전국" in regions:
            return valid_regions

        # 유효성 검사
        for r in regions:
            if r not in valid_regions:
                raise ValueError(f"❌ 잘못된 지역 값 발견: {r}")

        return regions

    # 문자열 리스트로 변환 후 쉼표로 다시 join (DB 저장을 위해 문자열로)
    data['regions'] = data['regions'].apply(clean_regions).apply(lambda x: ",".join(x))

    # ✅ 5-2. 전체 컬럼: 리스트 문자열 정제 → 그냥 문자열만 남기기
    def strip_list_string(val):
        if isinstance(val, str):
            # 예: ['소상공인'] → 소상공인
            return re.sub(r"^\[?['\"]?([^'\"]+)['\"]?\]?$", r"\1", val.strip())
        return val

    data = data.applymap(strip_list_string)


    # ✅ 5-3. 금액 컬럼 정제: "7,000,000원" → 7000000 (int)
    def parse_amount(val):
        if pd.isna(val):
            return None
        try:
            return int(re.sub(r"[^\d]", "", str(val)))
        except:
            return None

    data['min_amount'] = data['min_amount'].apply(parse_amount)
    data['max_amount'] = data['max_amount'].apply(parse_amount)

    # ✅ 5-4. years 컬럼 처리: 정수가 아니라면 반올림하여 정수로 변환
    def clean_years(val):
        try:
            if pd.isna(val):
                return None
            return int(round(float(val)))
        except:
            print(f"⚠️ 'years' 컬럼의 값 오류: {val}")
            return None

    data['years'] = data['years'].apply(clean_years)

    # ✅ 5-5. 제약사항 잘 지켰는지 확인 

    # ✅ 공통 전처리 함수 정의
    def validate_allowed_values_with_fallback(column_name, value, valid_list):
        if pd.isna(value):
            return ""

        items = [item.strip() for item in str(value).split(',') if item.strip()]
        valid_set = set(valid_list)

        # '기타'가 valid_list에 있는지 체크
        has_etc = '기타' in valid_set

        new_items = []
        invalid_items = []

        for item in items:
            if item in valid_set:
                new_items.append(item)
            else:
                invalid_items.append(item)

        if invalid_items:
            print(f"⚠️ [{column_name}] 컬럼에 허용되지 않은 값 발견되어 '기타'로 대체: {invalid_items}")

        # 없는 값은 제거하고, '기타'가 없으면 넣고, 이미 있으면 중복 제거
        if invalid_items:
            if has_etc:
                if '기타' not in new_items:
                    new_items.append('기타')
            else:
                # valid_list에 '기타'가 없으면 그냥 없앤다 (선택사항)
                pass

        # 중복 제거(순서 유지)
        seen = set()
        final_items = []
        for item in new_items:
            if item not in seen:
                final_items.append(item)
                seen.add(item)

        return ",".join(final_items)


    # ✅ 각 컬럼에 적용 (허용값은 위에서 직접 정의해줘야 함)
    data['support_fields'] = data['support_fields'].apply(lambda x: validate_allowed_values_with_fallback('지원분야', x, valid_support_fields))
    data['support_types'] = data['support_types'].apply(lambda x: validate_allowed_values_with_fallback('사업 종류', x, valid_support_types))
    data['targets'] = data['targets'].apply(lambda x: validate_allowed_values_with_fallback('지원대상', x, valid_targets))
    data['apply_methods'] = data['apply_methods'].apply(lambda x: validate_allowed_values_with_fallback('신청방법', x, valid_apply_methods))
    data['evaluation_methods'] = data['evaluation_methods'].apply(lambda x: validate_allowed_values_with_fallback('심사방법', x, valid_evaluation_methods))
    data['support_category'] = data['support_category'].apply(lambda x: validate_allowed_values_with_fallback('지원항목', x, valid_support_category))


    # ✅ "공고 내 미기재" → NaN으로 변환
    data = data.replace("공고 내 미기재", pd.NA)
    
    
    # 'file_name'과 'status' 컬럼 제거
    data = data.drop(['file_name', 'status'], axis=1)
    
    return data