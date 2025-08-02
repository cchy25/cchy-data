

from sqlalchemy import create_engine
import pymysql

from src.config import DB_URL, DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


def upload_to_mysql(data):
        

    # 7. SQLAlchemy 엔진 생성
    engine = create_engine(DB_URL)

    # 8. 테이블에 데이터 추가 (id는 자동 생성되므로 제외, index도 제외)
    data.to_sql(name='source', con=engine, if_exists='append', index=False) # if_exists=> fail, replace, append

    print("MySQL 업로드 완료")
    