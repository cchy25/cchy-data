import pandas as pd
import glob
import os

from src.crawler.crawl_gicon import crawl_gicon
from src.crawler.crawl_gisec import crawl_gisec

def combine_csv_files(csv_path_list):
    """
    주어진 CSV 파일 경로 목록을 읽어와 하나로 병합.

    Args:
    csv_path_list: 병합할 CSV 파일 경로 목록 (리스트).
    output_path: 결과 파일 저장 경로 (문자열).
    """
    combined_df = pd.DataFrame()
    for csv_path in csv_path_list:
        try:
            df = pd.read_csv(csv_path)
            combined_df = pd.concat([combined_df, df], axis=0, ignore_index=True)
        except FileNotFoundError:
            print(f"Error: File not found at {csv_path}")
        except pd.errors.EmptyDataError:
            print(f"Warning: {csv_path} is empty. Skipping.")
        except Exception as e:
            print(f"Error processing {csv_path}: {e}")

    return combined_df

    


def crawl_and_save_csv(batch_id):
    
        # 2. 크롤링 및 CSV 저장 (크롤링 결과 파일 경로 반환)
#raw_csv_path = crawl_and_save_csv(batch_id)

    crawl_gicon(batch_id)
    crawl_gisec(batch_id)
    # crawl_gjtp(batch_id)
    
    pattern = os.path.join("data/collected_data", f"*{batch_id}.csv")
    csv_list = glob.glob(pattern)

    output_file = f"data/combined_collected_data/{batch_id}" # 저장할 파일 경로
    df_combined = combine_csv_files(csv_list)
        
    df_filtered = df_combined[
        df_combined['PDF 내용'].notna() &
        df_combined['PDF 내용'].str.len().gt(100) &
        df_combined['PDF 내용'].str.len().lt(5000)
    ]
    df_filtered = df_filtered.reset_index(drop=True)

    if not df_filtered.empty:
        df_filtered.to_csv(output_file, index=False)
        print(f"Successfully combined files and saved to {output_file}")
        return output_file
    else:
        raise Exception("No data was combined. Check input files.")
        

    
    
    