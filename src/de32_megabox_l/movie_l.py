import pandas as pd
import os
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
def read2par(path="~/megabox/tmp/transform_parquet"):
    df = pd.read_parquet(f"{path}")
    return df
import pandas as pd
import os
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
def read2par(path="~/megabox/tmp/transform_parquet"):
    df = pd.read_parquet(f"{path}")
    return df

def make_million_chart():
    #TODO
    #import 파일을 호출 (7개)
    #새로운 df를 만들어서 누작관객수 100만관객을 넘기는데 필요한 기간을 받아서 저장
    #새로운 df에는 영화제목, 해당 일자, 연말기준 누적관객수, 연말기준 누적매출액만 저장
    #영화별 데이터 생성 이후 show_million_chart 호출

    #num_cols = ['rnum', 'movieNm', 'openDt', 'salesAmt',  'audiCnt']
    #순번, 영화명(국문), 영화개봉일, 당일매출액, 당일관객수

    df = read2par()
    
    df_million = df[df['audiAcc'] >= 1000000].copy()
    df['load_dt'] = df['load_dt'].astype(int).astype(str)
    df['load_dt'] = pd.to_datetime(df['load_dt'], format='%Y%m%d')
    df['openDt'] = pd.to_datetime(df['openDt'], errors='coerce')

    new_df = df_million.sort_values('load_dt').drop_duplicates('movieNm', keep='first')
    new_df['openDt'] = pd.to_datetime(new_df['openDt'], format='%Y-%m-%d')
    new_df['load_dt'] = pd.to_datetime(new_df['load_dt'], format='%Y%m%d') 
    new_df['days'] = (new_df['load_dt'] - new_df['openDt']).dt.days
    result = new_df[['movieNm', 'openDt', 'load_dt', 'days']].sort_values(by='days').reset_index(drop=True)
    

    return result

def print_df(df):
    df = make_million_chart()
    print(df)

def save2parquet(df: pd.DataFrame) -> None:
    path = os.path.expanduser("~/megabox/result_data/millions")
    Path(path).mkdir(parents=True, exist_ok=True)
    save_path = Path(path)/"million_viewer_movies.parquet"
    df.to_parquet(save_path, index=False)
    

df = make_million_chart()
print_df(df)
save2parquet(df)


