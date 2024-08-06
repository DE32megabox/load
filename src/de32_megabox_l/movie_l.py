import pandas as pd
import os
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
def read2par():
    base_dir="~/megabox/tmp/transform_parquet"
    parquet_pattern = Path(base_dir) / "load_dt=*"
    parquet_files = list(parquet_pattern.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {base_dir}")

    dataset = pq.ParquetDataset(parquet_files)
    table = dataset.read()
    df = table.to_pandas()
    return df

def make_million_chart():
    #TODO
    #import 파일을 호출 (7개)
    #새로운 df를 만들어서 누작관객수 100만관객을 넘기는데 필요한 기간을 받아서 저장
    #새로운 df에는 영화제목, 해당 일자, 연말기준 누적관객수, 연말기준 누적매출액만 저장
    #영화별 데이터 생성 이후 show_million_chart 호출
    
    m_df = {}

    for parquet_path in parquet_paths:
        df = read2par()
        for _, row in df.iterrows():
            movie_cd = row["movieCd"]
            daily_audience = int(row["audiCnt"])  # 문자열을 정수로 변환
            daily_revenue = int(row["salesAmt"])  # 문자열을 정수로 변환
            cumulative_audience = int(row["audiAcc"])  # 문자열을 정수로 변환
            cumulative_revenue = int(row["salesAcc"])  # 문자열을 정수로 변환
            show_date = row["openDt"]

            if movie_cd not in m_df:
                m_df[movie_cd] = {
                    "영화 제목": row["movieNm"],
                    "100만 관객수 돌파 일자": None,
                    "누적 관객수" : 0,
                    "누적 매출액" : 0,
                    "첫 상영일자" : show_date,
                }

            m_df[movie_cd]["누적 관객수"] += daily_audience
            m_df[movie_cd]["누적 매출액"] += daily_revenue

            if m_df[movie_cd]["누적 관객수"] >= 1000000 and m_df[movie_cd]["100만 관객수 돌파 일자"] is None:
                m_df[movie_cd]["100만 관객수 돌파 일자"] = show_date
    # 12월 31일까지 상영된 영화 중 100만 관객을 돌파한 영화만 필터링
    m_df = {
        movie_cd: data
        for movie_cd, data in m_df.items()
        if data["100만 관객수 돌파 일자"] is not None
    }

    # 결과 DataFrame 생성
    result_df = pd.DataFrame(m_df).T
    result_df = result_df.rename_axis("movieCd").reset_index()

    # 100만 돌파 소요 시간 계산 (단위: 일)
    print(result_df)
    result_df["100만 돌파 소요 시간"] = (
        pd.to_datetime(result_df["100만 관객수 돌파 일자"], format="%Y%m%d")
        - pd.to_datetime(result_df["첫 상영일자"], format="%Y%m%d")
    ).dt.days

    # 12월 31일 기준 정보 열 이름 변경
    result_df = result_df.rename(
        columns={
            "누적 관객수": "12월 31일 기준 누적 관객수",
            "누적 매출액": "12월 31일 기준 누적 매출액",
        }
    )

    return result_df


def print_df(df):
    print(
            df[
                [
                    "영화제목",
                    "100만 돌파 소요 시간",
                    "12월 31일 기준 누적 관객수",
                    "12월 31일 기준 누적 매출액",
                ]
            ]
        )


def save2parquet(df: pd.DataFrame) -> None:
    path = "~/megabox/result_data/millions"
    Path(path).mkdir(parents=True, exist_ok=True)
    save_path = Parh(path)/"million_viewer_movies.parquet"
    df.to_parquet(save_path, index=False)
    

df = read2par()
print(df.head(5))

