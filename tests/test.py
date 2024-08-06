from de32_megabox_l.movie_l import make_million_chart, print_df, save2parquet
import pandas as pd
import os


def test_chart():
    df = make_million_chart()
    assert isinstance(df, pd.DataFrame)


def test_print_df():
    print_df(df = make_million_chart())
    assert True


def test_s2p():
    save2parquet(df = make_million_chart())
    file_path = "~/megabox/result_data/millions/"
    assert os.path_exists(file_path)
        
