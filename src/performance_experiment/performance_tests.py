import pandas as pd
import polars as pl

def pl_mean_test_speed(df_pl):
    """
    Getting Mean per PULocationID
    """
    df_pl = df_pl[['PULocationID', 'trip_distance']].groupby('PULocationID').mean()
    return df_pl

def mean_test_speed_pd(df_pd):
    """
    Getting Mean per PULocationID
    """
    df_pd = df_pd[['PULocationID']].mean()
    return df_pd

def pd_read_parquet(path,dtype_backend, engine):
    """
    Converting parquet file into Pandas dataframe
    """
    df= pd.read_parquet(path,dtype_backend=dtype_backend, engine=engine)
    return df


def endwith_test_speed_pd(df_pd):
    """
    Only getting Zones that end with East
    """

    df_pd = df_pd[df_pd.store_and_fwd_flag.str.endswith('East', na=False)]

    return df_pd

def endwith_test_speed_pl(df_pl):
    """
    Only getting Zones that end with East
    """

    df_pl = df_pl.filter(pl.col("store_and_fwd_flag").str.ends_with('East'))

    return df_pl