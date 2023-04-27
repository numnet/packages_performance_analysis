import pandas as pd
import time
import pyarrow as pa


import polars as pl
import time


def pl_read_csv(path, ):
    """
    Converting csv file into Pandas dataframe
    """
    df= pl.read_csv(path,)
    return df

def pl_read_parquet(path, ):
    """
    Converting parquet file into Pandas dataframe
    """
    df= pl.read_parquet(path,)
    return df

def pd_read_parquet_default(path):
    """
    Converting parquet file into Pandas dataframe
    """
    df= pd.read_parquet(path)
    return df

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

def pd_read_csv(path,dtype_backend, engine):
    """
    Converting parquet file into Pandas dataframe
    """
    df= pd.read_csv(path,dtype_backend=dtype_backend, engine=engine)
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

def main():
    
    path_parquet="E:/datasets/Tests/yellow_tripdata_2021-01.parquet"
    path_csv="E:/datasets/Tests/yellow_tripdata_2021-01.csv"
    execution_times = {}
    
    print(f'Starting ETL for Pandas version {pd.__version__}')
    print('Extracting Parquet pyarrow...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        df_trips_pyarrow_pyarrow= pd_read_parquet(path_parquet,"pyarrow","pyarrow")
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'Extraction Parquet end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'read_parquet_pyarrow',start_time,end_time,execution_time]

    print('Extracting Parquet Polars...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        pl_df_trips= pl_read_parquet(path_parquet)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'Extraction Parquet end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'read_parquet_polars',start_time,end_time,execution_time]

    print('Extracting Parquet default...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        df_trips_default= pd_read_parquet_default(path_parquet)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'Extraction Parquet end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'pd_read_parquet_default',start_time,end_time,execution_time]
    
    print('Extracting CSV pyarrow...')
    
    for i in range(0,10):
        start_time = time.perf_counter()    
        df_trips_numpy_nullable_pyarrow= pd_read_parquet(path_parquet,"numpy_nullable","pyarrow")
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'Extraction CSV arrow_backend end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'read_csv_arrow_backend',start_time,end_time,execution_time]

    print('Extracting CSV Polars...')
    
    for i in range(0,10):
        start_time = time.perf_counter()    
        pl_df_trips= pl_read_csv(path_csv)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'Extraction CSV Polars end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'read_csv_polars',start_time,end_time,execution_time]

    print('mean_test_speed df_trips_pyarrow_pyarrow...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        mean_test= mean_test_speed_pd(df_trips_pyarrow_pyarrow)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'mean_test_speed end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'mean_test_speed_df_trips_pyarrow_pyarrow',start_time,end_time,execution_time]
        
    print('mean_test_speed df_trips_numpy_nullable_pyarrow...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        mean_test= mean_test_speed_pd(df_trips_numpy_nullable_pyarrow)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'mean_test_speed end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'mean_test_speed_df_trips_numpy_nullable_pyarrow',start_time,end_time,execution_time]

    print('mean_test_speed pl_df_trips...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        mean_test= pl_mean_test_speed(pl_df_trips)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'pl_mean_test_speed end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'pl_mean_test_speed',start_time,end_time,execution_time]

    print('mean_test_speed df_trips_default...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        mean_test= mean_test_speed_pd(df_trips_default)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'pl_mean_test_speed end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'mean_test_speed_df_trips_default',start_time,end_time,execution_time]

    print('endwith_test_speed_pd df_trips_default...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        mean_test= endwith_test_speed_pd(df_trips_default)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'pl_mean_test_speed end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'endwith_test_speed_pd_df_trips_default',start_time,end_time,execution_time]

    print('endwith_test_speed_pd df_trips_pyarrow_pyarrow...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        mean_test= endwith_test_speed_pd(df_trips_pyarrow_pyarrow)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'endwith_test_speed_pd end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'endwith_test_speed_df_trips_pyarrow_pyarrow',start_time,end_time,execution_time]

    print('endwith_test_speed_pl pl_df_trips...')
    for i in range(0,10):
        start_time = time.perf_counter()    
        mean_test= endwith_test_speed_pd(df_trips_pyarrow_pyarrow)
        end_time = time.perf_counter() 
        execution_time =end_time - start_time
        print(f'endwith_test_speed_pl end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,'endwith_test_speed_pl_pl_df_trips',start_time,end_time,execution_time]

    execution_times_pd = pd.DataFrame.from_dict(execution_times,orient='index',columns = ['iteration','operation','start_time','end_time','execution_time'])
    execution_times_pd.to_csv(f"Report pandas_{pd.__version__}.csv")

    print(df_trips_pyarrow_pyarrow.dtypes)
if __name__ == "__main__":
    
    main()