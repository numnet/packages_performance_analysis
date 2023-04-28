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

def pd_read_csv(path,dtype_backend, engine):
    """
    Converting parquet file into Pandas dataframe
    """
    df= pd.read_csv(path,dtype_backend=dtype_backend, engine=engine)
    return df

def pd_read_parquet(path,dtype_backend, engine):
    """
    Converting parquet file into Pandas dataframe
    """
    df= pd.read_parquet(path,dtype_backend=dtype_backend, engine=engine)
    return df

def conduct_experiment(function, func_args,description, num_iterations=10):
    """
    Conducts an experiment on a function and returns the average execution time
    """
    execution_times = {}
    for i in range(num_iterations):
        start_time = time.perf_counter()
        result = function(*func_args)
        end_time = time.perf_counter()
        execution_time  = end_time - start_time
        print(f'{description} end in {round(execution_time,3)} seconds')
        execution_times[len(execution_times)] = [i,description,start_time,end_time,execution_time]
    return execution_times


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


def main():
    
    path_parquet="E:/datasets/Tests/yellow_tripdata_2021-01.parquet"
    path_csv="E:/datasets/Tests/yellow_tripdata_2021-01.csv" 
    iterations = 10
    print(f'Starting ETL for Pandas version {pd.__version__}')
    print('Extracting Parquet pyarrow...')
    experiments_dict = {
        "pd_read_csv":{
            "func": pd_read_csv,
            "args":[path_csv,"numpy_nullable","pyarrow"]},
    }
    
    dict_key = list(experiments_dict.keys())[0]
    func_args = experiments_dict[dict_key]["args"]
    func = experiments_dict[dict_key]["func"]
    #func_args = [path_csv,"numpy_nullable","pyarrow"]
    execution_times = conduct_experiment(func,func_args,dict_key,iterations)
    execution_times_summary = pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])


    func_args = [path_parquet]
    execution_times = conduct_experiment(pd_read_parquet_default,func_args,"pd_read_parquet",iterations)
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])

    func_args = [path_parquet,"pyarrow","pyarrow"]
    execution_times = conduct_experiment(pd_read_parquet,func_args,"pd_read_parquet",iterations)
    
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])

    func_args = [path_parquet]
    execution_times = conduct_experiment(pl_read_parquet,func_args,"pl_read_parquet",iterations)
    
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])

    func_args = [path_csv]
    execution_times = conduct_experiment(pl_read_csv,func_args,"pl_read_csv",iterations)
    
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])
    

    df_pd_pyarrow_pyarrow = pd_read_parquet(path_parquet,"pyarrow","pyarrow")
    pl_df_trips = pl_read_parquet(path_parquet)

    func_args = [df_pd_pyarrow_pyarrow]
    execution_times = conduct_experiment(mean_test_speed_pd,func_args,"mean_test_speed_pd-df_pd_pyarrow_pyarrow",iterations)
    
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])

    func_args = [pl_df_trips]
    execution_times = conduct_experiment(pl_mean_test_speed,func_args,"pl_mean_test_speed-pl_df_trips",iterations)
    
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])

    func_args = [df_pd_pyarrow_pyarrow]
    execution_times = conduct_experiment(endwith_test_speed_pd,func_args,"endwith_test_speed_pd-df_pd_pyarrow_pyarrow",iterations)
    
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])

    func_args = [pl_df_trips]
    execution_times = conduct_experiment(endwith_test_speed_pl,func_args,"endwith_test_speed_pl-df_pd_pyarrow_pyarrow",iterations)
    
    execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])

if __name__ == "__main__":
    
    main()