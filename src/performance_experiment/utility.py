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

