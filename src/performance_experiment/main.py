import pandas as pd
import time
import pyarrow as pa


import polars as pl
import time

from utility import pl_read_csv, pl_read_parquet, pd_read_parquet_default, pd_read_csv, conduct_experiment, pd_read_parquet
from performance_tests import pl_mean_test_speed, mean_test_speed_pd, endwith_test_speed_pd, endwith_test_speed_pl




def main():
    
    path_parquet="E:/datasets/Tests/yellow_tripdata_2021-01.parquet"
    path_csv="E:/datasets/Tests/yellow_tripdata_2021-01.csv" 
    results_path="E:/datasets/Tests/results.csv"
    iterations = 10


    df_pd_pyarrow_pyarrow = pd_read_parquet(path_parquet,"pyarrow","pyarrow")
    df_pl = pl_read_parquet(path_parquet)
    experiments_dict = {
        "pd_read_csv":{
            "func": pd_read_csv,
            "args":[path_csv,"numpy_nullable","pyarrow"]
            },
        "pd_read_parquet":{
            "func": pd_read_parquet,
            "args":[path_parquet,"pyarrow","pyarrow"]
            },
        "pd_read_parquet_default":{
            "func": pd_read_parquet_default,
            "args":[path_parquet]
            },
        "pl_read_parquet":{
            "func": pl_read_parquet,
            "args":[path_parquet]
            },
        "pl_read_csv":{
            "func": pl_read_csv,
            "args":[path_csv]
            },
        "pl_mean_test_speed":{
            "func": pl_mean_test_speed,
            "args":[df_pl]
            },
        "mean_test_speed_pd":{
            "func": mean_test_speed_pd,
            "args":[df_pd_pyarrow_pyarrow]
            },
        "endwith_test_speed_pd":{
            "func": endwith_test_speed_pd,
            "args":[df_pd_pyarrow_pyarrow]
            },
        "endwith_test_speed_pl":{
            "func": endwith_test_speed_pl,
            "args":[df_pl]
            },
    }
    
    print(f'Starting ETL for Pandas version {pd.__version__}')
    
    #execution_times_summary = {}
    execution_times_summary = pd.DataFrame(columns=['iteration','description','start_time','end_time','execution_time'])
    for key in experiments_dict.keys():
        print("Starting experiment: ",key)
        func_args = experiments_dict[key]["args"]
        func = experiments_dict[key]["func"]
        execution_times = conduct_experiment(func,func_args,key,iterations)
        
        execution_times_summary = pd.concat([execution_times_summary,pd.DataFrame.from_dict(execution_times, orient='index', columns=['iteration','description','start_time','end_time','execution_time'])])
    
    print(execution_times_summary)
    execution_times_summary.to_csv(results_path)
    
if __name__ == "__main__":
    
    main()