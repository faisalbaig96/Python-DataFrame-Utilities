import pandas as pd
import glob 
import os
import re
from datetime import timedelta
import datetime as dt
from numpy import inf
import numpy as np
import sys


class DataFrameUtility:

    @staticmethod
    def column_check(columns, df):
        # check whether columns in df are matchimg with class attribute
        # returns the list of missing colums

        dataframe_columns = list(df.columns)

        missing_columns = [x for x in columns if x not in dataframe_columns]

        return missing_columns
    
    @staticmethod
    def column_data_type_check(column_data_type_mapping, df):
        # compares the data type of column with dictionary and returns 
        # the column if there is a mis-match
    
        data_type_info = []
        df_column_dtype_mapping = df.dtypes.astype('str').to_dict()
        
        for k in column_data_type_mapping:
            if column_data_type_mapping[k] != df_column_dtype_mapping.get(k):
                wrong_data_type = {}
                
                wrong_data_type.update({'column_name' : k, 
                                        'expected_data_type' : column_data_type_mapping[k],
                                        'received_data_type' : df_column_dtype_mapping[k]})
                
                data_type_info.append(wrong_data_type)

        return data_type_info



    @staticmethod
    def null_column_check(columns, df):
        # returns list of columns that have atleast a null value

        df = df[columns]
        null_columns = df.columns[df.isnull().any()].tolist()

        return null_columns


    @staticmethod
    def anti_join(left_df, right_df, on):
        # Does a left only join with specified list of columns in parameter

        right_df = right_df[on]
        right_df.drop_duplicates(inplace=True)

        result = pd.merge(left_df, right_df, on = on, how='left', indicator=True)
        result = result[result['_merge'] == 'left_only']
        result.drop(columns = {'_merge'}, inplace=True)

        return result