#!/usr/bin/env python
# coding: utf-8

import boto3
import io
import pandas as pd
import numpy as np
import os


class ConnectToAws():
    # TODO: retrieve all files
    # TODO: show all files and files sizes
    def __init__(self, S3_BUCKET_NAME):
        self.__S3_BUCKET_NAME = S3_BUCKET_NAME
        self.__s3 = boto3.client('s3')

    def download_file(self,filename):
        try:
            self.__s3.download_file(Bucket=self.__S3_BUCKET_NAME, 
                                    Key=filename, 
                                    Filename=filename)
        except:
            print("File not found")            

    def download_file_as_pandas(self,filename):
        try:
            obj = self.__s3.get_object(Bucket=self.__S3_BUCKET_NAME, Key=filename)
            df = pd.read_json(io.BytesIO(obj['Body'].read()))
            
            return df
        except:
            print("File not found")


def make_stronger_id(df, columns):
    for column in columns:
        df[column] = df[column].fillna(0).astype('Int64').astype(str)
        df.loc[df[column] == '0', column] = np.nan
    
    return df

    
def main():
   
    S3_BUCKET_NAME =''
    
    aws = ConnectToAws(S3_BUCKET_NAME)
    
if __name__ == "__main__":
    print("Start")
    main()
    print("End")
