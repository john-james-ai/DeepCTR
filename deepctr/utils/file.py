#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /file.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 20th 2022 05:16:06 am                                                    #
# Modified   : Friday May 20th 2022 05:16:07 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Temporary File and Directory classes that accept directory and filenames"""
import os
import shutil
from sklearn.datasets import load_iris
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------------------ #


class TempFile:
    def __init__(self, filepath: str, data=True, format="csv") -> None:
        self._filepath = filepath
        self._data = data
        self._format = format

    def __enter__(self):
        return self._filepath

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        shutil.rmtree(self._filepath, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #


class TempDirectory:
    def __init__(self, directory: str, data=True) -> None:
        self._directory = directory
        self._data = data

    def __enter__(self):
        return self._directory

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        shutil.rmtree(self._directory, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #
class FileCreator:
    def __init__(self, filepath: str, format="csv") -> None:
        self._filepath = filepath
        self._format = format

    def __enter__(self):
        return self._filepath

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        shutil.rmtree(self._filepath, ignore_errors=True)

    def create_csv(self):
        df = self._get_dataframe()
        df.to_csv(self._filepath)

    def create_parquet(self):
        df = self._get_dataframe()
        spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        sdf = spark.createDataFrame(df)
        sdf.write.parquet(self._filepath)

    def get_dataframe(self):
        data = load_iris(return_X_y=False, as_frame=True)
        df = data["data"]
        df.columns = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
        return df


# ------------------------------------------------------------------------------------------------ #
def create_dataframe():
    data = load_iris(return_X_y=False, as_frame=True)
    df = data["data"]
    df.columns = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    return df


# ------------------------------------------------------------------------------------------------ #
def file_creator(filepath: str):

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    df = create_dataframe()

    ext = os.path.splitext(filepath)[1]

    if "csv" in ext:
        df.to_csv(filepath)
    elif "parquet" in ext:
        spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        sdf = spark.createDataFrame(df)
        sdf.write.parquet(filepath)
    else:
        raise ValueError("Unrecognized file extension.")
