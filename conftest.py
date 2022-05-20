#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /conftest.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, March 14th 2022, 7:17:14 pm                                                   #
# Modified : Thursday, April 21st 2022, 5:42:37 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import pytest
import shutil
import tarfile
from pyspark.sql import SparkSession
from deepctr.dal.params import DatasetParams, FileParams, S3Params
from sklearn.datasets import load_iris

# ------------------------------------------------------------------------------------------------ #
#                                     SPARK FIXTURES                                               #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def spark_dataframe():
    filepath = "tests/data/test.parquet"
    spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark.read.parquet(filepath)


# ------------------------------------------------------------------------------------------------ #
#                                      TEST FILES                                                  #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="class")
def csv_filepath():
    filepath = "tests/data/test_write.csv"
    yield filepath
    shutil.rmtree(filepath, ignore_errors=True)


@pytest.fixture(scope="class")
def parquet_filepath():
    filepath = "tests/data/test_write.parquet"
    yield filepath
    shutil.rmtree(filepath, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #
#                                      DATA PARAMS                                                 #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="class")
def dataset_params():

    dp = DatasetParams(datasource="alibaba", dataset="vesuvio", stage="raw", home="tests/data/",)
    return dp


@pytest.fixture(scope="class")
def data_entity_params():

    dp = FileParams(
        datasource="alibaba",
        entity="user",
        dataset="vesuvio",
        stage="raw",
        home="tests/data/",
        format="csv",
    )
    return dp


@pytest.fixture(scope="class")
def s3_params():

    dp = S3Params(bucket="deepctr", folder="vesuvio", object="alibaba/vesuvio",)
    return dp


# ------------------------------------------------------------------------------------------------ #
#                                      CSV FILES                                                   #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="class")
def csvfile(dp_std):

    home = "tests/data/csvfile/"
    os.makedirs(home, exist_ok=True)
    data = load_iris(return_X_y=False, as_frame=True)

    filename = "csvfile.csv"
    filepath = os.path.join(home, filename)
    data["data"].to_csv(filepath)
    yield filepath
    # shutil.rmtree(filepath, ignore_errors=True)


@pytest.fixture(scope="class")
def csvfiles(dp_std):

    directory = "tests/data/csvfiles/"
    os.makedirs(directory, exist_ok=True)
    data = load_iris(return_X_y=False, as_frame=True)
    for i in range(5):
        filename = "dataframe" + "_" + str(i) + ".csv"
        filepath = os.path.join(directory, filename)
        data["data"].to_csv(filepath)
    yield directory
    # shutil.rmtree(directory, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #
#                                      ZIP FILES                                                   #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="class")
def zipfile(dp_std):

    source = "tests/data/csvfile/csvfile.csv"
    archive = "tests/data/archive.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(source)
    yield archive
    shutil.rmtree(archive, ignore_errors=True)


@pytest.fixture(scope="class")
def zipfiles(dp_std):

    source = "tests/data/csvfiles"
    archive = "tests/data/archive.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        files = os.listdir(source)
        for file in files:
            filepath = os.path.join(source, file)
            tar.add(filepath)
    yield archive
    shutil.rmtree(archive, ignore_errors=True)
