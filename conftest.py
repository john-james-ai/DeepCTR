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
import pytest
import shutil
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------------------ #
#                                     SPARK FIXTURES                                               #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def spark_dataframe():
    filepath = "tests/data/test.parquet"
    spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
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
#                                      TEST ASSETS                                                 #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="class")
def parquet_asset(spark_dataframe):
    asset = {}
    asset["name"] = "parquet_asset"
    asset["asset"] = spark_dataframe
    asset["dataset"] = "albaba"
    asset["stage"] = "rwa"
    asset["format"] = "paruet"
    asset["mode"] = "tes"
    asset["filepath"] = "data/alibaba/test/raw/parquet_asset.parquet"
    yield asset
    shutil.rmtree(asset["filepath"], ignore_errors=True)
