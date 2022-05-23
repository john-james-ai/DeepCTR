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
"""Includes fixtures, classes and functions supporting testing."""
import os
import pytest
import pymysql
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sklearn.datasets import load_iris

# ------------------------------------------------------------------------------------------------ #
#                                        IGNORE                                                    #
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = ["tests/old_tests/**/*.py"]

# ------------------------------------------------------------------------------------------------ #
#                                     SPARK FIXTURES                                               #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def spark_dataframe():
    data = load_iris(return_X_y=False, as_frame=True)
    df = data["data"]
    df.columns = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark.createDataFrame(df)


# ------------------------------------------------------------------------------------------------ #
#                                       DATABASE                                                   #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def connection():
    load_dotenv()
    connection = pymysql.connections.Connection(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        database="testdb",
        charset="utf8mb4",
    )
    reset = """ALTER TABLE `testtable` AUTO_INCREMENT=1;"""
    cursor = connection.cursor()
    cursor.execute(reset)
    cursor.close()
    yield connection
    connection.close()
