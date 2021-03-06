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
import pytest
from pyspark.sql import SparkSession
from sklearn.datasets import load_iris

from deepctr.dal import STAGES
from deepctr.dal.source import Source

from deepctr.dal.file import File
from deepctr.dal.context import SourceDBContext, FileDBContext
from deepctr.data.database import ConnectionFactory, Database
from deepctr.utils.database import parse_sql

CONNECTION = {
    "setup": "tests/database/test_db_setup.sql",
    "teardown": "tests/database/test_db_teardown.sql",
}

# ------------------------------------------------------------------------------------------------ #
#                                        IGNORE                                                    #
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = ["tests/old_tests/**/*.py"]
# ------------------------------------------------------------------------------------------------ #
#                                          SOURCE                                                  #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def source():
    return Source(name="alibaba", desc="Alibaba Click Dataset", url="www.alibaba.com")


# ------------------------------------------------------------------------------------------------ #
#                                      PARQUET FILE                                                #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def parquet_file():
    return File(
        name="test_parquet_file",
        desc="Test Parquet File",
        folder="tests/data/data_store",
        format="parquet",
        filename="parquetfile.parquet",
    )


# ------------------------------------------------------------------------------------------------ #
#                                        CSVFILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def csv_file():
    """For non-existing file tests"""
    return File(
        name="test_csv_file",
        desc="Test CSV File",
        folder="tests/data/data_store",
        format="csv",
        filename="csvfile.csv",
    )


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
#                                     CONNECTION                                                   #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def connection():
    connection = ConnectionFactory().get_connection()
    statements = parse_sql(filename=CONNECTION.get("setup"))
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
        connection.commit()
    connection.close()

    connection = ConnectionFactory(database="testdb").get_connection()
    yield connection
    statements = parse_sql(filename=CONNECTION.get("teardown"))
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
        connection.commit()
    connection.close()


# ------------------------------------------------------------------------------------------------ #
#                                         DBCONTEXTS                                               #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def filecontext(connection):
    database = Database(connection)
    context = FileDBContext(database)
    return context


# # ------------------------------------------------------------------------------------------------ #
# @pytest.fixture(scope="module")
# def datasetcontext(connection):
#     database = Database(connection)
#     context = DatasetDBContext(database)
#     return context


# # ------------------------------------------------------------------------------------------------ #
# @pytest.fixture(scope="module")
# def filedatasetcontext(connection):
#     database = Database(connection)
#     dataset = DatasetDBContext(database)
#     file = FileDBContext(database)
#     context = {"file": file, "dataset": dataset}
#     return context


# # ------------------------------------------------------------------------------------------------ #
# @pytest.fixture(scope="module")
# def taskcontext(connection):
#     database = Database(connection)
#     context = TaskDBContext(database)
#     return context


# # ------------------------------------------------------------------------------------------------ #
# @pytest.fixture(scope="module")
# def dagcontext(connection):
#     database = Database(connection)
#     context = DagDBContext(database)
#     return context


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def sourcecontext(connection):
    database = Database(connection)
    context = SourceDBContext(database)
    return context


# ------------------------------------------------------------------------------------------------ #
#                                         RAO TEST                                                 #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def local_source():
    file = File(
        name="test_upload_file",
        source="alibaba",
        dataset="test_dataset",
        file_system="local",
        format="csv",
        stage_id=4,
        stage_name=STAGES.get(4),
        home="tests/data",
        filepath="tests/data/test_dal/test_rao/test_rao_upload.csv",
    )
    return file


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def local_destination():
    file = File(
        name="test_download_file",
        source="alibaba",
        dataset="test_dataset",
        file_system="local",
        format="csv",
        stage_id=4,
        stage_name=STAGES.get(4),
    )
    return file


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def s3file():
    file = File(
        name="test_upload_file",
        source="alibaba",
        dataset="test_dataset",
        file_system="s3",
        format="csv",
        stage_id=4,
        stage_name=STAGES.get(4),
        bucket="deepctr",
    )
    return file


# ------------------------------------------------------------------------------------------------ #
#                                         FAO TEST                                                 #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def fao_file():
    file = File(
        name="test_fao_create",
        source="alibaba",
        dataset="test_dataset",
        file_system="local",
        format="csv",
        stage_id=2,
        stage_name=STAGES.get(2),
        home="tests/data",
    )
    return file
