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
from datetime import datetime
from pyspark.sql import SparkSession
from sklearn.datasets import load_iris
from deepctr.dal.dto import LocalFileDTO, S3FDatasetDTO, LocalDatasetDTO, S3FileDTO
from deepctr.data.database import ConnectionFactory

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
def connection_deepctr():
    connection = ConnectionFactory().get_connection()
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def connection():
    connection = ConnectionFactory().get_connection(database="testdb")
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def transaction_deepctr():
    connection = ConnectionFactory().get_connection()
    connection.begin()
    yield connection
    connection.commit()
    connection.close()


@pytest.fixture(scope="module")
def transaction():
    connection = ConnectionFactory().get_connection(database="testdb")
    connection.begin()
    yield connection
    connection.commit()
    connection.close()


# ------------------------------------------------------------------------------------------------ #
#                                          FILE DTOs                                               #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def valid_local_file_dto():
    return LocalFileDTO(
        name="test_file",
        dataset="test_dataset",
        dataset_id=101,
        datasource="avazu",
        stage="staged",
        format="csv",
        size=100,
        compressed=False,
        storage_type="local",
        dag_id=99,
        task_id=22,
        home="test/data",
    )


@pytest.fixture(scope="module")
def valid_local_file_result():
    result = {
        "name": "test_file",
        "dataset": "test_dataset",
        "dataset_id": 101,
        "datasource": "avazu",
        "stage": "staged",
        "format": "csv",
        "size": 100,
        "compressed": False,
        "filename": "test_file.csv",
        "filepath": "test/data/avazu/test_dataset/staged/test_file.csv",
        "folder": "test/data/avazu/test_dataset/staged",
        "bucket": None,
        "object_key": None,
        "storage_type": "local",
        "dag_id": 99,
        "task_id": 22,
        "home": "test/data",
        "created": datetime.now(),
    }
    return result


@pytest.fixture(scope="module")
def invalid_local_file_dto():
    return LocalFileDTO(
        name="test_file",
        dataset="test_dataset",
        dataset_id=101,
        datasource="avazu",
        stage="xxx",
        format="csv",
        size=100,
        compressed=False,
        storage_type="local",
        dag_id=99,
        task_id=22,
        home="test/data",
    )


@pytest.fixture(scope="module")
def valid_s3_file_dto():
    return S3FileDTO(
        name="test_file",
        dataset="test_dataset",
        dataset_id=101,
        datasource="avazu",
        format="csv",
        stage="staged",
        size=100,
        object_key="avazu/test_dataset/test_file.csv.tar.gz",
        bucket="deepctr",
        compressed=True,
        storage_type="s3",
        dag_id=99,
        task_id=22,
    )


@pytest.fixture(scope="module")
def valid_s3_file_result():
    result = {
        "name": "test_file",
        "dataset": "test_dataset",
        "dataset_id": 101,
        "datasource": "avazu",
        "stage": "staged",
        "format": "csv",
        "size": 100,
        "compressed": True,
        "folder": "avazu/test_dataset",
        "bucket": "deepctr",
        "object_key": "avazu/test_dataset/test_file.csv.tar.gz",
        "storage_type": "s3",
        "dag_id": 99,
        "task_id": 22,
        "created": datetime.now(),
    }
    return result


@pytest.fixture(scope="module")
def invalid_s3_file_dto():
    return S3FileDTO(
        name="test_file",
        dataset="test_dataset",
        dataset_id=101,
        datasource="avazu",
        format="csv",
        stage="XXXX",
        size=100,
        object_key="test_dataset/test_file.csv.tar.gz",
        bucket="deepctr",
        compressed=True,
        storage_type="s3",
        dag_id=99,
        task_id=22,
    )


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET DTOs                                              #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def valid_local_dataset_dto():
    return LocalDatasetDTO(
        name="test_dataset",
        datasource="avazu",
        stage="staged",
        size=100,
        storage_type="local",
        dag_id=99,
        home="test/data",
    )


@pytest.fixture(scope="module")
def valid_local_dataset_result():
    result = {
        "name": "test_dataset",
        "datasource": "avazu",
        "stage": "staged",
        "size": 100,
        "folder": "test/data/avazu/test_dataset/staged",
        "storage_type": "local",
        "dag_id": 99,
        "home": "test/data",
        "created": datetime.now(),
    }
    return result


@pytest.fixture(scope="module")
def invalid_local_dataset_dto():
    return LocalDatasetDTO(
        name="test_dataset",
        datasource="XXX",
        stage="staged",
        size=100,
        storage_type="local",
        dag_id=99,
        home="test/data",
    )


@pytest.fixture(scope="module")
def valid_s3_dataset_dto():
    return S3FDatasetDTO(
        name="test_dataset",
        datasource="avazu",
        stage="staged",
        size=400,
        folder="avazu/test_dataset",
        bucket="deepctr",
        storage_type="s3",
        dag_id=99,
    )


@pytest.fixture(scope="module")
def valid_s3_dataset_result():
    result = {
        "name": "test_dataset",
        "datasource": "avazu",
        "stage": "staged",
        "size": 400,
        "folder": "avazu/test_dataset",
        "bucket": "deepctr",
        "storage_type": "s3",
        "dag_id": 99,
        "created": datetime.now(),
    }
    return result


@pytest.fixture(scope="module")
def invalid_s3_dataset_dto():
    return S3FDatasetDTO(
        name="test_dataset",
        datasource="xxxx",
        stage="staged",
        size=400,
        folder="avazu/test_dataset",
        bucket="deepctr",
        storage_type="s3",
        dag_id=99,
    )
