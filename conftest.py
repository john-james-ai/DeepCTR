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
from pyspark.sql import SparkSession
from sklearn.datasets import load_iris

from deepctr.dal import STAGES
from deepctr.dal.base import File, Dataset
from deepctr.data.database import ConnectionFactory
from deepctr.utils.database import parse_sql

CONNECTION = {
    "setup": "tests/test_dal/test_dao_setup.sql",
    "teardown": "tests/test_dal/test_dao_teardown.sql",
}

# ------------------------------------------------------------------------------------------------ #
#                                        IGNORE                                                    #
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = ["tests/old_tests/**/*.py"]


# ------------------------------------------------------------------------------------------------ #
#                                           FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def file():
    return File(
        name="test_file",
        source="alibaba",
        storage_type="local",
        format="csv",
        stage_id=2,
        home="tests/data",
        dataset_id=0,
        dataset="test_dataset",
        filepath="tests/data/data_store/csvfile.csv",
    )


# ------------------------------------------------------------------------------------------------ #
#                                           FILE2                                                  #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def file2():
    """For non-existing file tests"""
    return File(
        name="test_file",
        source="alibaba",
        storage_type="local",
        format="csv",
        stage_id=2,
        home="tests/data",
        dataset_id=0,
        dataset="test_dataset",
    )


# ------------------------------------------------------------------------------------------------ #
#                                         DATASET                                                  #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dataset():
    datastore = "tests/data/data_store"
    dataset = Dataset(
        name="test_dataset",
        source="avazu",
        storage_type="local",
        folder=datastore,
        format="csv",
        stage_id=1,
        home="tests/data",
    )

    for i in range(1, 5):
        filename = "csvfile{}.csv".format(i)
        filepath = os.path.join(datastore, filename)
        file = File(
            name=filename.splitext(".")[0],
            source="avazu",
            storage_type="local",
            format="csv",
            stage=1,
            home="tests/data",
            filepath=filepath,
            dataset_id=0,
            dataset="test_dataset",
        )
        dataset.add_file(file)
    return dataset


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

    connection = ConnectionFactory(database="testdal").get_connection()
    yield connection
    statements = parse_sql(filename=CONNECTION.get("teardown"))
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
        connection.commit()
    connection.close()


# ------------------------------------------------------------------------------------------------ #
#                                         RAO TEST                                                 #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def local_source():
    file = File(
        name="test_upload_file",
        source="alibaba",
        dataset="test_dataset",
        storage_type="local",
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
        storage_type="local",
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
        storage_type="s3",
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
        storage_type="local",
        format="csv",
        stage_id=2,
        stage_name=STAGES.get(2),
        home="tests/data",
    )
    return file
