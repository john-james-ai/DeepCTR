#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /test_io.py                                                                           #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, April 10th 2022, 5:26:12 pm                                                   #
# Modified : Saturday, April 16th 2022, 7:50:58 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import inspect
import shutil
import pytest
import logging
import time
from pyspark.sql import SparkSession
import pandas as pd
from deepctr.utils.io import SparkS3, Parquet, FileManager

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.filemanager
class TestFileManager:
    def test_check_in_ok(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        asset_type = "data"
        collection = "aliba"
        item = "user_profile"
        stage = "staged"
        fileformat = ".csv"
        mode = "prad"

        lib = FileManager()
        filepath = lib.check_in(asset_type, collection, item, stage, fileformat, mode)
        assert "data" in filepath, logger.info(
            "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
        assert "alibaba" in filepath, logger.info(
            "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
        assert "user" in filepath, logger.info(
            "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
        assert "staged" in filepath, logger.info(
            "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
        assert ".csv" in filepath, logger.info(
            "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
        assert "prod" in filepath, logger.info(
            "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )

    def test_check_in_fail(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        asset_type = "data"
        collection = "aliba"
        item = "user_profile"
        stage = "raw"
        fileformat = ".csv"
        mode = "prad"

        lib = FileManager()
        with pytest.raises(FileExistsError):
            filepath = lib.check_in(asset_type, collection, item, stage, fileformat, mode)
            assert filepath is None, logger.info(
                "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
            )

    def test_check_out_ok(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        asset_type = "data"
        collection = "aliba"
        item = "user_profile"
        stage = "raw"
        fileformat = ".csv"
        mode = "prad"

        lib = FileManager()
        filepath = lib.check_out(asset_type, collection, item, stage, fileformat, mode)

        assert isinstance(filepath, str), logger.info(
            "\tFailed in {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )

    def test_check_out_fail(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        asset_type = "data"
        collection = "aliba"
        item = "user_profile"
        stage = "staged"
        fileformat = ".csv"
        mode = "prad"

        lib = FileManager()

        with pytest.raises(FileNotFoundError):
            filepath = lib.check_out(asset_type, collection, item, stage, fileformat, mode)
            assert not isinstance(filepath, str)


@pytest.mark.parquet
class TestParquet:
    def test_parquet(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        filepath1 = "data/alibaba/development/user_profile.csv"
        filepath2 = "tests/data/test_df1.parquet"

        if os.path.exists(filepath2):
            shutil.rmtree(filepath2)

        spark = SparkSession.builder.master("local[1]").appName("TestSpark").getOrCreate()
        df1 = spark.read.csv(filepath1)

        io = Parquet()
        io.write(df1, filepath2)

        assert os.path.exists(filepath2), "Parquet Error: Write didn't happen"

        df2 = io.read(filepath2)

        assert df1.schema == df2.schema, "Parquet Read Write Failure"


@pytest.mark.s3
class TestSparkS3:
    def test_s3_write(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        local_filepath = "tests/data/test_df.csv"
        s3_filepath = "test/test_df.csv"
        pdf = pd.read_csv(local_filepath)

        writer = SparkS3()
        writer.write(pdf, filepath=s3_filepath, bucket="deepctr")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_read(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        time.sleep(3)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        s3_filepath = "test/test_df.csv"
        reader = SparkS3()
        df = reader.read(filepath=s3_filepath, bucket="deepctr")
        assert isinstance(df, pd.DataFrame), "Read didn't return a dataframe"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
