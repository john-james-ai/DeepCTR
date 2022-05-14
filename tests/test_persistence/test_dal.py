#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_dal.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 07:05:02 pm                                                    #
# Modified   : Friday May 13th 2022 07:05:02 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
import shutil
from pyspark.sql import DataFrame

from deepctr.persistence.dal import SparkCSV, SparkParquet

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.dal
class TestSparkCSV:
    def test_write(self, caplog, spark_dataframe) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sdf = spark_dataframe
        filepath = "tests/data/persistence/test_spark_csv.csv"
        shutil.rmtree(filepath, ignore_errors=True)

        io = SparkCSV()
        io.write(data=sdf, filepath=filepath)

        assert os.path.exists(filepath), logging.error("SparkCSV failed to write")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/data/persistence/test_spark_csv.csv"

        io = SparkCSV()
        df = io.read(filepath)
        df.show()
        assert isinstance(df, DataFrame), logging.error(
            "SparkCSV failed to return a pyspark.sql.DataFrame object"
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.dal
class TestSparkParquet:
    def test_write(self, caplog, spark_dataframe) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sdf = spark_dataframe
        filepath = "tests/data/persistence/test_spark_parquet.parquet"
        shutil.rmtree(filepath, ignore_errors=True)

        io = SparkParquet()
        io.write(data=sdf, filepath=filepath)

        assert os.path.exists(filepath), logging.error("SparkParquet failed to write")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/data/persistence/test_spark_parquet.parquet"

        io = SparkParquet()
        df = io.read(filepath)
        df.show()
        assert isinstance(df, DataFrame), logging.error(
            "SparkParquet failed to return a pyspark.sql.DataFrame object"
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
