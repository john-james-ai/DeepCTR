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
import logging.config
from pyspark.sql import DataFrame

from deepctr.persistence.dal import SparkCSV, SparkParquet
from deepctr.utils.printing import Printer
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dal
class TestSparkCSV:
    def test_write(self, caplog, spark_dataframe, csv_filepath) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sdf = spark_dataframe
        filepath = csv_filepath

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(sdf, title)

        io = SparkCSV()
        io.write(data=sdf, filepath=filepath)

        assert os.path.exists(filepath), logging.error("SparkCSV failed to write")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog, csv_filepath) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = csv_filepath

        io = SparkCSV()
        sdf = io.read(filepath, header=True, sep=",")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(sdf, title)

        assert isinstance(sdf, DataFrame), logging.error(
            "SparkCSV failed to return a pyspark.sql.DataFrame object"
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read_fail(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "test/filenotfound.txt"

        io = SparkCSV()
        with pytest.raises(FileNotFoundError):
            io.read(filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, sdf, title):
        printer = Printer()
        printer.print_spark_dataframe_summary(content=sdf, title=title)


@pytest.mark.dal
class TestSparkParquet:
    def test_write(self, caplog, spark_dataframe, parquet_filepath) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sdf = spark_dataframe
        filepath = parquet_filepath

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(sdf, title)

        io = SparkParquet()
        io.write(data=sdf, filepath=filepath)

        assert os.path.exists(filepath), logging.error("SparkParquet failed to write")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog, parquet_filepath) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = parquet_filepath

        io = SparkParquet()
        sdf = io.read(filepath)

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(sdf, title)

        assert isinstance(sdf, DataFrame), logging.error(
            "SparkParquet failed to return a pyspark.sql.DataFrame object"
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read_fail(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "test/filenotfound.txt"

        io = SparkParquet()
        with pytest.raises(FileNotFoundError):
            io.read(filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, sdf, title):
        printer = Printer()
        printer.print_spark_dataframe_summary(content=sdf, title=title)
