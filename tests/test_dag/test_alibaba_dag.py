#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_alibaba_dag.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 14th 2022 02:30:32 am                                                  #
# Modified   : Saturday May 14th 2022 02:30:32 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import os
import inspect
import pytest
import logging
import shutil
from pyspark.sql import SparkSession

from deepctr.utils.config import YamlIO
from deepctr.dag.orchestrator import DataDAGBuilder
from deepctr.utils.printing import Printer

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.alibaba
class TestAlibabaETL:
    def test_setup(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree("data/test/alibaba/staged", ignore_errors=True)

        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_engine(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = YamlIO()
        config_filepath = "config/alibaba/alibaba_etl.yml"
        config = io.read(config_filepath)

        builder = DataDAGBuilder()
        builder.build(config=config)
        builder.dag.run(start=0, stop=15)

        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_impression(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "data/test/alibaba/staged/impression.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_user(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "data/test/alibaba/staged/user.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_ad(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "data/test/alibaba/staged/ad.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_behavior(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "data/test/alibaba/staged/behavior.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_tear_down(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree("data/test/alibaba/staged", ignore_errors=True)

        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, filepath, title):
        spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        sdf = spark.read.parquet(filepath)
        printer = Printer()
        print(sdf.rdd.getNumPartitions())
        printer.print_spark_dataframe_summary(content=sdf, title=title)
