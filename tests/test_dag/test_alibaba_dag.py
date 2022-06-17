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
# Modified   : Saturday May 28th 2022 06:06:09 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import os
import inspect
import pytest
import logging
import logging.config
import shutil
from pyspark.sql import SparkSession

from deepctr.utils.config import YamlIO
from deepctr.dag.orchestrator import DataDAGBuilder
from deepctr.utils.printing import Printer
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.skip()
class TestAlibabaETL:
    def test_setup(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree("tests/data/alibaba/vesuvio/staged", ignore_errors=True)

        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_engine(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = YamlIO()
        config_template_filepath = "config/alibaba.yml"
        config_template = io.read(config_template_filepath)

        builder = DataDAGBuilder()
        dag = (
            builder.datasource("alibaba")
            .dataset("vesuvio")
            .at("tests/data")
            .with_template(config_template)
            .and_context(context)
            .build()
            .dag
        )
        dag.run(start=0, stop=20)

        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_raw(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        directory = "tests/data/alibaba/vesuvio/raw/"
        files = ["raw_sample.csv", "ad_feature.csv", "behavior_log.csv", "user_profile.csv"]
        for file in files:
            filepath = os.path.join(directory, file)
            assert os.path.exists(filepath), logger.error("File {} was not created".format(file))

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_impression(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "tests/data/alibaba/vesuvio/staged/impression.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_user(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "tests/data/alibaba/vesuvio/staged/user.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_ad(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "tests/data/alibaba/vesuvio/staged/ad.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_behavior(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        destination = "tests/data/alibaba/vesuvio/staged/behavior.parquet"
        assert os.path.exists(destination), logger.error("TestAlibabaETL: Extract Failed")

        title = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
        self.show(destination, title)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def show(self, filepath, title):
        spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        sdf = spark.read.parquet(filepath)
        printer = Printer()
        print(sdf.rdd.getNumPartitions())
        printer.print_spark_dataframe_summary(content=sdf, title=title)
