#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepNeuralCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /test_io.py                                                                           #
# Language : Python 3.10.4                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepNeuralCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, April 8th 2022, 10:34:35 am                                                   #
# Modified : Saturday, April 9th 2022, 4:00:53 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import inspect
import pytest
import logging
import findspark
from deepctr.utils.io import SparkS3, SparkIO

findspark.init()


# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.sparks3
class TestS3io:
    def test_s3_spark_io(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/data/test_df.csv"
        s3_filepath = "test/"
        bucket = "deepctr"

        # Read csv and create Spark DataFrame
        io = SparkIO()
        df1 = io.read(filepath)

        writer = SparkS3()
        writer.write(data=df1, filepath=s3_filepath, bucket=bucket)

        logger.info("Loaded dataframe onto S3 bucket")

        io = SparkIO()
        df2 = io.read(s3_filepath, bucket=bucket)

        df1 = df1.toPandas()
        df2 = df2.toPandas()

        assert df1.equal(df2), "Inbound outbound dataframes are not equal"

        logger.info("Read dataframe from S3 bucket")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
