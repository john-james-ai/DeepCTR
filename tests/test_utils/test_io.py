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
import inspect
import pytest
import logging
import time
import pandas as pd
from deepctr.utils.io import SparkS3

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


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
