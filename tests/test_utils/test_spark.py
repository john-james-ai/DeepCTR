#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /test_spark.py                                                                        #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, May 3rd 2022, 6:30:23 pm                                                     #
# Modified : Tuesday, May 3rd 2022, 11:55:13 pm                                                    #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
import pyspark
import time
import pandas as pd
from sklearn.datasets import load_wine
from deepctr.utils.io import Parquet

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #
LOCAL_FILEPATH = "tests/data/test_df.csv"
PQFILEPATH1 = "tests/data/test_df1.parquet"
PQFILEPATH2 = "tests/data/test_df2.parquet"
PQFILEPATH3 = "tests/data/test_df3.parquet"
SDF = None
PDF = None


@pytest.mark.spark
class TestSparkIO:
    def test_write_pandas(self, caplog) -> None:
        logger.info("\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        caplog.set_level(logging.INFO)

        data = load_wine(as_frame=True)
        pdf = data["data"]

        assert isinstance(pdf, pd.DataFrame), logger.error("Not a pandas DataFrame")

        io = Parquet()
        io.write_pandas(data=pdf, filepath=PQFILEPATH1)

        assert os.path.exists(PQFILEPATH1), logger.error("Parquet file does not exist")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog) -> None:
        logger.info("\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        caplog.set_level(logging.INFO)

        time.sleep(3)

        io = Parquet()
        SDF = io.read(PQFILEPATH1)
        assert isinstance(SDF, pyspark.sql.DataFrame), logger.error("SDF is not a Spark DataFrame")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read_pandas(self, caplog) -> None:
        logger.info("\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        caplog.set_level(logging.INFO)

        time.sleep(3)

        io = Parquet()
        PDF = io.read_pandas(PQFILEPATH1)
        assert isinstance(PDF, pd.DataFrame), logger.error("Data is not a DataFrame")
        assert PDF.shape[0] > 0, logger.error("Pandas dataframe has no rows")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_write(self, caplog) -> None:
        logger.info("\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        caplog.set_level(logging.INFO)

        time.sleep(3)

        io = Parquet()
        SDF = io.read(PQFILEPATH1)
        io.write(data=SDF, filepath=PQFILEPATH2)

        assert os.path.exists(PQFILEPATH2), logger.error("Parquet file does not exist")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
