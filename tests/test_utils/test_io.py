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
# Modified : Friday, April 8th 2022, 1:19:29 pm                                                    #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import pandas as pd
import inspect
import pytest
import logging

from deepctr.data.io import S3Reader, S3Writer

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.s3io
class TestS3io:
    def test_s3_writer(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        task_no = 1
        task_name = "s3_writer_test"
        task_description = "test s3 writer class"
        params = {}
        params["filepath"] = "/test/test_dataframe.csv"

        filepath = "tests/data/test_df.csv"

        df1 = pd.read_csv(filepath)
        logger.info("*" * 80)
        logger.info(df1.shape)

        writer = S3Writer(task_no, task_name, task_description, params)
        logger.info(
            "{}\t{}\t{}\t{}".format(
                str(writer.task_no), writer.task_name, writer.task_description, writer.params
            )
        )
        writer.execute(data=df1)

        reader = S3Reader(task_no, task_name, task_description, params)
        logger.info(
            "{}\t{}\t{}\t{}".format(
                str(reader.task_no), reader.task_name, reader.task_description, reader.params
            )
        )
        df2 = reader.execute()

        assert df1.equals(df2), logger.error("DataFrames not equal")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_reader(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        task_no = 2
        task_name = "s3_reader_test"
        task_description = "test s3 reader class"
        params = {}
        params["filepath"] = "/test/test_dataframe.csv"

        filepath = "tests/data/test_df.csv"

        df1 = pd.read_csv(filepath)
        logger.info("*" * 80)
        logger.info(df1.shape)

        reader = S3Reader(task_no, task_name, task_description, params)
        logger.info(
            "{}\t{}\t{}\t{}".format(
                str(reader.task_no), reader.task_name, reader.task_description, reader.params
            )
        )
        df2 = reader.execute()

        assert df1.equals(df2), logger.error("DataFrames not equal")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
