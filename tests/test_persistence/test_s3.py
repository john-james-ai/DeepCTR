#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_s3.py                                                                         #
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


from deepctr.persistence.io import S3
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.s3
class TestS3:
    def test_upload_file(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/data/aws/aws_test1.csv"
        bucket = "deepctr"
        object = "test/aws_test1.csv"

        io = S3()
        io.upload_file(filepath, bucket, object)
        assert io.exists(bucket, object), logger.error("File didn't make it.")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_directory(self, caplog) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        directory = "tests/data/aws/"
        bucket = "deepctr"
        folder = "test"
        object = "test/aws_test2.csv"

        io = S3()
        io.upload_directory(directory, bucket, folder)
        assert io.exists(bucket, object), logger.error("File didn't make it.")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/data/aws2/aws_test1.csv"
        bucket = "deepctr"
        object = "test/aws_test1.csv"

        io = S3()
        io.download_file(bucket, object, filepath)
        assert os.path.exists(filepath), logger.error("File was not downloaded")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_directory(self, caplog) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        directory = "tests/data/aws/"
        bucket = "deepctr"
        folder = "test"
        filepath = "tests/data/aws/aws_test2.csv"

        io = S3()
        io.download_directory(bucket, folder, directory)
        assert os.path.exists(filepath), logger.error("Download directory failed")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
