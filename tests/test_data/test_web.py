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


from deepctr.dal.io import S3
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


HOME = "tests/data/s3/"
FOLDER = "test/s3"
BASE = "tests/data/"


@pytest.mark.s3
class TestS3:
    def test_upload_file(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = os.path.join(BASE, "csvfile", os.path.basename(csvfile))
        bucket = "deepctr"
        object = os.path.join(FOLDER, os.path.basename(csvfile))

        io = S3()
        io.upload_file(filepath, bucket, object, force=True)
        assert io.exists(bucket, object), logger.error("File didn't make it.")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = os.path.join(HOME, "download_file")
        bucket = "deepctr"
        object = os.path.join(FOLDER, os.path.basename(csvfile))

        io = S3()
        io.download_file(bucket, object, filepath, force=True)
        assert os.path.exists(filepath), logger.error("File was not downloaded")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_directory(self, caplog, csvfiles) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        directory = csvfiles
        bucket = "deepctr"
        folder = os.path.join(FOLDER, "upload_directory")

        io = S3()
        io.upload_directory(directory, bucket, folder, force=True)

        objects = os.listdir(csvfiles)
        for object in objects:
            object = os.path.join(folder, os.path.basename(object))
            assert io.exists(bucket, object), logger.error("File didn't make it.")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_directory(self, caplog) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        directory = os.path.join(HOME, "download_directory")
        bucket = "deepctr"
        folder = os.path.join(FOLDER, "upload_directory")
        filepath = os.path.join(directory, "dataframe_2.csv")

        io = S3()
        io.download_directory(bucket, folder, directory, force=True)
        assert os.path.exists(filepath), logger.error("Download directory failed")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete(self, caplog, csvfile) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        bucket = "deepctr"
        object = os.path.join(FOLDER, os.path.basename(csvfile))

        io = S3()
        assert io.exists(bucket, object), logger.error("Object doesn't exist")

        io.delete_object(bucket, object)
        assert not io.exists(bucket, object), logger.error("Object not deleted")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # bucket = "deepctr"
        # folder = "s3/"

        # shutil.rmtree(HOME)
        # io = S3()
        # io.delete_folder(bucket, folder)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
