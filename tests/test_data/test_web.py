#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_web.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 22nd 2022 01:40:01 am                                                    #
# Modified   : Friday June 17th 2022 11:16:39 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pytest
import logging
import shutil
import os
import logging.config

import time
from deepctr.utils.log_config import LOG_CONFIG
from deepctr.data.web import S3
from deepctr.utils.aws import upload_file, delete_file

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
BUCKET = "deepctr"
FILEPATH = "tests/data/test_web/exists.csv"
OBJECT_KEY = "test/exists.csv"


@pytest.mark.web
class TestWeb:
    def test_setup(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        output = "tests/data/test_web/test_download"
        shutil.rmtree(output, ignore_errors=True)

        upload_file(filepath=FILEPATH, bucket=BUCKET, object_key=OBJECT_KEY)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_exists(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        s3 = S3()
        assert s3.exists(bucket=BUCKET, object_key=OBJECT_KEY), logger.error("Exists failed.")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        filepath = "tests/data/test_web/csvfile.csv"
        object_key = None
        s3 = S3()

        # Test default object key w/o compression
        s3.upload_file(
            filepath=filepath, bucket=BUCKET, object_key=object_key, compress=False, force=True
        )
        time.sleep(2)
        assert s3.exists(bucket=BUCKET, object_key="csvfile.csv"), logger.error(
            "Upload (or exists) failed."
        )

        # Test default object key w/ compression
        s3.upload_file(filepath=filepath, bucket=BUCKET, object_key=object_key, force=True)
        time.sleep(2)
        assert s3.exists(bucket=BUCKET, object_key="csvfile.csv.tar.gz"), logger.error(
            "Upload (or exists) failed."
        )

        # Test custom object_key w/o compression
        object_key = "test/no_compression/custom.csv"
        s3.upload_file(
            filepath=filepath, bucket=BUCKET, object_key=object_key, compress=False, force=True
        )
        time.sleep(2)
        assert s3.exists(bucket=BUCKET, object_key=object_key), logger.error(
            "Upload (or exists) failed."
        )

        # Test custom object_key w/ compression
        object_key = "test/compression/custom.csv"
        s3.upload_file(filepath=filepath, bucket=BUCKET, object_key=object_key, force=True)
        object_key = object_key + ".tar.gz"
        time.sleep(2)
        assert s3.exists(bucket=BUCKET, object_key=object_key), logger.error(
            "Upload (or exists) failed."
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_compressed(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        filepath = "tests/data/test_web/test_download/output/custom.csv.tar.gz"
        object_key = "test/compression/custom.csv.tar.gz"
        s3 = S3()

        # Test expand is True
        s3.download_file(
            bucket=BUCKET, object_key=object_key, filepath=filepath, expand=False, force=True
        )
        assert os.path.exists(filepath), logger.error("Download failed. File does not exist.")

        shutil.rmtree(os.path.dirname(filepath), ignore_errors=True)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_expand(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        filepath = "tests/data/test_web/test_download/output/custom.csv.tar.gz/csvfile.csv"
        object_key = "test/compression/custom.csv.tar.gz"
        s3 = S3()

        s3.download_file(
            bucket=BUCKET, object_key=object_key, filepath=filepath, expand=True, force=True
        )
        assert os.path.exists(filepath), logger.error("Download failed. File does not exist.")

        shutil.rmtree(os.path.dirname(filepath), ignore_errors=True)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_list_object(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        bucket = "deepctr"
        folder = "test/no_compression/"
        s3 = S3()
        assert len(s3.list_objects(bucket=bucket, folder=folder)) == 3, logger.error(
            "List object failed."
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete_object(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        bucket = "deepctr"
        object_key = "test/no_compression/csvfile.csv"
        s3 = S3()
        s3.delete_object(bucket=bucket, object_key=object_key)
        assert not s3.exists(bucket=bucket, object_key=object_key), logger.error(
            "Delete object failed."
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete_folder(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        bucket = "deepctr"
        folder = "test"
        object_key = "test/no_compression/csvfile.csv"
        s3 = S3()
        s3.delete_folder(bucket=bucket, folder=folder)
        assert not s3.exists(bucket=bucket, object_key=object_key), logger.error(
            "Delete folder failed."
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        delete_file(bucket=BUCKET, object_key="test")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
