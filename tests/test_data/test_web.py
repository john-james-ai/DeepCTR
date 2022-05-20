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
# Created    : Friday May 13th 2022 07:05:02 pm                                                    #
# Modified   : Friday May 13th 2022 07:05:02 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import shutil
import logging
import logging.config

from deepctr.utils.file import TempFile
from deepctr.data.web import S3
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
BUCKET = "deepctr"
FLAT = "binding.csv"
ZIP = "forbidden.csv.tar.gz"
DIRECTORY = "unbound"


def get_params(compress: str):
    d = {}
    d["filepath"] = os.path.join(HOME, FLAT) + ".csv"
    d["filepath_compressed"] = os.path.join(HOME, ZIP) + ".csv.tar.gz"
    d["folder"] = "exacerbated"
    d["object"] = os.path.join(FOLDER, os.path.basename(d["filepath"]))
    d["object_compressed"] = os.path.join(FOLDER, os.path.basename(d["filepath"])) + ".tar.gz"
    d["directory"] = os.path.join(HOME, DIRECTORY)
    os.makedirs(d["directory"], exist_ok=True)
    return d


@pytest.mark.s3
class TestS3:
    def test_upload_file_uncompressed(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(compress=False)
        with TempFile(params["filepath"]) as tf:
            io = S3()
            io.upload_file(
                filepath=params["filepath"],
                bucket=BUCKET,
                object=params["object"],
                compress=False,
                force=True,
            )

            assert io.exists(BUCKET, params["object"]), logger.error("File didn't make it.")
            assert os.path.exists(tf), "Tempfile has been removed."
        assert os.path.exists(tf), "Tempfile has not been removed."

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_uncompressed_already_exists(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(compress=False)
        with TempFile(params["filepath"]) as tf:
            io = S3()
            io.upload_file(
                filepath=params["filepath"],
                bucket=BUCKET,
                object=params["object"],
                compress=False,
                force=False,
            )

            assert io.exists(BUCKET, params["object"]), logger.error("File didn't make it.")
            assert os.path.exists(tf), "Tempfile has been removed."
        assert os.path.exists(tf), "Tempfile has not been removed."

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_compressed(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(compress=False)
        with TempFile(params["filepath"]) as tf:
            io = S3()
            io.upload_file(
                filepath=params["filepath"],
                bucket=BUCKET,
                object=params["object"],
                compress=True,
                force=True,
            )

            assert io.exists(BUCKET, params["object"]), logger.error("File didn't make it.")
            assert os.path.exists(tf), "Tempfile has been removed."
        assert os.path.exists(tf), "Tempfile has not been removed."

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(compress=False)
        with TempFile(params["filepath"], data=False) as tf:
            io = S3()
            io.download_file(
                filepath=params["filepath"],
                bucket=BUCKET,
                object=params["object"],
                expand=False,
                force=True,
            )

            assert io.exists(BUCKET, params["object"]), logger.error("File didn't make it.")
            assert os.path.exists(tf), "Tempfile has been removed."
        assert os.path.exists(tf), "Tempfile has not been removed."

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_overwrite(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(compress=False)
        with TempFile(params["filepath"], data=False) as tf:
            io = S3()
            io.download_file(
                filepath=params["filepath"],
                bucket=BUCKET,
                object=params["object"],
                expand=False,
                force=True,
            )

            assert io.exists(BUCKET, params["object"]), logger.error("File didn't make it.")
            assert os.path.exists(tf), "Tempfile has been removed."
        assert os.path.exists(tf), "Tempfile has not been removed."

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_already_exists(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        params = get_params(compress=False)
        with TempFile(params["filepath"], data=False) as tf:
            io = S3()
            io.download_file(
                filepath=params["filepath"],
                bucket=BUCKET,
                object=params["object"],
                expand=False,
                force=False,
            )

            assert io.exists(BUCKET, params["object"]), logger.error("File didn't make it.")
            assert os.path.exists(tf), "Tempfile has been removed."
        assert os.path.exists(tf), "Tempfile has not been removed."

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    # def test_delete_folder(self, caplog, csvfile) -> None:
    #     logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    #     params = get_params(compress=False)
    #     with TempFile(params["filepath"], data=False) as tf:
    #         io = S3()
    #         io.download_file(
    #             filepath=params["filepath"],
    #             bucket=BUCKET,
    #             object=params["object"],
    #             expand=False,
    #             force=False,
    #         )

    #     io = S3()
    #     assert io.exists(bucket, object), logger.error("Object doesn't exist")

    #     io.delete_object(bucket, object)
    #     assert not io.exists(bucket, object), logger.error("Object not deleted")

    #     logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree(HOME, ignore_errors=True)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
