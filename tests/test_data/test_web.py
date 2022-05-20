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

from deepctr.utils.file import file_creator
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


HOME = "tests/data/s3/data/web"
FOLDER = "test/s3/data/web"
BUCKET = "deepctr"
FILENAME = "binding.csv"


def get_upload_params(compress: str):
    d = {}
    d["folder"] = FOLDER
    d["filepath"] = os.path.join(HOME, FILENAME)
    d["object"] = os.path.join(FOLDER, FILENAME)
    if compress:
        d["object"] = d["object"] + ".tar.gz"

    os.makedirs(os.path.dirname(d["filepath"]), exist_ok=True)
    return d


def test_upload(compress, force) -> None:

    params = get_upload_params(compress=compress)

    io = S3()
    io.upload_file(
        filepath=params["filepath"],
        bucket=BUCKET,
        object=params["object"],
        compress=compress,
        force=force,
    )
    assert io.exists(BUCKET, params["object"]), logger.error(
        "Failure in {}".format(inspect.stack()[1][3])
    )


@pytest.mark.data_web
@pytest.mark.data_web_upload
class TestS3Upload:
    def test_setup(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = os.path.join(HOME, FILENAME)
        file_creator(filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_uncompressed(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        compress = False
        force = False
        test_upload(compress, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_uncompressed_already_exists(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        compress = False
        force = False
        test_upload(compress, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_uncompressed_already_exists_overwrite(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        compress = False
        force = True
        test_upload(compress, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_compressed(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        compress = True
        force = False
        test_upload(compress, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_compressed_already_exists(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        compress = True
        force = False
        test_upload(compress, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_upload_file_compressed_already_exists_overwrite(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        compress = True
        force = True
        test_upload(compress, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree(HOME, ignore_errors=True)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


def get_download_params(expand: str):
    d = {}
    d["folder"] = FOLDER
    if expand:
        d["filepath"] = os.path.join(HOME, FILENAME)
        d["object"] = os.path.join(FOLDER, FILENAME) + ".tar.gz"
    else:
        d["filepath"] = os.path.join(HOME, FILENAME) + ".tar.gz"
        d["object"] = os.path.join(FOLDER, FILENAME)

    os.makedirs(os.path.dirname(d["filepath"]), exist_ok=True)
    return d


def test_download(expand, force) -> bool:

    params = get_download_params(expand=expand)

    io = S3()
    io.download_file(
        filepath=params["filepath"],
        bucket=BUCKET,
        object=params["object"],
        expand=expand,
        force=force,
    )

    assert os.path.exists(params["filepath"]), logger.error(
        "Failure in {}".format(inspect.stack()[1][3])
    )


@pytest.mark.data_web
@pytest.mark.data_web_download
class TestS3Download:
    def test_setup(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree(HOME, ignore_errors=True)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_unexpanded(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        EXPAND = False
        force = False
        test_download(EXPAND, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_unexpanded_already_exists(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        EXPAND = False
        force = False
        test_download(EXPAND, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_unexpanded_already_exists_overwrite(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        EXPAND = False
        force = True
        test_download(EXPAND, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_expanded(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        EXPAND = True
        force = False
        test_download(EXPAND, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_expanded_already_exists(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        EXPAND = True
        force = False
        test_download(EXPAND, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file_expanded_already_exists_overwrite(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        EXPAND = True
        force = True
        test_download(EXPAND, force)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_teardown(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree(HOME, ignore_errors=True)
        io = S3()
        io.delete_folder(bucket=BUCKET, folder=FOLDER)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
