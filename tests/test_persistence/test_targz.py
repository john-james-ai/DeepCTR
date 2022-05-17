#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_targz.py                                                                      #
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

from deepctr.persistence.io import TarGZ
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


@pytest.mark.tar
class TestTarGZ:
    def test_compress_file(self, caplog, csvfile) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = csvfile
        destination = "tests/data/targz/compressed_file.csv.tar.gz"

        io = TarGZ()
        io.compress(source, destination)
        assert os.path.exists(destination), logger.error("Compression failed.")

    def test_compress_dir(self, caplog, csvfiles) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = csvfiles
        destination = "tests/data/targz/compressed_files.csv.tar.gz"

        io = TarGZ()
        io.compress(source, destination)
        assert os.path.exists(destination), logger.error("Compression failed.")

    def test_expand_file(self, caplog, zipfile) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = zipfile
        destination = "tests/data/expand/single_file/"

        io = TarGZ()
        io.expand(source, destination)
        assert len(os.listdir(destination)) == 1, logger.error("Expand didn't happen")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_expand_directory(self, caplog, zipfiles) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = zipfiles
        destination = "tests/data/expand/multiple_files/"

        io = TarGZ()
        io.expand(source, destination)

        assert len(os.listdir(destination)) > 1, logger.error("Expand didn't happen")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_expand_directory_fail(self, caplog, zipfiles) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = zipfiles
        destination = "tests/data/expand/multiple_files/"

        io = TarGZ()
        io.expand(source, destination, force=False)

        assert len(os.listdir(destination)) > 1, logger.error("Expand didn't happen")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
