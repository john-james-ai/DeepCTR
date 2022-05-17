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
import shutil


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
    def test_setup(self):
        filepath = "tests/data/aws2"
        shutil.rmtree(filepath, ignore_errors=True)

        filepath = "tests/data/aws3"
        shutil.rmtree(filepath, ignore_errors=True)

    def test_compress(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = TarGZ()
        for i in range(4):
            source = "tests/data/aws/"
            destination = (
                os.path.join("tests", "data", "aws2", "archive") + "_" + str(i) + ".tar.gz"
            )
            io.compress(source, destination)

        assert os.path.exists(destination), logger.error("File not compressed.")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_expand(self, caplog) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = "tests/data/aws2/"
        destination = "tests/data/aws3/"

        io = TarGZ()
        io.expand(source, destination)
        assert len(os.listdir(destination)) == 2, logger.error("Expand didn't happen")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
