#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_rao.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 07:03:22 pm                                                  #
# Modified   : Friday June 24th 2022 11:45:15 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
import logging.config

# import shutil


from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.rao import RAO

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                       TEST RAO                                                   #
# ================================================================================================ #


@pytest.mark.dal
@pytest.mark.rao
class TestRAOFile:
    def test_upload_file(self, caplog, local_source, s3file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        rao = RAO()
        rao.upload_file(source=local_source, destination=s3file, compress=False, force=True)
        assert rao.exists(s3file)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_download_file(self, caplog, local_destination, s3file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        rao = RAO()
        rao.download_file(source=s3file, destination=local_destination, expand=False, force=True)
        assert os.path.exists(local_destination.filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
