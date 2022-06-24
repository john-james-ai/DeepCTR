#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_fao.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 07:03:22 pm                                                  #
# Modified   : Friday June 24th 2022 01:04:52 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import os
import inspect
import pytest
import logging
import logging.config

from deepctr.dal.fao import FAO
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                         TEST FAO                                                 #
# ================================================================================================ #


@pytest.mark.dal
@pytest.mark.fao
class TestFAO:
    def test_create(self, caplog, fao_file, spark_dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        fao.create(fao_file, spark_dataframe)
        assert os.path.exists(fao_file.filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog, fao_file, spark_dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        data = fao.read(fao_file)
        assert (data.exceptAll(spark_dataframe)).count() == 0

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_exists(self, caplog, fao_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        assert fao.exists(fao_file)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete(self, caplog, fao_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        fao.delete(fao_file)
        assert not os.path.exists(fao_file.filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_not_exists(self, caplog, fao_file):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        assert not fao.exists(fao_file)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
