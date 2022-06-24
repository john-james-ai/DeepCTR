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
# Modified   : Thursday June 23rd 2022 09:39:16 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from bz2 import compress
import os
import inspect
from tarfile import CompressionError
import pytest
import logging
import logging.config

# import shutil
from datetime import datetime

from deepctr.dal import STAGES
from deepctr.dal.fao import File  # , Dataset, FAO
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                         TEST FAO                                                 #
# ================================================================================================ #


# @pytest.mark.dal
# @pytest.mark.fao
# @pytest.mark.skip
# class TestFAO:

#     __file = File(
#         name="test_create",
#         source=FILE_SOURCE,
#         dataset=FILE_DATASET,
#         stage_id=FILE_STAGE_ID,
#         format=FILE_FORMAT,
#         created=FILE_CREATED,
#         home=FILE_HOME,
#     )

#     def test_setup(self, caplog):
#         shutil.rmtree(TestFAO.__file.filepath, ignore_errors=True)

#     def test_create(self, caplog, spark_dataframe):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         fao = FAO()
#         fao.create(TestFAO.__file, spark_dataframe)
#         assert os.path.exists(TestFAO.__file.filepath)

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_read(self, caplog, spark_dataframe):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         fao = FAO()
#         data = fao.read(TestFAO.__file)
#         assert (data.exceptAll(spark_dataframe)).count() == 0

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_delete(self, caplog):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         fao = FAO()
#         fao.delete(TestFAO.__file)
#         assert not os.path.exists(TestFAO.__file.filepath)

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_exists(self, caplog):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
#         fao = FAO()
#         assert not fao.exists(TestFAO.__file)

#         file = File(
#             name=FILE_NAME,
#             source=FILE_SOURCE,
#             dataset=FILE_DATASET,
#             stage_id=FILE_STAGE_ID,
#             format=FILE_FORMAT,
#             created=FILE_CREATED,
#             home=FILE_HOME,
#         )
#         assert fao.exists(file)

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))