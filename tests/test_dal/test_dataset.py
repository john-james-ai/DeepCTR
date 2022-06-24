#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_dataset.py                                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 07:03:22 pm                                                  #
# Modified   : Thursday June 23rd 2022 09:40:18 pm                                                 #
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


# @pytest.mark.dal
# @pytest.mark.dataset
# @pytest.mark.skip
# class TestDataset:
#     def test_dataset_valid(self, caplog):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         ds = Dataset(
#             name=DATASET_NAME,
#             source=DATASET_SOURCE,
#             stage_id=DATASET_STAGE_ID,
#             storage_type=DATASET_STORAGE_TYPE,
#         )
#         file = File(
#             name=FILE_NAME,
#             source=FILE_SOURCE,
#             dataset=FILE_DATASET,
#             stage_id=FILE_STAGE_ID,
#             format=FILE_FORMAT,
#             created=FILE_CREATED,
#             home=FILE_HOME,
#         )
#         ds.add_file(file)

#         assert ds.name == DATASET_NAME
#         assert ds.source == DATASET_SOURCE
#         assert ds.stage_id == DATASET_STAGE_ID
#         assert ds.storage_type == DATASET_STORAGE_TYPE
#         assert isinstance(ds.files, list)
#         assert isinstance(ds.files[0], File)
#         assert ds.files[0].name == FILE_NAME

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

