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
# Modified   : Friday June 24th 2022 06:03:30 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pytest
import logging
import logging.config

from deepctr.dal import STAGES
from deepctr.dal.base import Dataset
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
NAME = "test_dataset"
SOURCE = "alibaba"
STORAGE_TYPE = "local"
FOLDER = "tests/data/test_dal/test_dataset/alibaba/anastasia/2_loaded/"
FORMAT = "csv"
STAGE_ID = 2
STAGE_NAME = STAGES.get(STAGE_ID)
COMPRESSED = False
HOME = "tests/data/test_dal/test_dataset/"
BUCKET = "deepctr"


@pytest.mark.dal
@pytest.mark.dataset
class TestDataset:
    def test_dataset_valid_local_file_first(self, caplog, spark_dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        ds = Dataset(
            name=DATASET_NAME,
            source=DATASET_SOURCE,
            stage_id=DATASET_STAGE_ID,
            storage_type=DATASET_STORAGE_TYPE,
        )
        file = File(
            name=FILE_NAME,
            source=FILE_SOURCE,
            dataset=FILE_DATASET,
            stage_id=FILE_STAGE_ID,
            format=FILE_FORMAT,
            created=FILE_CREATED,
            home=FILE_HOME,
        )
        ds.add_file(file)

        assert ds.name == DATASET_NAME
        assert ds.source == DATASET_SOURCE
        assert ds.stage_id == DATASET_STAGE_ID
        assert ds.storage_type == DATASET_STORAGE_TYPE
        assert isinstance(ds.files, list)
        assert isinstance(ds.files[0], File)
        assert ds.files[0].name == FILE_NAME

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

