#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_file.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 07:03:22 pm                                                  #
# Modified   : Thursday June 23rd 2022 09:41:06 pm                                                 #
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
from datetime import datetime

from deepctr.dal import STAGES
from deepctr.dal.base import File
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                         TEST DAO                                                 #
# ================================================================================================ #
FILE_NAME = "test_fao_input"
FILE_SOURCE = "alibaba"
FILE_DATASET = "test_dataset"
FILE_STORAGE_TYPE = "local"
FILE_FORMAT = ".csv"
FILE_STAGE_ID = 2
FILE_STAGE_NAME = STAGES.get(FILE_STAGE_ID)
FILE_HOME = "tests/data/test_dal/test_fao"
FILE_BUCKET = "deepctr"
FILE_FILEPATH = os.path.join(FILE_HOME, FILE_SOURCE, FILE_DATASET, "2_loaded", FILE_NAME + ".csv")
FILE_COMPRESSED = True
FILE_SIZE = 0
FILE_ID = 3
FILE_CREATED = datetime.now()
FILE_OBJECT_KEY = "alibaba/vesuvio/ad_feature.csv.tar.gz"

DATASET_NAME = "vesuvio"
DATASET_SOURCE = "alibaba"
DATASET_STAGE_ID = 1
DATASET_STORAGE_TYPE = "s3"


@pytest.mark.dal
@pytest.mark.file
class TestFile:
    def test_local_file_valid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Test w/ minimal input
        file = File(
            name=FILE_NAME,
            source=FILE_SOURCE,
            dataset=FILE_DATASET,
            storage_type=FILE_STORAGE_TYPE,
            format=FILE_FORMAT,
            stage_id=FILE_STAGE_ID,
            home=FILE_HOME,
        )
        assert file.name == FILE_NAME
        assert file.source == FILE_SOURCE
        assert file.dataset == FILE_DATASET
        assert file.storage_type == FILE_STORAGE_TYPE
        assert file.format == FILE_FORMAT.replace(".", "")
        assert file.stage_id == FILE_STAGE_ID
        assert file.stage_name == FILE_STAGE_NAME
        assert file.home == FILE_HOME
        assert file.filepath == FILE_FILEPATH
        assert file.compressed is False
        assert file.size != 0
        assert file.id == 0
        assert isinstance(file.created, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_local_file_invalid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        # Invalid source
        with pytest.raises(ValueError):
            File(
                name=FILE_NAME,
                source="xada",
                dataset=FILE_DATASET,
                storage_type=FILE_STORAGE_TYPE,
                stage_id=FILE_STAGE_ID,
                format=FILE_FORMAT,
                created=FILE_CREATED,
                home=FILE_HOME,
            )

        # Invalid stage
        with pytest.raises(ValueError):
            File(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                storage_type=FILE_STORAGE_TYPE,
                stage_id=99,
                format=FILE_FORMAT,
                created=FILE_CREATED,
                home=FILE_HOME,
            )

        # Invalid format
        with pytest.raises(ValueError):
            File(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                storage_type=FILE_STORAGE_TYPE,
                stage_id=FILE_STAGE_ID,
                format="DS",
                created=FILE_CREATED,
                home=FILE_HOME,
            )

        # Invalid storage_type
        with pytest.raises(ValueError):
            File(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                storage_type="xkxxk",
                stage_id=FILE_STAGE_ID,
                format=FILE_FORMAT,
                created=FILE_CREATED,
                home=FILE_HOME,
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3file_valid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Test w/ minimal input
        file = File(
            name=FILE_NAME,
            source=FILE_SOURCE,
            dataset=FILE_DATASET,
            storage_type="s3",
            stage_id=FILE_STAGE_ID,
            format=FILE_FORMAT,
            bucket=FILE_BUCKET,
            filepath=FILE_OBJECT_KEY,
            compressed=True,
            created=FILE_CREATED,
        )
        assert file.name == FILE_NAME
        assert file.source == FILE_SOURCE
        assert file.dataset == FILE_DATASET
        assert file.stage_id == FILE_STAGE_ID
        assert file.format == FILE_FORMAT.replace(".", "")
        assert file.compressed is True
        assert file.size != 0
        assert file.id == 0
        assert file.created == FILE_CREATED

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3file_invalid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        # Invalid source
        with pytest.raises(ValueError):
            File(
                name=FILE_NAME,
                source="xada",
                dataset=FILE_DATASET,
                storage_type="s3",
                stage_id=FILE_STAGE_ID,
                bucket=FILE_BUCKET,
                filepath=FILE_OBJECT_KEY,
                format=FILE_FORMAT,
                created=FILE_CREATED,
            )

        # Invalid stage
        with pytest.raises(ValueError):
            File(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                storage_type="s3",
                stage_id=99,
                bucket=FILE_BUCKET,
                filepath=FILE_OBJECT_KEY,
                format=FILE_FORMAT,
                created=FILE_CREATED,
            )

        # Invalid format
        with pytest.raises(ValueError):
            File(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                storage_type="s3",
                bucket=FILE_BUCKET,
                filepath=FILE_OBJECT_KEY,
                stage_id=FILE_STAGE_ID,
                format="DS",
                created=FILE_CREATED,
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))