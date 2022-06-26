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
# Modified   : Sunday June 26th 2022 07:38:11 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
from datetime import datetime
import logging
import logging.config

from deepctr.dal import STAGES
from deepctr.dal.base import File, Dataset
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
NAME = "test_dataset"
SOURCE = "avazu"
STORAGE_TYPE = "local"
FOLDER = "tests/data/data_store"
FORMAT = "csv"
STAGE_ID = 1
STAGE_NAME = STAGES.get(STAGE_ID)
COMPRESSED = False
HOME = "tests/data"
BUCKET = "deepctr"


@pytest.mark.dal
@pytest.mark.dataset
class TestDataset:
    def test_dataset(self, caplog, dataset):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        assert dataset.name == NAME
        assert dataset.source == SOURCE
        assert dataset.storage_type == STORAGE_TYPE
        assert dataset.folder == FOLDER
        assert dataset.format == FORMAT
        assert dataset.stage_id == STAGE_ID
        assert dataset.stage_name == STAGE_NAME
        assert dataset.compressed == COMPRESSED
        assert dataset.size != 0
        assert dataset.home == HOME
        assert dataset.bucket is None
        assert len(dataset.files) == 4

        assert isinstance(dataset.created, datetime)
        assert isinstance(dataset.modified, datetime)
        assert isinstance(dataset.accessed, datetime)

        size = 0
        for i, (_, file) in enumerate(dataset.files.items()):
            i += 1
            size += file.size
            filename = "csvfile{}.csv".format(i)
            assert isinstance(file, File)
            assert file.name == os.path.splitext(filename)[0]
            assert file.source == SOURCE
            assert file.format == FORMAT
            assert file.storage_type == STORAGE_TYPE
            assert file.stage_id == STAGE_ID
            assert file.stage_name == STAGE_NAME
            assert file.home == HOME
            assert file.bucket is None
            assert file.filepath == os.path.join(FOLDER, filename)
            assert file.compressed is False
            assert file.size != 0
            assert file.rows != 0
            assert file.cols != 0
            assert isinstance(file.created, datetime)
            assert isinstance(file.modified, datetime)
            assert isinstance(file.accessed, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


def test_dataset_invalid(self, caplog, dataset):
    logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    # Invalid source
    with pytest.raises(ValueError):
        Dataset(
            name=NAME,
            source="xada",
            storage_type=STORAGE_TYPE,
            stage_id=STAGE_ID,
            format=FORMAT,
            home=HOME,
        )

    # Invalid format
    with pytest.raises(ValueError):
        Dataset(
            name=NAME,
            source=SOURCE,
            storage_type=STORAGE_TYPE,
            stage_id=STAGE_ID,
            format="DKSSK",
            home=HOME,
        )

    # Invalid storage type
    with pytest.raises(ValueError):
        Dataset(
            name=NAME,
            source=SOURCE,
            storage_type="DSAA",
            stage_id=STAGE_ID,
            format=FORMAT,
            home=HOME,
        )
    # Invalid stage id
    with pytest.raises(ValueError):
        Dataset(
            name=NAME,
            source=SOURCE,
            storage_type=STORAGE_TYPE,
            stage_id=923,
            format=FORMAT,
            home=HOME,
        )

    logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
