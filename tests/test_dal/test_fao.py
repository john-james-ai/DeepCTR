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
# Modified   : Wednesday June 22nd 2022 11:06:33 am                                                #
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
from datetime import datetime

from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.fao import LocalFile, Dataset, S3File, FAO

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                      TEST DAG DAO                                                #
# ================================================================================================ #
FILE_NAME = "test_fao_input"
FILE_SOURCE = "alibaba"
FILE_DATASET = "test_dataset"
FILE_STAGE_ID = 2
FILE_FORMAT = ".csv"
FILE_COMPRESSED = True
FILE_ID = 3
FILE_CREATED = datetime.now()
FILE_HOME = "tests/data/test_dal/test_fao"
FILE_FILEPATH = os.path.join(FILE_HOME, FILE_SOURCE, FILE_DATASET, "2_loaded", FILE_NAME + ".csv")
FILE_BUCKET = "deepctr"
FILE_OBJECT_KEY = "alibaba/vesuvio/ad_feature.csv.tar.gz"

DATASET_NAME = "vesuvio"
DATASET_SOURCE = "alibaba"
DATASET_STAGE_ID = 1
DATASET_STORAGE_TYPE = "s3"


@pytest.mark.dal
@pytest.mark.file
class TestFile:
    def test_localfile_valid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Test w/ minimal input
        file = LocalFile(
            name=FILE_NAME,
            source=FILE_SOURCE,
            dataset=FILE_DATASET,
            stage_id=FILE_STAGE_ID,
            format=FILE_FORMAT,
            created=FILE_CREATED,
            home=FILE_HOME,
        )
        assert file.name == FILE_NAME
        assert file.source == FILE_SOURCE
        assert file.dataset == FILE_DATASET
        assert file.stage_id == FILE_STAGE_ID
        assert file.format == FILE_FORMAT.replace(".", "")
        assert file.filepath == FILE_FILEPATH
        assert file.compressed is False
        assert file.size != 0
        assert file.id == 0
        assert file.home == FILE_HOME
        assert file.created == FILE_CREATED

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_localfile_invalid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        # Invalid source
        with pytest.raises(ValueError):
            LocalFile(
                name=FILE_NAME,
                source="xada",
                dataset=FILE_DATASET,
                stage_id=FILE_STAGE_ID,
                format=FILE_FORMAT,
                created=FILE_CREATED,
                home=FILE_HOME,
            )

        # Invalid stage
        with pytest.raises(ValueError):
            LocalFile(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                stage_id=99,
                format=FILE_FORMAT,
                created=FILE_CREATED,
                home=FILE_HOME,
            )

        # Invalid format
        with pytest.raises(ValueError):
            LocalFile(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                stage_id=FILE_STAGE_ID,
                format="DS",
                created=FILE_CREATED,
                home=FILE_HOME,
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3file_valid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Test w/ minimal input
        file = S3File(
            name=FILE_NAME,
            source=FILE_SOURCE,
            dataset=FILE_DATASET,
            stage_id=FILE_STAGE_ID,
            format=FILE_FORMAT,
            bucket=FILE_BUCKET,
            object_key=FILE_OBJECT_KEY,
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
            S3File(
                name=FILE_NAME,
                source="xada",
                dataset=FILE_DATASET,
                stage_id=FILE_STAGE_ID,
                bucket=FILE_BUCKET,
                object_key=FILE_OBJECT_KEY,
                format=FILE_FORMAT,
                created=FILE_CREATED,
            )

        # Invalid stage
        with pytest.raises(ValueError):
            S3File(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                stage_id=99,
                bucket=FILE_BUCKET,
                object_key=FILE_OBJECT_KEY,
                format=FILE_FORMAT,
                created=FILE_CREATED,
            )

        # Invalid format
        with pytest.raises(ValueError):
            S3File(
                name=FILE_NAME,
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                bucket=FILE_BUCKET,
                object_key=FILE_OBJECT_KEY,
                stage_id=FILE_STAGE_ID,
                format="DS",
                created=FILE_CREATED,
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.dal
@pytest.mark.file
class TestDataset:
    def test_dataset_valid(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        ds = Dataset(
            name=DATASET_NAME,
            source=DATASET_SOURCE,
            stage_id=DATASET_STAGE_ID,
            storage_type=DATASET_STORAGE_TYPE,
        )
        file = LocalFile(
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
        assert isinstance(ds.files[0], LocalFile)
        assert ds.files[0].name == FILE_NAME

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.dal
@pytest.mark.fao
class TestFAO:

    __file = LocalFile(
        name="test_create",
        source=FILE_SOURCE,
        dataset=FILE_DATASET,
        stage_id=FILE_STAGE_ID,
        format=FILE_FORMAT,
        created=FILE_CREATED,
        home=FILE_HOME,
    )

    def test_setup(self, caplog):
        shutil.rmtree(TestFAO.__file.filepath, ignore_errors=True)

    def test_create(self, caplog, spark_dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        fao.create(TestFAO.__file, spark_dataframe)
        assert os.path.exists(TestFAO.__file.filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_read(self, caplog, spark_dataframe):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        data = fao.read(TestFAO.__file)
        assert (data.exceptAll(spark_dataframe)).count() == 0

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_delete(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        fao = FAO()
        fao.delete(TestFAO.__file)
        assert not os.path.exists(TestFAO.__file.filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_exists(self, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        fao = FAO()
        assert not fao.exists(TestFAO.__file)

        file = LocalFile(
            name=FILE_NAME,
            source=FILE_SOURCE,
            dataset=FILE_DATASET,
            stage_id=FILE_STAGE_ID,
            format=FILE_FORMAT,
            created=FILE_CREATED,
            home=FILE_HOME,
        )
        assert fao.exists(file)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
