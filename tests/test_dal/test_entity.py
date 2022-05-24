#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_entity.py                                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 22nd 2022 01:40:01 am                                                    #
# Modified   : Monday May 23rd 2022 08:26:45 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging
import numpy as np
import logging.config

from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.entity import File, Dataset

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.entity
class TestFile:
    def test_local_file(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        file = File(
            name="test_file",
            dataset="test_dataset",
            datasource="criteo",
            stage="staged",
            format="csv",
            size=12345,
            home="test",
            dag_id=22,
            task_id=33,
        )

        assert file.name == "test_file", logging.error("Failed File Entity")
        assert file.dataset == "test_dataset", logging.error("Failed File Entity")
        assert file.datasource == "criteo", logging.error("Failed File Entity")
        assert file.stage == "staged", logging.error("Failed File Entity")
        assert file.format == "csv", logging.error("Failed File Entity")
        assert file.filename == "test_file.csv", logger.error("Incorrect filename")
        assert (
            file.filepath == "test/data/test_source/test_dataset/staged/test_file.csv"
        ), logger.error("Incorrect filepath")
        assert file.compressed is False, logging.error("Failed File Entity")
        assert file.bucket is None, logging.error("Failed File Entity")
        assert file.object_key is None, logging.error("Failed File Entity")
        assert file.home == "test/data", logging.error("Failed File Entity")
        assert file.state == "added", logging.error("Failed File Entity")
        assert file.size == 12345, logging.error("Failed File Entity")
        assert file.storage_type == "local", logging.error("Failed File Entity")
        assert file.dag_id == 22, logging.error("Failed File Entity")
        assert file.task_id == 33, logging.error("Failed File Entity")
        assert isinstance(file.created, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_file(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        file = File(
            name="test_file",
            dataset="test_dataset",
            datasource="criteo",
            stage="staged",
            compressed=True,
            bucket="deepctr",
            object_key="test/object.csv.tar.gz",
            format="tar.gz",
            size=12345,
            home="test",
            dag_id=22,
            task_id=33,
        )

        assert file.name == "test_file", logging.error("Failed File Entity")
        assert file.dataset == "test_dataset", logging.error("Failed File Entity")
        assert file.datasource == "criteo", logging.error("Failed File Entity")
        assert file.stage == "staged", logging.error("Failed File Entity")
        assert file.format == "csv", logging.error("Failed File Entity")
        assert file.filename == "test_object.csv.tar.gz", logger.error("Incorrect filename")
        assert file.compressed is True, logging.error("Failed File Entity")
        assert file.bucket == "deepctr", logging.error("Failed File Entity")
        assert file.object_key == "test/object", logging.error("Failed File Entity")
        assert file.home == "test/data", logging.error("Failed File Entity")
        assert file.state == "added", logging.error("Failed File Entity")
        assert file.size == 12345, logging.error("Failed File Entity")
        assert file.storage_type == "s3", logging.error("Failed File Entity")
        assert file.dag_id == 22, logging.error("Failed File Entity")
        assert file.task_id == 33, logging.error("Failed File Entity")
        assert isinstance(file.created, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_file_invalid(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        with pytest.raises(ValueError):

            file = File(
                name="test_file",
                dataset="test_dataset",
                datasource="criteo",
                stage="staged",
                compressed=True,
                bucket="deepctr",
                object_key="test/object.csv.tar.gz",
                format="xxx",
                size=12345,
                home="test",
                dag_id=22,
                task_id=33,
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


# ------------------------------------------------------------------------------------------------ #


class TestDataset:
    def test_local_dataset(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dataset = Dataset(
            name="test_dataset",
            datasource="criteo",
            stage="staged",
            folder="test/folder",  # Ignored for local datasets and files.
            size=12345,
            home="test/home",
            state="added",
            storage_type="local",
            dag_id=22,
        )

        assert dataset.name == "test_dataset", logging.error("Failed Dataset Entity")
        assert dataset.datasource == "criteo", logging.error("Failed Dataset Entity")
        assert dataset.stage == "staged", logging.error("Failed Dataset Entity")
        assert dataset.folder == "test/home/criteo/test_datasset/staged", logging.error(
            "Failed Dataset Entity"
        )
        assert dataset.bucket is None, logging.error("Failed Dataset Entity")
        assert dataset.object_key is None, logging.error("Failed Dataset Entity")
        assert dataset.home == "test/home", logging.error("Failed Dataset Entity")
        assert dataset.state == "added", logging.error("Failed Dataset Entity")
        assert dataset.size == 12345, logging.error("Failed Dataset Entity")
        assert dataset.storage_type == "local", logging.error("Failed Dataset Entity")
        assert dataset.dag_id == 22, logging.error("Failed Dataset Entity")
        assert isinstance(dataset.created, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_dataset(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dataset = Dataset(
            name="test_dataset",
            datasource="criteo",
            stage="staged",
            bucket="deepctr",
            folder="test/folder",  # Ignored for local datasets and files.
            size=12345,
            home="test/home",
            state="added",
            storage_type="local",
            dag_id=22,
        )

        assert dataset.name == "test_dataset", logging.error("Failed Dataset Entity")
        assert dataset.datasource == "criteo", logging.error("Failed Dataset Entity")
        assert dataset.stage == "staged", logging.error("Failed Dataset Entity")
        assert dataset.folder == "test/folder", logging.error("Failed Dataset Entity")
        assert dataset.bucket == "deepctr", logging.error("Failed Dataset Entity")
        assert dataset.object_key is None, logging.error("Failed Dataset Entity")
        assert dataset.home == "test/home", logging.error("Failed Dataset Entity")
        assert dataset.state == "added", logging.error("Failed Dataset Entity")
        assert dataset.size == 12345, logging.error("Failed Dataset Entity")
        assert dataset.storage_type == "s3", logging.error("Failed Dataset Entity")
        assert dataset.dag_id == 22, logging.error("Failed Dataset Entity")
        assert isinstance(dataset.created, datetime)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_dataset_invalid(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        with pytest.raises(ValueError):

            dataset = Dataset(
                name="test_dataset",
                dataset="test_dataset",
                datasource="criteo",
                stage="staged",
                compressed=True,
                bucket="deepctr",
                object_key="test/object.csv.tar.gz",
                format="xxx",
                size=12345,
                home="test",
                dag_id=22,
                task_id=33,
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

