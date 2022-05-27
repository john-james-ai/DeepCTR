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
# Modified   : Thursday May 26th 2022 03:20:13 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging
import logging.config

from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.entity import LocalEntityFactory, S3EntityFactory

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.skip()
class TestFile:
    def test_local_file(self, caplog, valid_local_file_dto, valid_local_file_result) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = LocalEntityFactory()
        file = factory.create_file(valid_local_file_dto)
        actual = file.to_dict()
        expected = valid_local_file_result

        for k, v in actual.items():
            if k == "created":
                assert isinstance(actual[k], datetime)
            else:
                assert actual[k] == expected[k], logger.error(
                    "Error. Actual {}: {} doesn't equal expected {}: {}".format(
                        k, str(actual[k]), k, str(expected[k])
                    )
                )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_file(self, caplog, valid_s3_file_dto, valid_s3_file_result) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = S3EntityFactory()
        file = factory.create_file(valid_s3_file_dto)
        actual = file.to_dict()
        expected = valid_s3_file_result

        for k, v in actual.items():
            if k == "created":
                assert isinstance(actual[k], datetime)
            else:
                assert actual[k] == expected[k], logger.error(
                    "Error. Actual {}: {} doesn't equal expected {}: {}".format(
                        k, str(actual[k]), k, str(expected[k])
                    )
                )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_invalid_local_file(self, caplog, invalid_local_file_dto) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = LocalEntityFactory()
        with pytest.raises(ValueError):
            factory.create_file(invalid_local_file_dto)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_invalid_s3_file(self, caplog, invalid_s3_file_dto) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = S3EntityFactory()
        with pytest.raises(ValueError):
            factory.create_file(invalid_s3_file_dto)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


@pytest.mark.entity
class TestDataset:
    def test_local_dataset(
        self, caplog, valid_local_dataset_dto, valid_local_dataset_result
    ) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = LocalEntityFactory()
        dataset = factory.create_dataset(valid_local_dataset_dto)
        actual = dataset.to_dict()
        expected = valid_local_dataset_result

        for k, v in actual.items():
            if k == "created":
                assert isinstance(actual[k], datetime)
            else:
                assert actual[k] == expected[k], logger.error(
                    "Error. Actual {}: {} doesn't equal expected {}: {}".format(
                        k, str(actual[k]), k, str(expected[k])
                    )
                )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_s3_dataset(self, caplog, valid_s3_dataset_dto, valid_s3_dataset_result) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = S3EntityFactory()
        dataset = factory.create_dataset(valid_s3_dataset_dto)
        actual = dataset.to_dict()
        expected = valid_s3_dataset_result

        for k, v in actual.items():
            if k == "created":
                assert isinstance(actual[k], datetime)
            else:
                assert actual[k] == expected[k], logger.error(
                    "Error. Actual {}: {} doesn't equal expected {}: {}".format(
                        k, str(actual[k]), k, str(expected[k])
                    )
                )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_invalid_local_dataset(self, caplog, invalid_local_dataset_dto) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = LocalEntityFactory()
        with pytest.raises(ValueError):
            factory.create_dataset(invalid_local_dataset_dto)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_invalid_s3_dataset(self, caplog, invalid_s3_dataset_dto) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        factory = S3EntityFactory()
        with pytest.raises(ValueError):
            factory.create_dataset(invalid_s3_dataset_dto)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
