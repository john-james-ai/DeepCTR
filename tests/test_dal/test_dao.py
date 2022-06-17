#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_dao.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 07:03:22 pm                                                  #
# Modified   : Friday May 27th 2022 08:13:08 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pytest
import logging
import logging.config

from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.entity import LocalDataset
from deepctr.dal.dao import LocalDatasetDAO
from deepctr.dal.dto import DagDTO, TaskDTO

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dao
class TestLocalDatasetDAO:
    def test_create_dag(self, caplog, connection_dal):
        factory = Dag

    def test_create_local_dataset_from_dto(
        self, caplog, valid_local_dataset_dto, valid_local_dataset_result, connection_dal
    ):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        connection = connection_dal

        dao = LocalDatasetDAO(connection)
        dataset = dao.create(valid_local_dataset_dto)
        actual = dataset.to_dict()
        expected = valid_local_dataset_result
        assert isinstance(dataset, LocalDataset), logger.error("Not a valid dataset.")
        for k, v in actual.items():
            if k != "created":
                assert (
                    actual[k] == expected[k]
                ), "Error: actual {}: {} does not equal expected {}: {}".format(
                    str(k), str(actual[k]), str(k), str(expected[k])
                )
        print("\n# ", 76 * "=", " #")
        print(dataset)
        print("# ", 76 * "=", " #")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_add_local_dataset(self, caplog, connection_dal, valid_local_dataset_dto) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        connection = connection_dal
        dao = LocalDatasetDAO(connection)
        dataset = dao.create(valid_local_dataset_dto)
        dao.add(dataset)

        print("\n# ", 76 * "=", " #")
        print(dao.findall())
        print("# ", 76 * "=", " #")
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    # def test_local_dataset_exists(self, connection_dal, valid_local_dataset_dto):
    #     logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    #     connection = connection_dal
    #     dao = LocalDatasetDAO(connection)
    #     dataset = dao.create(valid_local_dataset_dto)

    #     print("\n# ", 76 * "=", " #")
    #     print(dao.exists(dataset.name, dataset.datasource, dataset.stage))
    #     print("# ", 76 * "=", " #")

    #     assert dao.exists(
    #         dataset.name, dataset.datasource, dataset.stage
    #     ), "Dataset exists or add failed"

    #     logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
