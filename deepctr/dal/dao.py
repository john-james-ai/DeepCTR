#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /dao.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 21st 2022 11:10:43 pm                                                  #
# Modified   : Thursday May 26th 2022 10:38:33 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Union
import logging
from pymysql.connections import Connection
from typing import Any

from deepctr.dal.base import DAO
from deepctr.dal.entity import LocalFile, LocalDataset  # , S3File, S3Dataset
from deepctr.dal.entity import LocalEntityFactory  # , S3EntityFactory
from deepctr.dal.dto import DTO
from deepctr.dal.sequel import (
    LocalFileInsert,
    LocalFileSelectOne,
    LocalFileSelectByColumn,
    LocalFileSelectByKey,
    LocalFileSelectAll,
    LocalFileDelete,
    LocalFileExists,
)

from deepctr.dal.sequel import (
    LocalDatasetInsert,
    LocalDatasetSelectOne,
    LocalDatasetSelectByColumn,
    LocalDatasetSelectByKey,
    LocalDatasetSelectAll,
    LocalDatasetDelete,
    LocalDatasetExists,
)


# from deepctr.dal.sequel import (
#     S3FileSelect,
#     S3FileInsert,
#     S3FileDelete,
#     S3FileSelectAll,
#     S3FileExists,
# )

# from deepctr.dal.sequel import (
#     LocalDatasetSelect,
#     LocalDatasetInsert,
#     LocalDatasetDelete,
#     LocalDatasetSelectAll,
#     LocalDatasetExists,
# )
# from deepctr.dal.sequel import (
#     S3DatasetSelect,
#     S3DatasetInsert,
#     S3DatasetDelete,
#     S3DatasetSelectAll,
#     S3DatasetExists,
# )
from deepctr.data.database import Database
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                       LOCAL DATASET DAO                                          #
# ------------------------------------------------------------------------------------------------ #


class LocalDatasetDAO(DAO):
    """Provides access to local dataset ."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._database = Database(connection)

    def create(self, data: Union[dict, DTO]) -> LocalDataset:
        factory = LocalEntityFactory()
        return factory.create_dataset(data)

    def add(self, dataset: LocalDataset) -> None:
        """Sets state of LocalDataset entity to 'added' and stores and adds its local store."""
        sequel = LocalDatasetInsert(dataset)
        self._database.execute(sequel.statement, sequel.parameters)
        logger.info("Inserted dataset named {} into the database.".format(dataset.name))

    def find(self, id: int) -> Union[LocalDataset, None]:
        """Finds a LocalDataset entity by the designated column and value"""
        sequel = LocalDatasetSelectOne(parameters=(id))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_key(self, name: str, datasource: str, stage: str) -> Union[LocalDataset, None]:
        """Finds a LocalDataset entity by the designated column and value"""
        sequel = LocalDatasetSelectByKey(name=name, datasource=datasource, stage=stage)
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_column(self, column: str, value: Any) -> Union[LocalDataset, list]:
        """Finds a LocalDataset entity by the designated column and value"""
        sequel = LocalDatasetSelectByColumn(column=column, value=value)
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self) -> list:
        """Returns all datasets in the database."""
        sequel = LocalDatasetSelectAll()
        return self._database.select_all(sequel.statement)

    def remove(self, id: int) -> None:
        """Removes the dataset based upon the selection criteria"""
        sequel = LocalDatasetDelete(id=id)
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, name: str, datasource: str, stage: str) -> bool:
        """Determines if a dataset exists"""
        sequel = LocalDatasetExists(name=name, datasource=datasource, stage=stage)
        return self._database.exists(sequel.statement, sequel.parameters)

    def get_dataset_id(self, name: str, datasource: str, stage: str) -> Union[LocalDataset, None]:
        row = self.find_by_key(name, datasource, stage)
        try:
            return row["id"]
        except KeyError as e:
            message = "No Dataset Found for name: {}\tdatasource: {}\t: stage: {}\t{}".format(
                name, datasource, stage, e
            )
            logger.error(message)
            raise ValueError(message)

    def begin_transaction(self) -> None:
        self._database.begin_transaction()

    def rollback(self) -> None:
        self._database.rollback()

    def save(self) -> None:
        self._database.save()


# ------------------------------------------------------------------------------------------------ #
#                                         LOCAL FILE DAO                                           #
# ------------------------------------------------------------------------------------------------ #
class LocalFileDAO(DAO):
    """Provides access to local file table."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._database = Database(connection)

    def create(self, data: Union[dict, DTO]) -> LocalFile:
        factory = LocalEntityFactory()
        return factory.create_file(data)

    def add(self, file: LocalFile) -> None:
        """Sets state of LocalFile entity to 'added' and stores and adds its local store."""
        sequel = LocalFileInsert(file)
        self._database.execute(sequel.statement, sequel.parameters)
        logger.info("Inserted file named {} into the database.".format(file.filename))

    def find(self, id: int) -> Union[LocalFile, None]:
        """Finds a LocalFile entity by the designated column and value"""
        sequel = LocalFileSelectOne(parameters=(id))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_key(
        self, name: str, dataset: str, datasource: str, stage: str
    ) -> Union[LocalFile, None]:
        """Finds a LocalFile entity by the designated column and value"""
        sequel = LocalFileSelectByKey(
            name=name, dataset=dataset, datasource=datasource, stage=stage
        )
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_column(self, column: str, value: Any) -> Union[LocalFile, list]:
        """Finds a LocalFile entity by the designated column and value"""
        sequel = LocalFileSelectByColumn(column=column, value=value)
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self) -> list:
        """Returns all files in the database."""
        sequel = LocalFileSelectAll()
        return self._database.select_all(sequel.statement)

    def remove(self, id: int) -> None:
        """Removes the file based upon the selection criteria"""
        sequel = LocalFileDelete(id=id)
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, name: str, dataset: str, datasource: str, stage: str) -> bool:
        """Determines if a file exists"""
        sequel = LocalFileExists(name=name, dataset=dataset, datasource=datasource, stage=stage)
        return self._database.exists(sequel.statement, sequel.parameters)

    def begin_transaction(self) -> None:
        self._database.begin_transaction()

    def rollback(self) -> None:
        self._database.rollback()

    def save(self) -> None:
        self._database.save()
