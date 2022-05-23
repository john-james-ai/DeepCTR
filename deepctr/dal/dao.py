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
# Modified   : Monday May 23rd 2022 07:52:27 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
from typing import Union
import logging
from pyspark.sql import DataFrame
import pymysql
from pymysql.connections import Connection
from typing import Any

from deepctr.dal.base import DAO
from deepctr.dal.entity import Entity, File, Dataset
from deepctr.dal.dto import FileDTO, DatasetDTO
from deepctr.dal.sequel import FileSelect, FileInsert, FileDelete, FileSelectAll, FileExists
from deepctr.dal.sequel import (
    DatasetSelect,
    DatasetInsert,
    DatasetDelete,
    DatasetSelectAll,
    DatasetExists,
)
from deepctr.data.database import Database
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                      FILES                                                       #
# ------------------------------------------------------------------------------------------------ #


class Files(DAO):
    """Collection of in-memory File entities."""

    def __init__(self, connection: pymysql.connector.Connection) -> None:
        self._database = Database(connection)

    @property
    def connection(self) -> Connection:
        return self._connection

    @connection.setter
    def connection(self, connection: Connection) -> None:
        self._connection = connection

    def create(self, dto: FileDTO) -> File:
        return File(
            name=dto.name,
            dataset=dto.dataset,
            dataset_id=dto.dataset_id,
            datasource=dto.datasource,
            stage=dto.stage,
            storage_type=dto.storage_type,
            format=dto.format,
            compressed=dto.compressed,
            bucket=dto.bucket,
            object_key=dto.object_key,
            task_id=dto.task_id,
            created=datetime.now(),
            home=dto.home,
        )

    def add(self, file: File) -> None:
        """Sets state of File entity to 'added' and stores and adds its local store."""
        sequel = FileInsert(file)
        self._database.execute(sequel.statement, sequel.parameters)
        logger.info("Inserted file named {} into the database.".format(file.filename))

    def find(self, column: str, value: Any) -> Union[Entity, None]:
        """Finds a File entity by the designated column and value"""
        sequel = FileSelect(column=column, value=value)
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self, column: str, value: Any) -> DataFrame:
        """Returns all files in the database."""
        sequel = FileSelectAll(column=column, value=value)
        return self._database.select_all(sequel.statement, sequel.parameters)

    def remove(self, column: str, value: Any) -> None:
        """Removes the file based upon the selection criteria"""
        sequel = FileDelete(column=column, value=value)
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, column: str, value: Any) -> int:
        """Determines if a file exists based upon criteria"""
        sequel = FileExists(column=column, value=value)
        return self._database.exists(sequel.statement, sequel.parameters)


# ------------------------------------------------------------------------------------------------ #
#                                   DATASETS                                                       #
# ------------------------------------------------------------------------------------------------ #


class Datasets(DAO):
    """Collection of in-memory Dataset entities."""

    def __init__(self, connection: pymysql.connector.Connection) -> None:
        self._database = Database(connection)

    @property
    def connection(self) -> Connection:
        return self._connection

    @connection.setter
    def connection(self, connection: Connection) -> None:
        self._connection = connection

    def create(self, dto: DatasetDTO) -> Dataset:
        return Dataset(
            name=dto.name,
            stage=dto.stage,
            datasource=dto.datasource,
            storage_type=dto.storage_type,
            folder=dto.folder,
            bucket=dto.bucket,
            size=dto.size,
            dag_id=dto.dag_id,
            created=datetime.now(),
            home=dto.home,
        )

    def add(self, dataset: Dataset) -> None:
        """Sets state of Dataset entity to 'added' and stores and adds its local store."""
        sequel = DatasetInsert(dataset)
        self._database.execute(sequel.statement, sequel.parameters)
        logger.info("Inserted dataset named {} into the database.".format(dataset.datasetname))

    def find(self, column: str, value: Any) -> Union[Entity, None]:
        """Finds a Dataset entity by the designated column and value"""
        sequel = DatasetSelect(column=column, value=value)
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self, column: str, value: Any) -> DataFrame:
        """Returns all datasets in the database."""
        sequel = DatasetSelectAll(column=column, value=value)
        return self._database.select_all(sequel.statement, sequel.parameters)

    def remove(self, column: str, value: Any) -> None:
        """Removes the dataset based upon the selection criteria"""
        sequel = DatasetDelete(column=column, value=value)
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, column: str, value: Any) -> int:
        """Determines if a dataset exists based upon criteria"""
        sequel = DatasetExists(column=column, value=value)
        return self._database.exists(sequel.statement, sequel.parameters)
