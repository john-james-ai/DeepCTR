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
# Modified   : Wednesday May 25th 2022 01:17:28 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Union
import logging
from pymysql.connectionss import Connection
from typing import Any

from deepctr.dal.base import DAO
from deepctr.dal.entity import LocalFile  # , LocalDataset, S3File, S3Dataset
from deepctr.dal.entity import LocalEntityFactory  # , S3EntityFactory
from deepctr.dal.dto import LocalFileDTO  # , LocalDatasetDTO, S3FileDTO, S3FDatasetDTO
from deepctr.dal.sequel import (
    LocalFileSelect,
    LocalFileInsert,
    LocalFileDelete,
    LocalFileSelectAll,
    LocalFileExists,
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
#                                      FILES                                                       #
# ------------------------------------------------------------------------------------------------ #


class LocalFileDAO(DAO):
    """Provides access to local file table."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._database = Database(connection)

    @property
    def connection(self) -> Connection:
        return self._connection

    @connection.setter
    def connection(self, connection: Connection) -> None:
        self._connection = connection
        self._database = Database(connection)

    def create(self, dto: LocalFileDTO) -> LocalFile:
        factory = LocalEntityFactory()
        return factory.create_file(dto)

    def add(self, file: LocalFile) -> None:
        """Sets state of LocalFile entity to 'added' and stores and adds its local store."""
        sequel = LocalFileInsert(file)
        self._database.execute(sequel.statement, sequel.parameters)
        logger.info("Inserted file named {} into the database.".format(file.filename))

    def find(self, column: str, value: Any) -> Union[LocalFile, None]:
        """Finds a LocalFile entity by the designated column and value"""
        sequel = LocalFileSelect(column=column, value=value)
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self, column: str, value: Any) -> list:
        """Returns all files in the database."""
        sequel = LocalFileSelectAll(column=column, value=value)
        return self._database.select_all(sequel.statement, sequel.parameters)

    def remove(self, column: str, value: Any) -> None:
        """Removes the file based upon the selection criteria"""
        sequel = LocalFileDelete(column=column, value=value)
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, file: LocalFile) -> bool:
        """Determines if a file exists"""
        sequel = LocalFileExists(file)
        return self._database.exists(sequel.statement, sequel.parameters)

    def save(self) -> None:
        self._database.save()

