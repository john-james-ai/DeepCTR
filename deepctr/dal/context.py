#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /context.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 22nd 2022 12:30:45 am                                                    #
# Modified   : Tuesday June 28th 2022 12:42:45 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset context object that implements the Repository/Unit of Work Pattern."""
from abc import ABC
import logging

from deepctr.dal.base import EntityMapper
from deepctr.data.database import Database
from deepctr.dal.source import SourceMapper
from deepctr.dal.file import FileMapper

# from deepctr.dal.mapper import FileMapper, DatasetMapper, TaskMapper, DagMapper
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                       DBCONTEXT                                                  #
# ------------------------------------------------------------------------------------------------ #
class DBContext(ABC):
    """Base class for database context classes.

    Context controls the database connection and transactions, as well as the
    mappers.

    Args:
    connection (pymysql.connections.Connection): Connection to the database
    database (Database): Object that reads and writes to the database
    mapper (EntityMapper): Contains SQL and factor reconstitute method for the entity.

    """

    def __init__(self, database: Database) -> None:
        self._database = database
        self._mapper = None

    @property
    def mapper(self) -> EntityMapper:
        return self._mapper

    @property
    def database(self) -> Database:
        return self._database

    def begin_transaction(self) -> None:
        self._database.begin_transaction()

    def commit(self) -> None:
        self._database.commit()

    def close(self) -> None:
        self._database.close()

    def rollback(self) -> None:
        self._database.rollback()


# ------------------------------------------------------------------------------------------------ #
#                                  SOURCE DBCONTEXT                                                #
# ------------------------------------------------------------------------------------------------ #
class SourceDBContext(DBContext):
    def __init__(self, database: Database) -> None:
        super(SourceDBContext, self).__init__(database=database)
        self._mapper = SourceMapper()


# ------------------------------------------------------------------------------------------------ #
#                                    DBCONTEXT FILE                                                #
# ------------------------------------------------------------------------------------------------ #
class FileDBContext(DBContext):
    def __init__(self, database: Database) -> None:
        super(FileDBContext, self).__init__(database=database)
        self._mapper = FileMapper()


# # ------------------------------------------------------------------------------------------------ #
# #                                  DBCONTEXT DATASET                                               #
# # ------------------------------------------------------------------------------------------------ #
# class DatasetDBContext(DBContext):
#     def __init__(self, database: Database) -> None:
#         super(DatasetDBContext, self).__init__(database=database)
#         self._mapper = DatasetMapper()


# # ------------------------------------------------------------------------------------------------ #
# #                                   DBCONTEXT DAG                                                  #
# # ------------------------------------------------------------------------------------------------ #
# class DagDBContext(DBContext):
#     def __init__(self, database: Database) -> None:
#         super(DagDBContext, self).__init__(database=database)
#         self._mapper = DagMapper()


# # ------------------------------------------------------------------------------------------------ #
# #                                   DBCONTEXT DAG                                                  #
# # ------------------------------------------------------------------------------------------------ #
# class TaskDBContext(DBContext):
#     def __init__(self, database: Database) -> None:
#         super(TaskDBContext, self).__init__(database=database)
#         self._mapper = TaskMapper()
