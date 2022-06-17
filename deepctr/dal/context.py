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
# Modified   : Saturday May 28th 2022 06:31:15 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset context object that implements the Repository/Unit of Work Pattern."""
from abc import ABC, abstractmethod
import logging
from deepctr.utils.log_config import LOG_CONFIG
from pymysql.connections import Connection
from deepctr.dal.entity import DagORM, Dataset, TaskORM
from deepctr.dal.dao import LocalDatasetDAO, LocalFileDAO, S3DatasetDAO, S3FileDAO, DagDAO, TaskDAO
from deepctr.dal.base import DAO

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Context(ABC):
    """Base class defining the interface for all context objects.

    Context controls the database connection and transactions, as well as the
    data access objects (DAOs), one for each table. Microsoft EF Framework calls these dbSets. subclasses
    assign the specific DAOs required for the class and context.

    Contexts are created as global variables and instantiated by the client. They are then passed
    to the DagBuilder through the 'and_context' method. It can then be obtained by
    calling the globals() function and indexing on the 'context' member.

    Usage:
    ------
    connection = ConnectionFactory(name='deepctr')
    globals context
    context = DataContext(connection)

    """

    def __init__(self, connection: Connection) -> None:
        self._name = self.__class__.__name__.lower()
        self._connection = connection
        self.connect()
        self._entities = {}
        logger.info("Instantiated {}".format(self._name))

    def __enter__(self):
        self._connection.begin()
        return self

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        if exception_type is None:
            self.commit()
            self.close()
        else:
            logger.error(
                "Exception type: {}\tException value: {}\n{}".format(
                    exception_type, exception_value, traceback
                )
            )
            self.rollback()
            self.close()
            raise

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, connection: Connection):
        self._connection = connection

    @property
    def dag(self) -> DagORM:
        return self._dag

    @dag.setter
    def dag(self, dag: DagORM) -> None:
        self._dag = dag

    @property
    def task(self) -> TaskORM:
        return self._task

    @task.setter
    def task(self, task: TaskORM) -> None:
        self._task = task

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @dataset.setter
    def dataset(self, dataset: Dataset) -> None:
        self._dataset = dataset

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def rollback(self) -> None:
        self._connection.rollback()

    @abstractmethod
    def connect(self) -> None:
        """Adds connections to all the data access objects."""
        pass

    @abstractmethod
    def save(self) -> None:
        """Runs save to commit changes on all data access objects."""
        pass


# ------------------------------------------------------------------------------------------------ #
class DataContext(Context):
    """Defines the context for data related DAGs"""

    def __init__(self, connection: Connection) -> None:
        super(DataContext, self).__init__(connection)

    @property
    def datasets(self) -> DAO:
        return self._datasets

    @datasets.setter
    def datasets(self, datasets):
        self._datasets = datasets

    @property
    def files(self) -> DAO:
        return self._files

    @property
    def localfiles(self) -> DAO:
        return self._localfiles

    @property
    def localdatasets(self) -> DAO:
        return self._localdatasets

    @property
    def s3files(self) -> DAO:
        return self._s3files

    @property
    def s3datasets(self) -> DAO:
        return self._s3datasets

    @property
    def dags(self) -> DAO:
        return self._dags

    @property
    def tasks(self) -> DAO:
        return self._tasks

    def connect(self) -> None:
        self._localfiles = LocalFileDAO(self._connection)
        self._localdatasets = LocalDatasetDAO(self._connection)
        self._s3files = S3FileDAO(self._connection)
        self._s3datasets = S3DatasetDAO(self._connection)
        self._dags = DagDAO(self._connection)
        self._tasks = TaskDAO(self._connection)

    def save(self) -> None:
        self._localfiles.save()
        self._localdatasets.save()
        self._s3files.save()
        self._s3datasets.save()
        self._dags.save()
        self._tasks.save()
