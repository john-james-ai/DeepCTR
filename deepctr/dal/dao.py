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
# Modified   : Saturday June 18th 2022 02:47:49 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Union
from datetime import datetime
import logging
from typing import Any
from pymysql.connections import Connection

from deepctr.dal.base import DAO
from deepctr.utils.decorators import tracer

from deepctr.dal.dto import DTO
from deepctr.dal.sequel import (
    DagInsert,
    DagSelectOne,
    DagSelectByColumn,
    DagSelectAll,
    DagDelete,
    DagExists,
    DagStart,
    DagStop,
)
from deepctr.dal.sequel import (
    TaskInsert,
    TaskSelectOne,
    TaskSelectByColumn,
    TaskSelectByKey,
    TaskSelectAll,
    TaskDelete,
    TaskExists,
    TaskStart,
    TaskStop,
)


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
    S3FileInsert,
    S3FileSelectOne,
    S3FileSelectByColumn,
    S3FileSelectByKey,
    S3FileSelectAll,
    S3FileDelete,
    S3FileExists,
)


from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                           DAG                                                    #
# ------------------------------------------------------------------------------------------------ #


class DagDAO(DAO):
    """Provides access to dag table ."""

    def __init__(self, connection: Connection) -> None:
        super(DagDAO, self).__init__(connection)

    def create(self, data: Union[dict, DTO]) -> Dag:
        factory = DagFactory()
        return factory.create_dag(data)

    def add(self, dag: DagORM) -> None:
        """Sets state of LocalDataset entity to 'added' and stores and adds its local store."""
        sequel = DagInsert(dag)
        id = self._database.insert(sequel.statement, sequel.parameters)
        dag.id = id
        logger.info("Executed command: {} with id: {}".format(sequel.command, str(id)))
        return dag

    def find(self, id: int) -> Union[DagORM, None]:
        """Finds a LocalDataset entity by the designated column and value"""
        sequel = DagSelectOne(parameters=(id))
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_column(self, column: str, value: Any) -> Union[DagORM, list]:
        """Finds a Dag entity by the designated column and value"""
        sequel = DagSelectByColumn(column=column, value=value)
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self) -> list:
        """Returns all datasets in the database."""
        sequel = DagSelectAll()
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select_all(sequel.statement)

    def remove(self, id: int) -> None:
        """Removes the dataset based upon the selection criteria"""
        sequel = DagDelete(id=id)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, id: int) -> bool:
        """Determines if a dataset exists"""
        sequel = DagExists(id=id)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.exists(sequel.statement, sequel.parameters)

    def start(self, dag: DagORM) -> None:
        dag.start = datetime.now()
        sequel = DagStart(dag=dag)
        logger.info("Executed command: {}".format(sequel.command))
        self._database.execute(sequel.statement, sequel.parameters)
        return dag

    def stop(self, dag: DagORM) -> None:
        dag.stop = datetime.now()
        dag.duration = (dag.stop - dag.start).total_seconds()
        sequel = DagStop(dag=dag)
        logger.info("Executed command: {}".format(sequel.command))
        self._database.execute(sequel.statement, sequel.parameters)
        return dag

    def begin_transaction(self) -> None:
        self._database.begin_transaction()

    def rollback(self) -> None:
        self._database.rollback()

    def save(self) -> None:
        self._database.save()


# ------------------------------------------------------------------------------------------------ #
#                                          TASK                                                    #
# ------------------------------------------------------------------------------------------------ #


class TaskDAO(DAO):
    """Provides access to task table ."""

    def __init__(self, connection: Connection) -> None:
        super(TaskDAO, self).__init__(connection)

    def create(self, data: Union[dict, DTO]) -> TaskORM:
        factory = DagFactory()
        return factory.create_task(data)

    def add(self, task: TaskORM) -> None:
        """Sets state of LocalDataset entity to 'added' and stores and adds its local store."""
        sequel = TaskInsert(task)
        id = self._database.insert(sequel.statement, sequel.parameters)
        task.id = id
        logger.info("Executed command: {}".format(sequel.command))
        return task

    def find(self, id: int) -> Union[TaskORM, None]:
        """Finds a LocalDataset entity by the designated column and value"""
        sequel = TaskSelectOne(parameters=(id))
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_key(self, task_id: int, dag_id: int) -> Union[TaskORM, None]:
        """Finds a Task entity by the dag and task id"""
        sequel = TaskSelectByKey(task_id=task_id, dag_id=dag_id)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_column(self, column: str, value: Any) -> Union[TaskORM, list]:
        """Finds a Task entity by the designated column and value"""
        sequel = TaskSelectByColumn(column=column, value=value)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self) -> list:
        """Returns all datasets in the database."""
        sequel = TaskSelectAll()
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select_all(sequel.statement)

    def remove(self, id: int) -> None:
        """Removes the dataset based upon the selection criteria"""
        sequel = TaskDelete(id=id)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, id: int) -> bool:
        """Determines if a dataset exists"""
        sequel = TaskExists(id=id)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.exists(sequel.statement, sequel.parameters)

    def begin_transaction(self) -> None:
        self._database.begin_transaction()

    def rollback(self) -> None:
        self._database.rollback()

    def save(self) -> None:
        self._database.save()

    def start(self, task: TaskORM) -> None:
        task.start = datetime.now()
        sequel = TaskStart(task=task)
        logger.info("Executed command: {}".format(sequel.command))
        self._database.execute(sequel.statement, sequel.parameters)
        return task

    def stop(self, task: TaskORM) -> None:
        task.stop = datetime.now()
        task.duration = (task.stop - task.start).total_seconds()
        sequel = TaskStop(task=task)
        logger.info("Executed command: {}".format(sequel.command))
        self._database.execute(sequel.statement, sequel.parameters)
        return task


# ------------------------------------------------------------------------------------------------ #
#                                         LOCAL FILE DAO                                           #
# ------------------------------------------------------------------------------------------------ #
class LocalFileDAO(DAO):
    """Provides access to local file table."""

    def __init__(self, connection: Connection) -> None:
        super(LocalFileDAO, self).__init__(connection)

    def create(self, data: Union[dict, DTO]) -> LocalFile:
        factory = LocalEntityFactory()
        return factory.create_file(data)

    def add(self, file: LocalFile) -> None:
        """Sets state of LocalFile entity to 'added' and stores and adds its local store."""
        sequel = LocalFileInsert(file)
        id = self._database.insert(sequel.statement, sequel.parameters)
        file.id = id
        logger.info("Executed command: {}".format(sequel.command))
        return id

    def find(self, id: int) -> Union[LocalFile, None]:
        """Finds a LocalFile entity by the designated column and value"""
        sequel = LocalFileSelectOne(parameters=(id))
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_key(
        self, name: str, dataset: str, datasource: str, stage: str
    ) -> Union[LocalFile, None]:
        """Finds a LocalFile entity by the designated column and value"""
        sequel = LocalFileSelectByKey(
            name=name, dataset=dataset, datasource=datasource, stage=stage
        )
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_column(self, column: str, value: Any) -> Union[LocalFile, list]:
        """Finds a LocalFile entity by the designated column and value"""
        sequel = LocalFileSelectByColumn(column=column, value=value)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self) -> list:
        """Returns all files in the database."""
        sequel = LocalFileSelectAll()
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select_all(sequel.statement)

    def remove(self, id: int) -> None:
        """Removes the file based upon the selection criteria"""
        sequel = LocalFileDelete(id=id)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, file: File, id: bool = False) -> bool:
        """Determines if a file exists.

        Args:
            file (File): The FileORM object
            id (bool): If True, the primary key for the object matching if exists.
        """

        """Determines if a file exists"""
        sequel = LocalFileExists(
            name=file.name,
            dataset=file.dataset,
            datasource=file.datasource,
            storage_type=file.storage_type,
        )
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.exists(sequel.statement, sequel.parameters)

    def begin_transaction(self) -> None:
        self._database.begin_transaction()

    def rollback(self) -> None:
        self._database.rollback()

    def save(self) -> None:
        self._database.save()


# ------------------------------------------------------------------------------------------------ #
#                                         S3 FILE DAO                                           #
# ------------------------------------------------------------------------------------------------ #
class S3FileDAO(DAO):
    """Provides access to s3 file table."""

    def __init__(self, connection: Connection) -> None:
        super(S3FileDAO, self).__init__(connection)

    def create(self, data: Union[dict, DTO]) -> S3File:
        factory = S3EntityFactory()
        return factory.create_file(data)

    def add(self, file: S3File) -> None:
        """Sets state of S3File entity to 'added' and stores and adds its s3 store."""
        sequel = S3FileInsert(file)
        id = self._database.insert(sequel.statement, sequel.parameters)
        file.id = id
        logger.info("Executed command: {}".format(sequel.command))
        return id

    def find(self, id: int) -> Union[S3File, None]:
        """Finds a S3File entity by the designated column and value"""
        sequel = S3FileSelectOne(parameters=(id))
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_key(
        self, name: str, dataset: str, datasource: str, stage: str
    ) -> Union[S3File, None]:
        """Finds a S3File entity by the designated column and value"""
        sequel = S3FileSelectByKey(name=name, dataset=dataset, datasource=datasource, stage=stage)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def find_by_column(self, column: str, value: Any) -> Union[S3File, list]:
        """Finds a S3File entity by the designated column and value"""
        sequel = S3FileSelectByColumn(column=column, value=value)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select(sequel.statement, sequel.parameters)

    def findall(self) -> list:
        """Returns all files in the database."""
        sequel = S3FileSelectAll()
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.select_all(sequel.statement)

    def remove(self, id: int) -> None:
        """Removes the file based upon the selection criteria"""
        sequel = S3FileDelete(id=id)
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.execute(sequel.statement, sequel.parameters)

    def exists(self, file: File, id: bool = False) -> bool:
        """Determines if a file exists.

        Args:
            file (File): The FileORM object
            id (bool): If True, the primary key for the object matching if exists.
        """

        sequel = S3FileExists(
            name=file.name,
            dataset=file.dataset,
            datasource=file.datasource,
            storage_type=file.storage_type,
        )
        logger.info("Executed command: {}".format(sequel.command))
        return self._database.exists(sequel.statement, sequel.parameters)

    def begin_transaction(self) -> None:
        self._database.begin_transaction()

    def rollback(self) -> None:
        self._database.rollback()

    def save(self) -> None:
        self._database.save()
