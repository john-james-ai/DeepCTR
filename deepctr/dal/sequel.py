#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /sequel.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 22nd 2022 08:41:02 pm                                                    #
# Modified   : Saturday May 28th 2022 03:43:00 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module contains SQL command objects for each entity type."""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from deepctr.dal.entity import Dataset, LocalFile, LocalDataset, S3File, S3Dataset, DagORM, TaskORM
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                            DAG                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagInsert:
    entity: DagORM
    statement: str = None
    parameters: tuple = None
    command: str = "dag_insert"

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `dag`
            (`seq`, `name`, `desc`, `start`, `stop`, `duration`, `created`)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.seq,
            self.entity.name,
            self.entity.desc,
            self.entity.start,
            self.entity.stop,
            self.entity.duration,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagSelectOne:
    parameters: tuple
    statement: str = None
    command: str = "dag_selectone"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dag` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None
    command: str = "dag_select_by_column"

    def __post_init__(self) -> None:
        self.parameters = self.value
        d = {
            "id": """SELECT * FROM `dag` WHERE `id`=%s;""",
            "name": """SELECT * FROM `dag` WHERE `name`=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagSelectAll:
    statement: str = None
    command: str = "dag_selectall"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dag`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagDelete:
    parameters: tuple = None
    statement: str = None
    command: str = "dag_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `dag` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagExists:
    statement: str = None
    parameters: tuple = None
    command: str = "dag_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `dag` WHERE `id`=%s);"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagStart:
    dag: DagORM
    parameters: tuple = None
    statement: str = None
    command: str = "dag_start"

    def __post_init__(self) -> None:
        self.parameters = (self.dag.start, self.dag.id)
        self.statement = """UPDATE `dag` SET `start` = %s WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagStop:
    dag: DagORM
    parameters: tuple = None
    statement: str = None
    command: str = "dag_stop"

    def __post_init__(self) -> None:
        self.parameters = (self.dag.stop, self.dag.duration, self.dag.id)
        self.statement = """UPDATE `dag` SET `stop` = %s, `duration` = %s WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
#                                            TASK                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskInsert:
    entity: TaskORM
    statement: str = None
    parameters: tuple = None
    command: str = "task_insert"

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `task`
            (`seq`, `name`, `desc`, `dag_id`, `start`, `stop`, `duration`, `created`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.seq,
            self.entity.name,
            self.entity.desc,
            self.entity.dag_id,
            self.entity.start,
            self.entity.stop,
            self.entity.duration,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskSelectOne:
    parameters: tuple
    statement: str = None
    command: str = "task_selectone"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `task` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None
    command: str = "task_select_by_column"

    def __post_init__(self) -> None:
        self.parameters = self.value
        d = {
            "id": """SELECT * FROM `task` WHERE `id`=%s;""",
            "name": """SELECT * FROM `task` WHERE `name`=%s;""",
            "task_id": """SELECT * FROM `task` WHERE `dag_id`=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskSelectByKey:
    task_id: int
    dag_id: int
    parameters: tuple = None
    statement: str = None
    command: str = "task_select_by_key"

    def __post_init__(self) -> None:
        self.parameters = (self.task_id, self.dag_id)
        self.statement = """SELECT * FROM `task` WHERE `id`=%s AND `dag_id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskSelectAll:
    statement: str = None
    command: str = "task_selectall"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `task`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDelete:
    parameters: tuple = None
    statement: str = None
    command: str = "task_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `task` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskExists:
    statement: str = None
    parameters: tuple = None
    command: str = "task_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `task` WHERE `id`=%s);"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskStart:
    task: TaskORM
    parameters: tuple = None
    statement: str = None
    command: str = "task_start"

    def __post_init__(self) -> None:
        self.parameters = (self.task.start, self.task.id)
        self.statement = """UPDATE `task` SET `start` = %s WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskStop:
    task: TaskORM
    parameters: tuple = None
    statement: str = None
    command: str = "task_stop"

    def __post_init__(self) -> None:

        self.parameters = (self.task.stop, self.task.duration, self.task.id)
        self.statement = """UPDATE `task` SET `stop` = %s, `duration` = %s WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
#                                            FILE                                                  #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class LocalFileInsert:
    entity: LocalFile
    statement: str = None
    parameters: tuple = None
    command: str = "localfile_insert"

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO localfile
            (`name`, `dataset`, `dataset_id`, `datasource`, `stage`, `storage_type`, `filename`, `filepath`, `format`,
            `compressed`, `size`, `dag_id`, `task_id`, `home`, `created`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.dataset,
            self.entity.dataset_id,
            self.entity.datasource,
            self.entity.stage,
            self.entity.storage_type,
            self.entity.filename,
            self.entity.filepath,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.dag_id,
            self.entity.task_id,
            self.entity.home,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectOne:
    parameters: tuple
    statement: str = None
    command: str = "localfile_selectone"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None
    command: str = "localfile_select_by_column"

    def __post_init__(self) -> None:
        self.parameters = self.value
        d = {
            "id": """SELECT * FROM `localfile` WHERE `id`=%s;""",
            "name": """SELECT * FROM `localfile` WHERE `name`=%s;""",
            "dataset": """SELECT * FROM `localfile` WHERE `dataset`=%s;""",
            "datasource": """SELECT * FROM `localfile` WHERE `datasource`=%s;""",
            "stage": """SELECT * FROM `localfile` WHERE `stage` =%s;""",
            "storage_type": """SELECT * FROM `localfile` WHERE `storage_type`=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectByKey:
    name: str
    dataset: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None
    command: str = "localfile_select_by_key"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile` WHERE `name`=%s AND
                                                            `dataset`=%s AND
                                                            `datasource`=%s AND
                                                            `stage` =%s;"""
        self.parameters(self.name, self.dataset, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectAll:
    statement: str = None
    command: str = "localfile_selectall"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileDelete:
    parameters: tuple = None
    statement: str = None
    command: str = "localfile_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `localfile` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileExists:
    name: str
    dataset: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None
    command: str = "localfile_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `localfile` WHERE `name`=%s AND
                                                                   `dataset_id`=%s AND
                                                                   `datasource`=%s AND
                                                                   `storage_type` =%s);"""
        self.parameters = (self.name, self.dataset, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
#                                           S3 FILE                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3FileInsert:
    entity: S3File
    statement: str = None
    parameters: tuple = None
    command: str = "s3file_insert"

    def __post_init__(self) -> None:
        self.statement = """
                INSERT INTO `s3file`
                (`name`, `dataset`, `dataset_id`, `datasource`, `storage_type`, `bucket`, `object_key`, `format`, `compressed`,
                `size`, `dag_id`, `task_id`, `created`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        self.parameters = (
            self.entity.name,
            self.entity.dataset,
            self.entity.dataset_id,
            self.entity.datasource,
            self.entity.storage_type,
            self.entity.bucket,
            self.entity.object_key,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.dag_id,
            self.entity.task_id,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectOne:
    parameters: tuple
    statement: str = None
    command: str = "s3file_selectone"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None
    command: str = "s3file_select_by_column"

    def __post_init__(self) -> None:
        self.parameters = self.value
        d = {
            "id": """SELECT * FROM `s3file` WHERE `id`=%s;""",
            "name": """SELECT * FROM `s3file` WHERE `name`=%s;""",
            "dataset": """SELECT * FROM `s3file` WHERE `dataset`=%s;""",
            "datasource": """SELECT * FROM `s3file` WHERE `datasource`=%s;""",
            "stage": """SELECT * FROM `s3file` WHERE `stage` =%s;""",
            "storage_type": """SELECT * FROM `s3file` WHERE `storage_type`=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectByKey:
    name: str
    dataset: str
    datasource: str
    bucket: str = "deepctr"
    statement: str = None
    parameters: tuple = None
    command: str = "s3file_select_by_key"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file` WHERE `name`=%s AND
                                                            `dataset`=%s AND
                                                            `datasource`=%s AND
                                                            `bucket`=%s;"""
        self.parameters(self.name, self.dataset, self.datasource, self.bucket)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectAll:
    statement: str = None
    command: str = "s3file_selectall"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileExists:
    name: str
    dataset: str
    datasource: str
    statement: str = None
    parameters: tuple = None
    bucket: str = "deepctr"
    command: str = "s3file_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `s3file` WHERE `name`=%s AND
                                                                   `dataset_id`=%s AND
                                                                   `datasource`=%s AND
                                                                   `storage_type`=%s);"""
        self.parameters = (self.name, self.dataset, self.datasource, self.bucket)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileDelete:
    parameters: tuple
    statement: str = None
    command: str = "s3file_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `s3file` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
#                                         DATASET                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetInsert:
    entity: Dataset
    statement: str = None
    parameters: tuple = None
    command: str = "dataset_insert"

    def __post_init__(self) -> None:
        self.statement = """
                    INSERT INTO dataset
                    (`name`, `desc`, `status`, `datasource`, `storage_type`,`table`, `dag_id`, `task_id`, `created`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.status,
            self.entity.datasource,
            self.entity.storage_type,
            self.entity.table,
            self.entity.dag_id,
            self.entity.task_id,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelectOne:
    parameters: tuple
    statement: str
    command: str = "dataset_selectone"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dataset` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None
    command: str = "dataset_select_by_column"

    def __post_init__(self) -> None:
        self.parameters = self.value
        d = {
            "id": """SELECT * FROM `dataset` WHERE `id`=%s;""",
            "name": """SELECT * FROM `dataset` WHERE `name`=%s;""",
            "datasource": """SELECT * FROM `dataset` WHERE `datasource`=%s;""",
            "storage_type": """SELECT * FROM `dataset` WHERE `storage_type`=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelectByKey:
    name: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None
    command: str = "dataset_select_by_key"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dataset` WHERE `name`=%s AND
                                                            `datasource`=%s;"""
        self.parameters(self.name, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelectAll:
    statement: str = None
    command: str = "dataset_selectall"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetUpdate:
    name: str
    desc: str
    status: str
    datasource: str
    storage_type: str
    table: str
    dag_id: int
    task_id: int
    created: datetime
    parameters: tuple
    statement: str
    command: str = "dataset_update"

    def __post_init__(self) -> None:
        self.parameters = (
            self.name,
            self.desc,
            self.status,
            self.datasource,
            self.storage_type,
            self.table,
            self.dag_id,
            self.task_id,
            self.created,
        )
        self.statement = """UPDATE `dataset` SET {`name`, `desc`, `status`, `datasource`,
        `storage_type`, `table`, `dag_id`, `task_id`, `created`} WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDelete:
    parameters: tuple
    statement: str
    command: str = "dataset_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `dataset` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetExists:
    name: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None
    command: str = "dataset_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `file` WHERE `name`=%s AND
                                                                   `datasource`=%s AND
                                                                   `storage_type`=%s);"""
        self.parameters = (self.name, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
#                                       LOCAL DATASET                                              #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class LocalDatasetInsert:
    entity: LocalDataset
    statement: str = None
    parameters: tuple = None
    command: str = "localdataset_insert"

    def __post_init__(self) -> None:
        self.statement = """
                    INSERT INTO localdataset
                    (`name`, `datasource`, `stage`, `storage_type`, `folder`, `size`, `dag_id`, `home`, `created`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
        self.parameters = (
            self.entity.name,
            self.entity.datasource,
            self.entity.stage,
            self.entity.storage_type,
            self.entity.folder,
            self.entity.size,
            self.entity.dag_id,
            self.entity.home,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectOne:
    parameters: tuple
    statement: str
    command: str = "localdataset_selectone"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localdataset` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None
    command: str = "localdataset_select_by_column"

    def __post_init__(self) -> None:
        self.parameters = self.value
        d = {
            "id": """SELECT * FROM `localdataset` WHERE `id`=%s;""",
            "name": """SELECT * FROM `localdataset` WHERE `name`=%s;""",
            "datasource": """SELECT * FROM `localdataset` WHERE `datasource`=%s;""",
            "stage": """SELECT * FROM `localdataset` WHERE `stage` =%s;""",
            "storage_type": """SELECT * FROM `localdataset` WHERE `storage_type`=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectByKey:
    name: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None
    command: str = "localdataset_select_by_key"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localdataset` WHERE `name`=%s AND
                                                            `datasource`=%s AND
                                                            `stage` =%s;"""
        self.parameters(self.name, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectAll:
    statement: str = None
    command: str = "localdataset_selectall"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localdataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetUpdate:
    name: str
    desc: str
    status: str
    datasource: str
    stage: str
    storage_type: str
    folder: str
    size: int
    dag_id: int
    task_id: int
    home: str
    created: datetime
    parameters: tuple
    statement: str
    command: str = "local_dataset_update"

    def __post_init__(self) -> None:
        self.parameters = (
            self.name,
            self.desc,
            self.status,
            self.datasource,
            self.stage,
            self.storage_type,
            self.folder,
            self.size,
            self.dag_id,
            self.task_id,
            self.home,
            self.created,
        )
        self.statement = """UPDATE `localdataset` SET {`name`, `desc`, `status`, `datasource`,
        `stage`, `storage_type`, `folder`,`size`, `dag_id`, `task_id`,`home`, `created`} WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetDelete:
    parameters: tuple
    statement: str
    command: str = "localdataset_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `localdataset` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetExists:
    name: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None
    command: str = "localdataset_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `localfile` WHERE `name`=%s AND
                                                                   `datasource`=%s AND
                                                                   `storage_type` =%s);"""
        self.parameters = (self.name, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
#                                       S3 DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3DatasetInsert:
    entity: S3Dataset
    statement: str = None
    parameters: tuple = None
    command: str = "s3dataset_insert"

    def __post_init__(self) -> None:
        self.statement = """
                INSERT INTO s3dataset
                (`name`, `datasource`, `storage_type`, `bucket`, `folder`, `size`, `dag_id`, `created`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
        self.parameters = (
            self.entity.name,
            self.entity.datasource,
            self.entity.storage_type,
            self.entity.bucket,
            self.entity.folder,
            self.entity.size,
            self.entity.dag_id,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelectOne:
    parameters: tuple
    statement: str = None
    command: str = "s3dataset_selectone"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3dataset` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None
    command: str = "s3dataset_select_by_column"

    def __post_init__(self) -> None:
        self.parameters = self.value
        d = {
            "id": """SELECT * FROM `s3dataset` WHERE `id`=%s;""",
            "name": """SELECT * FROM `s3dataset` WHERE `name`=%s;""",
            "datasource": """SELECT * FROM `s3dataset` WHERE `datasource`=%s;""",
            "stage": """SELECT * FROM `s3dataset` WHERE `stage` =%s;""",
            "storage_type": """SELECT * FROM `s3dataset` WHERE `storage_type`=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


@dataclass
class S3DatasetSelectByKey:
    name: str
    datasource: str
    bucket: str = "deepctr"
    statement: str = None
    parameters: tuple = None
    command: str = "s3dataset_select_by_key"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file` WHERE `name`=%s AND
                                                            `datasource`=%s AND
                                                            `bucket`=%s;"""
        self.parameters(self.name, self.datasource, self.bucket)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelectAll:
    statement: str = None
    command: str = "s3dataset_selectall"

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3dataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetDelete:
    parameters: tuple
    statement: str = None
    command: str = "s3dataset_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `s3dataset` WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetExists:
    name: str
    datasource: str
    statement: str = None
    parameters: tuple = None
    command: str = "s3dataset_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `s3dataset` WHERE `name`=%s AND
                                                                   `datasource`=%s AND
                                                                   `storage_type` = %s);"""
        self.parameters = (self.name, self.datasource)
