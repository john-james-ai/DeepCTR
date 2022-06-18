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
# Modified   : Saturday June 18th 2022 09:26:32 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module contains SQL command objects for each entity type."""
import logging
from dataclasses import dataclass
from typing import Any
from deepctr.dag.base import Operator
from deepctr.dag.orchestrator import DAG
from deepctr.dal.fao import LocalFile, S3File
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                            DAG                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagInsert:
    entity: DAG
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
    dag: DAG
    parameters: tuple = None
    statement: str = None
    command: str = "dag_start"

    def __post_init__(self) -> None:
        self.parameters = (self.dag.start, self.dag.id)
        self.statement = """UPDATE `dag` SET `start` = %s WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagStop:
    dag: DAG
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
    task: Operator
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
            self.task.seq,
            self.task.name,
            self.task.desc,
            self.task.dag_id,
            self.task.start,
            self.task.stop,
            self.task.duration,
            self.task.created,
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
            "dag_id": """SELECT * FROM `task` WHERE `dag_id`=%s;""",
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
    task: Operator
    parameters: tuple = None
    statement: str = None
    command: str = "task_start"

    def __post_init__(self) -> None:
        self.parameters = (self.task.start, self.task.id)
        self.statement = """UPDATE `task` SET `start` = %s WHERE `id`=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskStop:
    task: Operator
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
    file: LocalFile
    statement: str = None
    parameters: tuple = None
    command: str = "localfile_insert"

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO localfile
            (`name`, `source`, `dataset`, `stage_id`, `stage_name`, `filepath`, `directory`, `filename`, `format`,
            `compressed`, `size`, `dag_id`, `task_id`, `created`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.file.name,
            self.file.source,
            self.file.dataset,
            self.file.stage_id,
            self.file.stage_name,
            self.file.filepath,
            self.file.directory,
            self.file.filename,
            self.file.format,
            self.file.compressed,
            self.file.size,
            self.file.dag_id,
            self.file.task_id,
            self.file.created,
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
            "source": """SELECT * FROM `localfile` WHERE `source`=%s;""",
            "stage_id": """SELECT * FROM `localfile` WHERE `stage_id` =%s;""",
            "stage_name": """SELECT * FROM `localfile` WHERE `stage_name` =%s;""",
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
                                                            `source`=%s AND
                                                            `stage_id` =%s;"""
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
                                                                   `dataset`=%s AND
                                                                   `source`=%s);"""
        self.parameters = (self.name, self.dataset, self.source)


# ------------------------------------------------------------------------------------------------ #
#                                           S3 FILE                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3FileInsert:
    file: S3File
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
            self.file.name,
            self.file.source,
            self.file.dataset,
            self.file.stage_id,
            self.file.stage_name,
            self.file.bucket,
            self.file.object_key,
            self.file.filename,
            self.file.format,
            self.file.compressed,
            self.file.size,
            self.file.dag_id,
            self.file.task_id,
            self.file.created,
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
            "source": """SELECT * FROM `s3file` WHERE `source`=%s;""",
            "stage_id": """SELECT * FROM `s3file` WHERE `stage_id` =%s;""",
            "stage_name": """SELECT * FROM `s3file` WHERE `stage_name` =%s;""",
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
                                                            `source`=%s AND
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
    bucket: str
    object_key: str
    statement: str = None
    parameters: tuple = None
    bucket: str = "deepctr"
    command: str = "s3file_exists"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `s3file` WHERE `bucket`=%s AND
                                                                   `object_key`=%s);"""
        self.parameters = (self.bucket, self.object_key)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileDelete:
    parameters: tuple
    statement: str = None
    command: str = "s3file_delete"

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `s3file` WHERE `id`=%s;"""
