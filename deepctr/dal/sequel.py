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
# Modified   : Tuesday June 21st 2022 02:32:10 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module contains SQL command objects for each entity type."""
from abc import ABC, abstractmethod
import logging
from dataclasses import dataclass
from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.entity import Entity

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                            DAG                                                   #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DagInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `dag`
            (`name`, `desc`,`n_tasks`, `n_tasks_done`,`created`, `modified`, `started`, `stopped`, `duration`, `return_code`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.n_tasks,
            self.entity.n_tasks_done,
            self.entity.created,
            self.entity.modified,
            self.entity.started,
            self.entity.stopped,
            self.entity.duration,
            self.entity.return_code,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dag` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dag`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `dag` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `dag`
                            SET `name` = %s,
                                `desc` = %s,
                                `n_tasks` = %s,
                                `n_tasks_done` = %s,
                                `created` = %s,
                                `modified` = %s,
                                `started` = %s,
                                `stopped` = %s,
                                `duration` = %s,
                                `return_code` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.n_tasks,
            self.entity.n_tasks_done,
            self.entity.created,
            self.entity.modified,
            self.entity.started,
            self.entity.stopped,
            self.entity.duration,
            self.entity.return_code,
            self.entity.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                            TASK                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `task`
            (`name`, `desc`, `seq`, `dag_id`, `created`, `modified`, `started`, `stopped`, `duration`, `return_code`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.seq,
            self.entity.dag_id,
            self.entity.created,
            self.entity.modified,
            self.entity.started,
            self.entity.stopped,
            self.entity.duration,
            self.entity.return_code,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `task` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `task`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `task` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `task`
                            SET `name` = %s,
                                `desc` = %s,
                                `seq` = %s,
                                `dag_id` = %s,
                                `created` = %s,
                                `modified` = %s,
                                `started` = %s,
                                `stopped` = %s,
                                `duration` = %s,
                                `return_code` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.seq,
            self.entity.dag_id,
            self.entity.created,
            self.entity.modified,
            self.entity.started,
            self.entity.stopped,
            self.entity.duration,
            self.entity.return_code,
            self.entity.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                          LOCAL FILE                                              #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class LocalFileInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO localfile
            (`name`, `source`, `dataset`, `stage_id`, `stage_name`, `filepath`, `format`,
            `compressed`, `size`, `created`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.dataset,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.filepath,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `localfile` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `localfile`
                            SET `name` = %s,
                                `source` = %s,
                                `dataset` = %s,
                                `stage_id` = %s,
                                `stage_name` = %s,
                                `filepath` = %s,
                                `format` = %s,
                                `compressed` = %s,
                                `size` = %s,
                                `created` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.dataset,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.filepath,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.created,
            self.entity.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                            S3 FILE                                              #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3FileInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO s3file
            (`name`, `source`, `dataset`, `stage_id`, `stage_name`, `bucket`, `object_key`, `format`,
            `compressed`, `size`, `created`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.dataset,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.bucket,
            self.entity.object_key,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `s3file` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `s3file`
                            SET `name` = %s,
                                `source` = %s,
                                `dataset` = %s,
                                `stage_id` = %s,
                                `stage_name` = %s,
                                `bucket` = %s,
                                `object_key` = %s,
                                `format` = %s,
                                `compressed` = %s,
                                `size` = %s,
                                `created` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.dataset,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.bucket,
            self.entity.object_key,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.created,
            self.entity.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                          COMMAND                                                 #
# ------------------------------------------------------------------------------------------------ #
class EntitySQL(ABC):
    """Abstract base class for command classes, one for each entity."""

    @abstractmethod
    def insert(self, entity: Entity):
        pass

    @abstractmethod
    def select(self, id: int):
        pass

    @abstractmethod
    def select_all(self):
        pass

    @abstractmethod
    def update(self, entity: Entity):
        pass

    @abstractmethod
    def delete(self, id: int):
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         DAG COMMAND                                              #
# ------------------------------------------------------------------------------------------------ #
class DagSQL(EntitySQL):
    """Commands for the DAG table."""

    def insert(self, entity: Entity) -> DagInsert:
        return DagInsert(entity)

    def select(self, id: int) -> DagSelect:
        return DagSelect(parameters=(id,))

    def select_all(self) -> DagSelectAll:
        return DagSelectAll()

    def update(self, entity: Entity) -> DagUpdate:
        return DagUpdate(entity)

    def delete(self, id: int) -> DagDelete:
        return DagDelete(parameters=(id,))


# ------------------------------------------------------------------------------------------------ #
#                                        TASK COMMAND                                              #
# ------------------------------------------------------------------------------------------------ #
class TaskSQL(EntitySQL):
    """Commands for the Task table."""

    def insert(self, entity: Entity) -> TaskInsert:
        return TaskInsert(entity)

    def select(self, id: int) -> TaskSelect:
        return TaskSelect(parameters=(id,))

    def select_all(self) -> TaskSelectAll:
        return TaskSelectAll()

    def update(self, entity: Entity) -> TaskUpdate:
        return TaskUpdate(entity)

    def delete(self, id: int) -> TaskDelete:
        return TaskDelete(parameters=(id,))


# ------------------------------------------------------------------------------------------------ #
#                                     LOCALFILE COMMAND                                            #
# ------------------------------------------------------------------------------------------------ #
class LocalFileSQL(EntitySQL):
    """Commands for the LOCALFILE table."""

    def insert(self, entity: Entity) -> LocalFileInsert:
        return LocalFileInsert(entity)

    def select(self, id: int) -> LocalFileSelect:
        return LocalFileSelect(parameters=(id,))

    def select_all(self) -> LocalFileSelectAll:
        return LocalFileSelectAll()

    def update(self, entity: Entity) -> LocalFileUpdate:
        return LocalFileUpdate(entity)

    def delete(self, id: int) -> LocalFileDelete:
        return LocalFileDelete(parameters=(id,))


# ------------------------------------------------------------------------------------------------ #
#                                     S3FILE COMMAND                                            #
# ------------------------------------------------------------------------------------------------ #
class S3FileSQL(EntitySQL):
    """Commands for the S3FILE table."""

    def insert(self, entity: Entity) -> S3FileInsert:
        return S3FileInsert(entity)

    def select(self, id: int) -> S3FileSelect:
        return S3FileSelect(parameters=(id,))

    def select_all(self) -> S3FileSelectAll:
        return S3FileSelectAll()

    def update(self, entity: Entity) -> S3FileUpdate:
        return S3FileUpdate(entity)

    def delete(self, id: int) -> S3FileDelete:
        return S3FileDelete(parameters=(id,))
