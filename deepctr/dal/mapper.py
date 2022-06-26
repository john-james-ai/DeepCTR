#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /mapper.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 22nd 2022 08:41:02 pm                                                    #
# Modified   : Sunday June 26th 2022 03:08:54 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module contains SQL command objects for each entity type."""
from abc import ABC, abstractmethod
import logging
from dataclasses import dataclass
from deepctr.utils.log_config import LOG_CONFIG
from deepctr import Entity
from deepctr.dal.base import File, Dataset

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
#                                          FILE                                                    #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `file`
            (`name`, `source`, `dataset_id`, `dataset`, `storage_type`, `format`,
            `stage_id`, `stage_name`, `home`, `bucket`, `filepath`,
            `compressed`, `rows`, `cols`, `size`,`exists`,
            `created`, `modified`,`accessed`)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.dataset_id,
            self.entity.dataset,
            self.entity.storage_type,
            self.entity.format,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.home,
            self.entity.bucket,
            self.entity.filepath,
            self.entity.compressed,
            self.entity.rows,
            self.entity.cols,
            self.entity.size,
            self.entity.exists,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `file` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSelectByDatasetId:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `file` WHERE `dataset_id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `file`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `file` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `file`
                            SET `name` = %s,
                                `source` = %s,
                                `dataset_id` = %s,
                                `dataset` = %s,
                                `storage_type` = %s,
                                `format` = %s,
                                `stage_id` = %s,
                                `stage_name` = %s,
                                `home` = %s,
                                `bucket` = %s,
                                `filepath` = %s,
                                `compressed` = %s,
                                `rows` = %s,
                                `cols` = %s,
                                `size` = %s,
                                `exists` = %s,
                                `created` = %s,
                                `modified` = %s,
                                `accessed` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.dataset_id,
            self.entity.dataset,
            self.entity.storage_type,
            self.entity.format,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.home,
            self.entity.bucket,
            self.entity.filepath,
            self.entity.compressed,
            self.entity.rows,
            self.entity.cols,
            self.entity.size,
            self.entity.exists,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
            self.entity.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                         DATASET                                                  #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `dataset`
            (`name`, `source`, `storage_type`, `folder`,`format`,
             `stage_id`, `stage_name`, `compressed`, `size`,`home`,
             `bucket`,  `created`,  `modified`,  `accessed`)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s,%s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.storage_type,
            self.entity.folder,
            self.entity.format,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.compressed,
            self.entity.size,
            self.entity.home,
            self.entity.bucket,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dataset` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `dataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `dataset` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `dataset`
                            SET `name` = %s,
                                `source` = %s,
                                `storage_type` = %s,
                                `folder` = %s,
                                `format` = %s,
                                `stage_id` = %s,
                                `stage_name` = %s,
                                `compressed` = %s,
                                `size` = %s,
                                `home` = %s,
                                `bucket` = %s,
                                `created` = %s,
                                `modified` = %s,
                                `accessed` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.source,
            self.entity.storage_type,
            self.entity.folder,
            self.entity.format,
            self.entity.stage_id,
            self.entity.stage_name,
            self.entity.compressed,
            self.entity.size,
            self.entity.home,
            self.entity.bucket,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
            self.entity.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                          MAPPER                                                  #
# ------------------------------------------------------------------------------------------------ #
class EntityMapper(ABC):
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

    @abstractmethod
    def factory(self, record: dict) -> Entity:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         DAG MAPPER                                               #
# ------------------------------------------------------------------------------------------------ #
class DagMapper(EntityMapper):
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

    def factory(self, record: dict) -> Entity:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                        TASK MAPPER                                               #
# ------------------------------------------------------------------------------------------------ #
class TaskMapper(EntityMapper):
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

    def factory(self, record: dict) -> Entity:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                        FILE MAPPER                                               #
# ------------------------------------------------------------------------------------------------ #
class FileMapper(EntityMapper):
    """Commands for the file table."""

    def insert(self, entity: Entity) -> FileInsert:
        return FileInsert(entity)

    def select(self, id: int) -> FileSelect:
        return FileSelect(parameters=(id,))

    def select_by_dataset_id(self, dataset_id: int) -> FileSelectByDatasetId:
        return FileSelectByDatasetId(parameters=(dataset_id,))

    def select_all(self) -> FileSelectAll:
        return FileSelectAll()

    def update(self, entity: Entity) -> FileUpdate:
        return FileUpdate(entity)

    def delete(self, id: int) -> FileDelete:
        return FileDelete(parameters=(id,))

    def factory(self, record: dict) -> Entity:
        return File(
            id=record["id"],
            name=record["name"],
            source=record["source"],
            storage_type=record["storage_type"],
            format=record["format"],
            stage_id=record["stage_id"],
            stage_name=record["stage_name"],
            home=record["home"],
            bucket=record["bucket"],
            filepath=record["filepath"],
            compressed=record["compressed"],
            size=record["size"],
            rows=record["rows"],
            cols=record["cols"],
            dataset_id=record["dataset_id"],
            created=record["created"],
            modified=record["modified"],
            accessed=record["accessed"],
        )


# ------------------------------------------------------------------------------------------------ #
#                                       DATASET MAPPER                                             #
# ------------------------------------------------------------------------------------------------ #
class DatasetMapper(EntityMapper):
    """Commands for the dataset table."""

    def insert(self, entity: Entity) -> DatasetInsert:
        return DatasetInsert(entity)

    def select(self, id: int) -> DatasetSelect:
        return DatasetSelect(parameters=(id,))

    def select_all(self) -> DatasetSelectAll:
        return DatasetSelectAll()

    def update(self, entity: Entity) -> DatasetUpdate:
        return DatasetUpdate(entity)

    def delete(self, id: int) -> DatasetDelete:
        return DatasetDelete(parameters=(id,))

    def factory(self, record: dict) -> Entity:
        return Dataset(
            id=record["id"],
            name=record["name"],
            source=record["source"],
            storage_type=record["storage_type"],
            folder=record["folder"],
            format=record["format"],
            stage_id=record["stage_id"],
            stage_name=record["stage_name"],
            compressed=record["compressed"],
            size=record["size"],
            home=record["home"],
            bucket=record["bucket"],
            created=record["created"],
            modified=record["modified"],
            accessed=record["accessed"],
        )
