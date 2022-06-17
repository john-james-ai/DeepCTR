#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /entity.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 19th 2022 06:43:34 pm                                                  #
# Modified   : Saturday May 28th 2022 02:53:39 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import os
from types import SimpleNamespace
from dataclasses import dataclass
from datetime import datetime
import logging
from difflib import get_close_matches
from typing import Union

from deepctr.dal.base import Entity
from deepctr.dal.dto import DTO
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
BUCKETS = ["deepctr"]
STAGES = ["raw", "staged", "interim", "clean", "processed", "extract", "archive"]
DATASOURCES = ["alibaba", "avazu", "criteo"]
FORMATS = ["csv", "parquet", "pickle", "tar.gz"]
STORAGE_TYPES = ["local", "s3"]
# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperatorEntity(Entity):
    def start(self) -> None:
        self.start = datetime.now()

    def stop(self) -> None:
        self.stop: datetime.now()
        self.duration = (self.stop - self.start).total_seconds()

    def to_dict(self) -> dict:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         DAG OBJECT                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagORM(OperatorEntity):

    id: int
    seq: int
    name: str
    desc: str
    duration: int
    created: datetime
    start: datetime
    stop: datetime

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "seq": self.seq,
            "name": self.name,
            "desc": self.desc,
            "start": self.start,
            "stop": self.stop,
            "duration": self.duration,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                         TASK OBJECT                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskORM(OperatorEntity):

    id: int
    seq: int
    name: str
    desc: str
    duration: int
    dag_id: int
    created: datetime
    start: datetime
    stop: datetime

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "seq": self.seq,
            "name": self.name,
            "desc": self.desc,
            "dag_id": self.dag_id,
            "start": self.start,
            "stop": self.stop,
            "duration": self.duration,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT FILE                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AbstractFile(Entity):
    """Defines the interface for File objects."""

    name: str  # Name of file without the extension
    dataset: str  # Name of the dataset, distinguishing it from the original
    dataset_id: int  # The id for the dataset to which the file belongs.
    datasource: str  # Original datasource, i.e. 'alibaba', 'criter', etc....
    storage_type: str  # Either 'local' or 's3'

    dag_id: int  # The dag_id for the dag in which the file was created.
    task_id: int  # The task_id for the task that created the file.

    def to_dict(self) -> dict:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         FILE                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class File(AbstractFile):
    """Defines the file objects stored locally."""

    id: int = 0
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "dataset": self.dataset,
            "dataset_id": self.dataset_id,
            "datasource": self.datasource,
            "storage_type": self.storage_type,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                     LOCAL FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class LocalFile(AbstractFile):
    """Defines the file objects stored locally."""

    stage: str
    filename: str
    filepath: str
    format: str
    compressed: bool
    size: int
    home: str
    id: int = 0
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "dataset": self.dataset,
            "dataset_id": self.dataset_id,
            "datasource": self.datasource,
            "stage": self.stage,
            "storage_type": self.storage_type,
            "filename": self.filename,
            "filepath": self.filepath,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "home": self.home,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                        S3 FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class S3File(AbstractFile):
    """Defines the S3 file objects."""

    bucket: str
    object_key: str
    format: str
    compressed: bool
    size: int
    id: int = 0
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "dataset": self.dataset,
            "dataset_id": self.dataset_id,
            "datasource": self.datasource,
            "storage_type": self.storage_type,
            "bucket": self.bucket,
            "object_key": self.object_key,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "screated": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT DATASET                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AbstractDataset(Entity):
    """Defines the interface for Dataset objects."""

    name: str  # Name of file without the extension
    datasource: str  # Original datasource, i.e. 'alibaba', 'criter', etc....
    storage_type: str  # Either 'local' or 's3'
    dag_id: int  # The dag_id for the dag in which the file was created.

    def to_dict(self) -> dict:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         DATASET                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class Dataset(AbstractDataset):
    """Defines the core Dataset table, containing datasets of all types.."""

    id: int = 0
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "datasource": self.datasource,
            "storage_type": self.storage_type,
            "table": self.table,
            "dag_id": self.dag_id,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                     LOCAL DATASET                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class LocalDataset(AbstractDataset):
    """Defines the file objects stored locally."""

    folder: str
    size: int
    stage: str
    home: str
    id: int = 0
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "datasource": self.datasource,
            "stage": self.stage,
            "storage_type": self.storage_type,
            "folder": self.folder,
            "size": self.size,
            "dag_id": self.dag_id,
            "home": self.home,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                        S3 DATASET                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class S3Dataset(AbstractDataset):
    """Defines the S3 file objects."""

    bucket: str
    folder: str
    size: int
    id: int = 0
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "datasource": self.datasource,
            "storage_type": self.storage_type,
            "bucket": self.bucket,
            "folder": self.folder,
            "size": self.size,
            "dag_id": self.dag_id,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT DAG FACTORY                                          #
# ------------------------------------------------------------------------------------------------ #
class AbstractDagFactory(ABC):
    """Defines the interface for the DAG/TASK factory."""

    @abstractmethod
    def create_dag(self, data: Union[DTO, dict]) -> DagORM:
        pass

    @abstractmethod
    def create_task(self, data: Union[DTO, dict]) -> TaskORM:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                      DAG FACTORY                                                 #
# ------------------------------------------------------------------------------------------------ #
class DagFactory(AbstractDagFactory):
    """Defines the interface for the DAG/TASK factory."""

    def create_dag(self, data: Union[DTO, dict]) -> DagORM:
        data = self._validate(data)
        dag = DagORM(
            id=data.id,
            seq=data.seq,
            name=data.name,
            desc=data.desc,
            start=data.start,
            stop=None,
            duration=None,
            created=data.created,
        )
        return dag

    def create_task(self, data: Union[DTO, dict]) -> TaskORM:
        data = self._validate(data)
        task = TaskORM(
            id=data.id,
            seq=data.seq,
            dag_id=data.dag_id,
            name=data.name,
            desc=data.desc,
            start=None,
            stop=None,
            duration=None,
            created=data.created,
        )
        return task

    def _validate(self, data: Union[DTO, dict]) -> DTO:
        if isinstance(data, dict):
            data = SimpleNamespace(**data)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class AbstractFactory(ABC):
    """The interface defining the methods for returning the abstract File and Dataset products."""

    @abstractmethod
    def create_file(self, data: Union[DTO, dict]) -> AbstractFile:
        pass

    @abstractmethod
    def create_dataset(self, data: Union[DTO, dict]) -> AbstractDataset:
        pass

    def _validate(self, data: Union[DTO, dict]) -> DTO:
        """ Validates / coerces data parameters and returns a valid DTO."""
        if isinstance(data, dict):
            data = SimpleNamespace(**data)
        try:
            data.datasource = get_close_matches(data.datasource, DATASOURCES)[0]
            data.storage_type = get_close_matches(data.storage_type, STORAGE_TYPES)[0]

        except IndexError as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)

        return data


# ------------------------------------------------------------------------------------------------ #
#                                       ENTITY FACTORY                                             #
# ------------------------------------------------------------------------------------------------ #
class EntityFactory(AbstractFactory):
    """Creates file and dataset objects"""

    def create_file(self, data: Union[DTO, dict]) -> AbstractFile:
        if isinstance(data, DTO):
            return self._create_file_from_dto(data)
        else:
            return self._create_file_from_dict(data)

    def _create_file_from_dto(self, data: DTO) -> AbstractFile:

        # Validate / convert to namespace / coerce parameters
        data = self._validate_file(data)

        file = File(
            name=data.name,
            dataset=data.dataset,
            dataset_id=data.dataset_id,
            datasource=data.datasource,
            storage_type=data.storage_type,
            dag_id=data.dag_id,
            task_id=data.task_id,
            home=data.home,
        )

        return file

    def _create_file_from_dict(self, data: dict) -> AbstractFile:
        data = self._validate_file(data)
        file = File(
            name=data.name,
            dataset=data.dataset,
            dataset_id=data.dataset_id,
            datasource=data.datasource,
            storage_type=data.storage_type,
            dag_id=data.dag_id,
            task_id=data.task_id,
            created=data.created,
        )
        return file

    def create_dataset(self, data: Union[DTO, dict]) -> AbstractDataset:
        if isinstance(data, DTO):
            return self._create_dataset_from_dto(data)
        else:
            return self._create_dataset_from_dict(data)

    def _create_dataset_from_dto(self, data: DTO) -> AbstractDataset:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_dataset(data)

        dataset = Dataset(
            name=data.name,
            desc=data.desc,
            status=data.status,
            datasource=data.datasource,
            storage_type=data.storage_type,
            dag_id=data.dag_id,
            task_id=data.task_id,
        )

        return dataset

    def _create_dataset_from_dict(self, data: dict) -> AbstractDataset:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_dataset(data)

        dataset = Dataset(
            name=data.name,
            desc=data.desc,
            status=data.status,
            datasource=data.datasource,
            storage_type=data.storage_type,
            dag_id=data.dag_id,
            task_id=data.task_id,
        )

        return dataset

    def _validate_file(self, data: Union[DTO, dict]) -> DTO:
        data = super(EntityFactory, self)._validate(data)
        try:
            data.stage = get_close_matches(data.stage, STAGES)[0]
            data.format = get_close_matches(data.format, FORMATS)[0]

        except IndexError as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)
        return data

    def _validate_dataset(self, data: Union[DTO, dict]) -> DTO:
        data = super(EntityFactory, self)._validate(data)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                  LOCAL ENTITY FACTORY                                            #
# ------------------------------------------------------------------------------------------------ #
class LocalEntityFactory(AbstractFactory):
    """Creates local file and dataset objects"""

    def create_file(self, data: Union[DTO, dict]) -> AbstractFile:
        if isinstance(data, DTO):
            return self._create_file_from_dto(data)
        else:
            return self._create_file_from_dict(data)

    def _create_file_from_dto(self, data: DTO) -> AbstractFile:

        # Validate / convert to namespace / coerce parameters
        data = self._validate_file(data)

        # Format the filename and filepath based parameters of the file.
        filename = data.name + "." + data.format
        if data.compressed and "tar.gz" not in filename:
            filename = filename + ".tar.gz"

        filepath = os.path.join(data.home, data.datasource, data.dataset, data.stage, filename)

        file = LocalFile(
            name=data.name,
            desc=data.desc,
            dataset=data.dataset,
            dataset_id=data.dataset_id,
            datasource=data.datasource,
            stage=data.stage,
            storage_type=data.storage_type,
            filename=filename,
            filepath=filepath,
            format=data.format,
            compressed=data.compressed,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
            home=data.home,
        )

        return file

    def _create_file_from_dict(self, data: dict) -> AbstractFile:
        data = self._validate_file(data)
        file = LocalFile(
            name=data.name,
            desc=data.desc,
            dataset=data.dataset,
            dataset_id=data.dataset_id,
            datasource=data.datasource,
            stage=data.stage,
            storage_type=data.storage_type,
            filename=data.filename,
            filepath=data.filepath,
            format=data.format,
            compressed=data.compressed,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
            home=data.home,
            created=data.created,
        )
        return file

    def create_dataset(self, data: Union[DTO, dict]) -> AbstractDataset:
        if isinstance(data, DTO):
            return self._create_dataset_from_dto(data)
        else:
            return self._create_dataset_from_dict(data)

    def _create_dataset_from_dto(self, data: DTO) -> AbstractDataset:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_dataset(data)

        # Folder's are assigned based upon a specified file organization
        folder = os.path.join(data.home, data.datasource, data.name, data.stage)

        dataset = LocalDataset(
            name=data.name,
            desc=data.desc,
            status=data.status,
            datasource=data.datasource,
            stage=data.stage,
            storage_type=data.storage_type,
            folder=folder,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
            home=data.home,
        )

        return dataset

    def _create_dataset_from_dict(self, data: dict) -> AbstractDataset:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_dataset(data)

        dataset = LocalDataset(
            name=data.name,
            desc=data.desc,
            status=data.status,
            datasource=data.datasource,
            stage=data.stage,
            storage_type=data.storage_type,
            folder=data.folder,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
            home=data.home,
        )

        return dataset

    def _validate_file(self, data: Union[DTO, dict]) -> DTO:
        data = super(LocalEntityFactory, self)._validate(data)
        try:
            data.stage = get_close_matches(data.stage, STAGES)[0]
            data.format = get_close_matches(data.format, FORMATS)[0]

        except IndexError as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)
        return data

    def _validate_dataset(self, data: Union[DTO, dict]) -> DTO:
        data = super(LocalEntityFactory, self)._validate(data)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                   S3 ENTITY FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class S3EntityFactory(AbstractFactory):
    """Creates local file and dataset objects"""

    def create_file(self, data: Union[DTO, dict]) -> AbstractFile:
        if isinstance(data, DTO):
            return self._create_file_from_dto(data)
        else:
            return self._create_file_from_dict(data)

    def _create_file_from_dto(self, data: DTO) -> AbstractFile:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_file(data)

        # If compressed, appstop .tar.gz to the object_key, if it is not already there.
        if data.compressed and "tar.gz" not in data.object_key:
            data.object_key = data.object_key + ".tar.gz"

        file = S3File(
            name=data.name,
            desc=data.desc,
            dataset=data.dataset,
            dataset_id=data.dataset_id,
            datasource=data.datasource,
            storage_type=data.storage_type,
            bucket=data.bucket,
            object_key=data.object_key,
            format=data.format,
            compressed=data.compressed,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
        )

        return file

    def _create_file_from_dict(self, data: dict) -> AbstractFile:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_file(data)

        file = S3File(
            name=data.name,
            desc=data.desc,
            dataset=data.dataset,
            dataset_id=data.dataset_id,
            datasource=data.datasource,
            storage_type=data.storage_type,
            bucket=data.bucket,
            object_key=data.object_key,
            format=data.format,
            compressed=data.compressed,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
        )

        return file

    def create_dataset(self, data: Union[DTO, dict]) -> AbstractDataset:
        if isinstance(data, DTO):
            return self._create_dataset_from_dto(data)
        else:
            return self._create_dataset_from_dict(data)

    def _create_dataset_from_dto(self, data: DTO) -> AbstractDataset:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_dataset(data)

        # Folder's are assigned based upon a specified file organization
        folder = os.path.join(data.datasource, data.name)

        dataset = S3Dataset(
            name=data.name,
            desc=data.desc,
            status=data.status,
            datasource=data.datasource,
            storage_type=data.storage_type,
            bucket=data.bucket,
            folder=folder,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
        )

        return dataset

    def _create_dataset_from_dict(self, data: dict) -> AbstractDataset:
        # Validate / convert to namespace / coerce parameters
        data = self._validate_dataset(data)

        dataset = S3Dataset(
            name=data.name,
            desc=data.desc,
            status=data.status,
            datasource=data.datasource,
            storage_type=data.storage_type,
            bucket=data.bucket,
            folder=data.folder,
            size=data.size,
            dag_id=data.dag_id,
            task_id=data.task_id,
        )

        return dataset

    def _validate_file(self, data: Union[DTO, dict]) -> DTO:
        data = super(LocalEntityFactory, self)._validate(data)
        try:
            data.format = get_close_matches(data.format, FORMATS)[0]

        except IndexError as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)
        return data

    def _validate_dataset(self, data: Union[DTO, dict]) -> DTO:
        data = super(LocalEntityFactory, self)._validate(data)
        return data
