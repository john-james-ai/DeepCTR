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
# Modified   : Wednesday May 25th 2022 10:33:52 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import os
from dataclasses import dataclass

from datetime import datetime
import logging
from difflib import get_close_matches

from deepctr.dal.dto import DTO
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
BUCKETS = ["deepctr"]
STAGES = ["raw", "staged", "interim", "clean", "processed", "extract", "archive"]
STATES = ["added", "modified", "deleted"]
DATASOURCES = ["alibaba", "avazu", "criteo"]
FORMATS = ["csv", "parquet", "pickle", "tar.gz"]
STORAGE_TYPES = ["local", "s3"]


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT FILE                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AbstractFile(ABC):
    """Defines the interface for File objects."""

    @abstractmethod
    def to_dict(self) -> dict:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                     LOCAL FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class LocalFile(AbstractFile):
    """Defines the file objects stored locally."""

    name: str
    dataset: str
    datasource: str
    stage: str
    format: str
    state: str = "added"
    size: int = 0
    compressed: bool = False
    filename: str = None
    filepath: str = None
    storage_type: str = "local"
    dag_id: int = None
    task_id: int = None
    home: str = "data"
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "dataset": self.dataset,
            "datasource": self.datasource,
            "stage": self.stage,
            "format": self.format,
            "state": self.state,
            "size": self.size,
            "compressed": self.compressed,
            "filename": self.filename,
            "filepath": self.filepath,
            "storage_type": self.storage_type,
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

    name: str
    dataset: str
    datasource: str
    stage: str
    format: str
    object_key: str
    bucket: str = "deepctr"
    state: str = "added"
    size: int = 0
    compressed: bool = True
    storage_type: str = "s3"
    dag_id: int = None
    task_id: int = None
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "dataset": self.dataset,
            "datasource": self.datasource,
            "stage": self.stage,
            "format": self.format,
            "object_key": self.object_key,
            "bucket": self.bucket,
            "state": self.state,
            "size": self.size,
            "compressed": self.compressed,
            "storage_type": self.storage_type,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT DATASET                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AbstractDataset(ABC):
    """Defines the interface for Dataset objects."""

    @abstractmethod
    def to_dict(self) -> dict:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                     LOCAL DATASET                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class LocalDataset(AbstractDataset):
    """Defines the file objects stored locally."""

    name: str
    datasource: str
    stage: str
    state: str = "added"
    size: int = 0
    folder: str = None
    storage_type: str = "local"
    dag_id: int = None
    home: str = "data"
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "stage": self.stage,
            "state": self.state,
            "size": self.size,
            "folder": self.folder,
            "storage_type": self.storage_type,
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

    name: str
    datasource: str
    stage: str
    folder: str
    bucket: str = "deepctr"
    state: str = "added"
    size: int = 0
    storage_type: str = "s3"
    dag_id: int = None
    created: datetime = datetime.now()

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "datasource": self.datasource,
            "stage": self.stage,
            "folder": self.folder,
            "bucket": self.bucket,
            "state": self.state,
            "size": self.size,
            "storage_type": self.storage_type,
            "dag_id": self.dag_id,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class AbstractFactory(ABC):
    """The interface defining the methods for returning the abstract File and Dataset products."""

    @abstractmethod
    def create_file(self, dto: DTO) -> AbstractFile:
        pass

    @abstractmethod
    def create_dataset(self, dto: DTO) -> AbstractDataset:
        pass

    def _validate(self, dto: DTO) -> DTO:
        """ Validates / coerces dto parameters and returns a valid DTO."""
        try:
            dto.state = get_close_matches(dto.state, STATES)[0]
            dto.stage = get_close_matches(dto.stage, STAGES)[0]
            dto.datasource = get_close_matches(dto.datasource, DATASOURCES)[0]
            dto.storage_type = get_close_matches(dto.storage_type, STORAGE_TYPES)[0]

        except IndexError as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)

        return dto


# ------------------------------------------------------------------------------------------------ #
#                                  LOCAL ENTITY FACTORY                                            #
# ------------------------------------------------------------------------------------------------ #
class LocalEntityFactory(AbstractFactory):
    """Creates local file and dataset objects"""

    def create_file(self, dto: DTO) -> AbstractFile:
        # Validate / coerce parameters
        dto = self._validate(dto)
        try:
            dto.format = get_close_matches(dto.format, FORMATS)[0]

        except IndexError as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)

        # Format the filename and filepath based parameters of the file.
        filename = dto.name + "." + dto.format
        if dto.compressed and "tar.gz" not in filename:
            filename = filename + ".tar.gz"

        filepath = os.path.join(dto.home, dto.datasource, dto.dataset, dto.stage, filename)

        file = LocalFile(
            name=dto.name,
            dataset=dto.dataset,
            datasource=dto.datasource,
            stage=dto.stage,
            format=dto.format,
            state=dto.state,
            size=dto.size,
            compressed=dto.compressed,
            filename=filename,
            filepath=filepath,
            storage_type=dto.storage_type,
            dag_id=dto.dag_id,
            task_id=dto.task_id,
            home=dto.home,
        )

        return file

    def create_dataset(self, dto: DTO) -> AbstractDataset:
        # Validate / coerce parameters
        dto = self._validate(dto)

        # Folder's are assigned based upon a specified file organization
        folder = os.path.join(dto.home, dto.datasource, dto.name, dto.stage)

        dataset = LocalDataset(
            name=dto.name,
            datasource=dto.datasource,
            stage=dto.stage,
            state=dto.state,
            size=dto.size,
            folder=folder,
            storage_type=dto.storage_type,
            dag_id=dto.dag_id,
            home=dto.home,
        )

        return dataset


# ------------------------------------------------------------------------------------------------ #
#                                   S3 ENTITY FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class S3EntityFactory(AbstractFactory):
    """Creates local file and dataset objects"""

    def create_file(self, dto: DTO) -> AbstractFile:
        # Validate / coerce parameters
        dto = self._validate(dto)
        try:
            dto.format = get_close_matches(dto.format, FORMATS)[0]

        except IndexError as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)

        # If compressed, append .tar.gz to the object_key, if it is not already there.
        if dto.compressed and "tar.gz" not in dto.object_key:
            dto.object_key = dto.object_key + ".tar.gz"

        file = S3File(
            name=dto.name,
            dataset=dto.dataset,
            datasource=dto.datasource,
            stage=dto.stage,
            format=dto.format,
            state=dto.state,
            size=dto.size,
            compressed=dto.compressed,
            bucket=dto.bucket,
            object_key=dto.object_key,
            storage_type=dto.storage_type,
            dag_id=dto.dag_id,
            task_id=dto.task_id,
        )

        return file

    def create_dataset(self, dto: DTO) -> AbstractDataset:
        # Validate / coerce parameters
        dto = self._validate(dto)

        # Folder's are assigned based upon a specified file organization
        folder = os.path.join(dto.datasource, dto.name)

        dataset = S3Dataset(
            name=dto.name,
            datasource=dto.datasource,
            stage=dto.stage,
            state=dto.state,
            size=dto.size,
            folder=folder,
            storage_type=dto.storage_type,
            dag_id=dto.dag_id,
        )

        return dataset
