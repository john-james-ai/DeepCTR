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
# Modified   : Tuesday May 24th 2022 12:31:50 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from dataclasses import dataclass
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from difflib import get_close_matches

from deepctr.dal.base import DatasetPathFinder, FilePathFinder
from deepctr.utils.config import LOG_CONFIG

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
#                                    ENTITY CLASSES                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class Entity(ABC):
    """Base class for File classes including the members and validation common to all subclasses."""

    name: str
    state: str = "added"
    created: datetime = datetime.now()
    id: int = 0  # Have the data access object update the id


# ------------------------------------------------------------------------------------------------ #
#                                    FILE ENTITY                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class File(Entity):
    """Base class for File classes including the members and validation common to all subclasses."""

    name: str
    dataset: str
    datasource: str
    stage: str
    format: str
    state: str = "added"
    filename: str = None
    created: datetime = datetime.now()

    @abstractmethod
    def __post_init__(self) -> None:
        try:
            self.datasource = get_close_matches(self.datasource, DATASOURCES)[0]
            self.stage = get_close_matches(self.stage, STAGES)[0]
            self.format = get_close_matches(self.format, FORMATS)[0]
            self.state = get_close_matches(self.state, STATES)[0]

        except IndexError() as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)

        self.filename = self.name + "." + self.format


# ------------------------------------------------------------------------------------------------ #
#                                  LOCAL FILE ENTITY                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass()
class LocalFile(File):
    """Local file metadata entity."""

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
    folder: str = None
    bucket: str = None
    object_key: str = None
    storage_type: str = "local"
    dag_id: int = None
    task_id: int = None
    home: str = "data"

    def __post_init__(self) -> None:
        super(LocalFile).__post_init__(self)
        try:
            self.storage_type = get_close_matches(self.format, STORAGE_TYPES)[0]

        except IndexError() as e:
            logging.error("Invalid local File parameters.\n{}".format(e))
            raise ValueError(e)

        self.filepath = FilePathFinder.get_path(
            name=self.name,
            datasource=self.datasource,
            dataset=self.dataset,
            stage=self.stage,
            format=self.format,
            home=self.home,
        )
        self.folder = os.path.dirname(self.filepath)
        if os.path.exists(self.filepath):
            self.size = os.path.getsize(self.filepath)


# ------------------------------------------------------------------------------------------------ #
#                                     S3 FILE ENTITY                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass()
class S3File(File):
    """S3 file metadata entity."""

    name: str
    dataset: str
    datasource: str
    stage: str
    format: str
    bucket: str
    object_key: str
    state: str = "added"
    size: int = 0
    compressed: bool = True
    filename: str = None
    folder: str = None
    storage_type: str = "s3"
    dag_id: int = None
    task_id: int = None
    home: str = None

    def __post_init__(self) -> None:
        super(LocalFile).__post_init__(self)
        try:
            self.bucket = get_close_matches(self.bucket, BUCKETS)[0]
            self.storage_type = get_close_matches(self.format, STORAGE_TYPES)[0]

        except IndexError() as e:
            logging.error("Invalid local File parameters.\n{}".format(e))
            raise ValueError(e)

        self.folder = os.path.dirname(self.object_key)


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET ENTITY                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass()
class Dataset(Entity):
    """Dataset Entity."""

    name: str
    stage: str
    datasource: str
    folder: str = None
    size: int = 0
    state: str = "added"
    dag_id: int = 0
    files = {}

    def __post_init__(self) -> None:
        try:
            self.stage = get_close_matches(self.stage, STAGES)[0]
            self.datasource = get_close_matches(self.datasource, DATASOURCES)[0]

        except IndexError() as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)

    def add_file(self, file: File) -> None:
        self.files[file.id] = file

    def get_file(self, id: int) -> File:
        try:
            return self.files[id]
        except KeyError as e:
            logger.error("File id {} not found in dataset {}:{}.".format(str(id), str(self.id)))
