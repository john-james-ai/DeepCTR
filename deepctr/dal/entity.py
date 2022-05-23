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
# Modified   : Thursday May 19th 2022 06:43:35 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from dataclasses import dataclass, field
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
DATASOURCES = ["alibaba", "avazu", "criteo"]
FORMATS = ["csv", "parquet", "pickle", "tar.gz"]
STORAGE_TYPES = ["local", "s3"]


# ------------------------------------------------------------------------------------------------ #
#                                    ENTITY CLASSES                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class Entity(ABC):
    """Base class for Entities corresponding to tables in the Database."""

    @abstractmethod
    def __post_init__(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                     FILE ENTITY                                                  #
# ------------------------------------------------------------------------------------------------ #


@dataclass(frozen=True)
class File(Entity):
    """Base class for metadata entities."""

    name: str
    dataset: str
    datasource: str
    stage: str
    format: str
    size: int = 0
    filename: str = None
    filepath: str = None
    compressed: bool = False
    bucket: str = None
    object_key: str = None
    home: str = "data"
    state: str = "added"
    storage_type: str = field(default=STORAGE_TYPES[0])
    dag_id: int = 0
    task_id: int = 0
    created: datetime = None

    def __post_init__(self) -> None:
        try:
            self.datasource = get_close_matches(self.datasource, DATASOURCES)[0]
            self.stage = get_close_matches(self.stage, STAGES)[0]
            self.format = get_close_matches(self.format, FORMATS)[0]
            self.storage_type = get_close_matches(self.format, STORAGE_TYPES)[0]
            self.filename = self.name + self.format
            self.filepath = FilePathFinder.get_path(
                name=self.name,
                datasource=self.datasource,
                dataset=self.dataset,
                stage=self.stage,
                format=self.format,
                home=self.home,
            )
            if os.path.exists(self.filepath):
                self.size = os.path.getsize(self.filepath)
        except IndexError() as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)


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
    bucket: str = None
    home: str = "data"
    state: str = "added"
    storage_type: str = field(default=STORAGE_TYPES[0])
    files = {}

    def __post_init__(self) -> None:
        try:
            self.datasource = get_close_matches(self.datasource, DATASOURCES)[0]
            self.stage = get_close_matches(self.stage, STAGES)[0]
            self.storage_type = get_close_matches(self.format, STORAGE_TYPES)[0]
            self.folder = DatasetPathFinder.get_path(
                name=self.name, datasource=self.datasource, stage=self.stage, home=self.home,
            )
        except IndexError() as e:
            logging.error("Invalid File parameters.\n{}".format(e))
            raise ValueError(e)

    def add_file(self, file: File) -> None:
        self.files[file.name] = file
