#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /dto.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 19th 2022 06:43:34 pm                                                  #
# Modified   : Saturday May 28th 2022 04:19:18 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC
from datetime import datetime
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #
#                                  FILE DATA TRANSFER OBJECTS                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DTO(ABC):
    name: str


# ------------------------------------------------------------------------------------------------ #
#                                     DAG TRANSFER OBJECTS                                         #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DagDTO(DTO):
    """DTO for directed acyclic graphs."""

    seq: int
    desc: str
    created: datetime
    start: datetime = None
    stop: datetime = None
    duration: int = None
    id: int = 0


# ------------------------------------------------------------------------------------------------ #
#                                     TASK TRANSFER OBJECTS                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDTO(DTO):
    """DTO for tasks within directed acyclic graphs."""

    seq: int
    desc: str
    dag_id: int
    created: datetime
    start: datetime = None
    stop: datetime = None
    duration: int = None
    id: int = 0


# ------------------------------------------------------------------------------------------------ #
#                                  FILE DATA TRANSFER OBJECTS                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AbstractFileDTO(DTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    dataset: str
    dataset_id: int
    datasource: str
    format: str
    size: int
    compressed: bool
    storage_type: str
    dag_id: int
    task_id: int


@dataclass
class LocalFileDTO(AbstractFileDTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    stage: str
    home: str


@dataclass
class S3FileDTO(AbstractFileDTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    object_key: str
    bucket: str


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET DATA TRANSFER OBJECTS                                    #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class AbstractDatasetDTO(DTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    datasource: str
    storage_type: str
    size: int
    dag_id: int


@dataclass
class LocalDatasetDTO(AbstractDatasetDTO):
    """Dataset DTO for intra and inter layer transfer of parameter data."""

    stage: str
    home: str


@dataclass
class S3FDatasetDTO(AbstractDatasetDTO):
    """Dataset DTO for intra and inter layer transfer of parameter data."""

    folder: str
    bucket: str
