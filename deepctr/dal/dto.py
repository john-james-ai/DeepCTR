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
# Modified   : Wednesday May 25th 2022 01:52:25 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #
#                                  FILE DATA TRANSFER OBJECTS                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DTO(ABC):
    name: str


@dataclass
class AbstractFileDTO(DTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    name: str
    dataset: str
    datasource: str
    stage: str
    format: str
    state: str
    size: int
    compressed: bool
    storage_type: str
    dag_id: int
    task_id: int


@dataclass
class LocalFileDTO(AbstractFileDTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    name: str
    dataset: str
    datasource: str
    stage: str
    format: str
    state: str
    size: int
    compressed: bool
    storage_type: str
    dag_id: int
    task_id: int
    home: str


@dataclass
class S3FileDTO(AbstractFileDTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    name: str
    dataset: str
    datasource: str
    format: str
    stage: str
    state: str
    size: int
    object_key: str
    bucket: str
    compressed: bool
    storage_type: str
    dag_id: int
    task_id: int


# ------------------------------------------------------------------------------------------------ #
#                                 DATASET DATA TRANSFER OBJECTS                                    #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class AbstractDatasetDTO(DTO):
    """File DTO for intra and inter layer transfer of parameter data."""

    name: str
    datasource: str
    stage: str
    state: str
    size: int
    storage_type: str
    dag_id: int


@dataclass
class LocalDatasetDTO(AbstractDatasetDTO):
    """Dataset DTO for intra and inter layer transfer of parameter data."""

    name: str
    datasource: str
    stage: str
    state: str
    size: int
    storage_type: str
    dag_id: int
    home: str


@dataclass
class S3FDatasetDTO(AbstractDatasetDTO):
    """Dataset DTO for intra and inter layer transfer of parameter data."""

    name: str
    datasource: str
    bucket: str
    folder: str
    stage: str
    state: str
    size: int
    storage_type: str
    dag_id: int
