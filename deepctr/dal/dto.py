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
# Modified   : Monday May 23rd 2022 06:36:28 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from datetime import datetime

# ------------------------------------------------------------------------------------------------ #
#                                  DATA TRANSFER OBJECTS                                           #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileDTO:
    """File DTO for intra and inter layer transfer of file data."""

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
    storage_type: str = "local"
    dag_id: int = 0
    task_id: int = 0
    dataset_id: int = 0
    created: datetime = None


@dataclass
class DatasetDTO:
    """Dataset DTO for intra and inter layer transfer of dataset data."""

    name: str
    stage: str
    datasource: str
    folder: str = None
    size: int = 0
    bucket: str = None
    home: str = "data"
    state: str = "added"
    storage_type: str = "local"
    dag_id: int = 0
    created: datetime = None
