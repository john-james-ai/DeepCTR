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
# Created    : Monday June 20th 2022 02:23:53 am                                                   #
# Modified   : Wednesday June 22nd 2022 05:02:43 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataclass entities that encapsulate Task, DAG, and File info for database representation."""
from abc import ABC
from dataclasses import dataclass
from datetime import datetime

# ------------------------------------------------------------------------------------------------ #


@dataclass
class Entity(ABC):
    name: str


@dataclass
class DagEntity(Entity):
    """Instantiated in the start method from instance variables"""

    desc: str
    n_tasks: int
    n_tasks_done: int = 0  # Incremented in the after each task is successfully completed.
    created: datetime = None  # Set at start of dag
    modified: datetime = None  # Updated each time the Entity changes
    started: datetime = None  # Set at start of dag
    stopped: datetime = None  # Stopped is updated when the dag completes
    duration: int = 0  # Computed upon dag completion
    return_code: int = 1  # Set to 0 in the stopped method of the dag.
    id: int = 0  # Defaults to 0 and updated on first insert into DB


@dataclass
class TaskEntity(Entity):
    """Instantiated in the start method from instance variables"""

    desc: str
    seq: int  # Created from operator object instance variable
    dag_id: int  # From the context object.
    created: datetime  # Set at start of dag
    modified: datetime = None  # Updated each time the Entity changes
    started: datetime = None  # Set at start of dag
    stopped: datetime = None  # Stopped is updated when the dag completes
    duration: int = 0  # Computed upon dag completion
    return_code: int = 1  # Set to 0 in the stopped method of the dag.
    id: int = 0  # Defaults to 0 and updated on first insert into DB


@dataclass
class S3FileEntity(Entity):
    """Instantiated in the start method from instance variables"""

    source: str  # External source of data, i.e. alibaba, avazu, etc...
    dataset: str  # Short name for dataset, i.e vesuvio
    stage_id: int  # Integer for the data processing stage
    stage_name: str  # Name for the stage
    bucket: str  # S3 Bucket name
    object_key: str  # Path to resource within bucket
    format: str  # Underlying format of data, either csv or parquet
    compressed: bool = True  # True if compressed, false otherwise
    size: int = 0  # Size of resource on aws
    created: datetime = None  # Set at start of dag
    id: int = 0  # Defaults to 0 and updated on first insert into DB


@dataclass
class FileEntity(Entity):
    """Instantiated in the start method from instance variables"""

    source: str  # External source of data, i.e. alibaba, avazu, etc...
    dataset: str  # Short name for dataset, i.e vesuvio
    stage_id: int  # Integer for the data processing stage
    stage_name: str  # Name for the stage
    format: str  # Underlying format of data, either csv or parquet
    filepath: str = None  # Path to the file on disk
    compressed: bool = False  # True if compressed, false otherwise
    size: int = 0  # Size of resource on aws
    created: datetime = None  # Set at start of dag
    id: int = 0  # Defaults to 0 and updated on first insert into DB
