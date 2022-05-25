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
# Modified   : Wednesday May 25th 2022 01:06:53 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module contains SQL command objects for each entity type."""
import logging
from dataclasses import dataclass
from deepctr.dal.entity import LocalFile, LocalDataset, S3File, S3Dataset
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                            FILE                                                  #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class LocalFileInsert:
    file: LocalFile
    statement: str = """
    INSERT INTO localfile
    (name, dataset, dataset_id, datasource, stage, format, size, compressed, filename, filepath,
    storage_type, dag_id, task_id, home, created)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.parameters = (
            self.file.name,
            self.file.dataset,
            self.file.dataset_id,
            self.file.datasource,
            self.file.stage,
            self.file.format,
            self.file.size,
            self.file.compressed,
            self.file.filename,
            self.file.filepath,
            self.file.storage_type,
            self.file.dag_id,
            self.file.task_id,
            self.file.home,
            self.file.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelect:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT * FROM `localfile` WHERE id=%s;""",
            "name": """SELECT * FROM `localfile` WHERE name=%s;""",
            "dataset": """SELECT * FROM `localfile` WHERE dataset=%s;""",
            "datasource": """SELECT * FROM `localfile` WHERE datasource=%s;""",
            "stage": """SELECT * FROM `localfile` WHERE stage =%s;""",
            "storage_type": """SELECT * FROM `localfile` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectAll:
    column: str = None
    parameters: tuple = None
    statement: str = """SELECT * FROM `localfile`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileExists:
    file: LocalFile
    statement: str
    parameters: tuple

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `localfile` WHERE name=%s AND
                                                                   dataset=%s AND
                                                                   datasource=%s AND
                                                                   stage =%s);"""
        self.parameters = (self.file.name, self.file.dataset, self.file.datasource, self.file.stage)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileDelete:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """DELETE FROM `localfile` WHERE id=%s;""",
            "name": """DELETE FROM `localfile` WHERE name=%s;""",
            "dataset": """DELETE FROM `localfile` WHERE dataset=%s;""",
            "datasource": """DELETE FROM `localfile` WHERE datasource=%s;""",
            "stage": """DELETE FROM `localfile` WHERE stage =%s;""",
            "storage_type": """DELETE FROM `localfile` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
#                                           S3 FILE                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3FileInsert:
    file: S3File
    statement: str = """
    INSERT INTO s3file
    (name, dataset, dataset_id, datasource, stage, format, object_key, bucket, size, compressed,
    storage_type, dag_id, task_id, created)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.parameters = (
            self.file.name,
            self.file.dataset,
            self.file.dataset_id,
            self.file.datasource,
            self.file.stage,
            self.file.format,
            self.file.object_key,
            self.file.bucket,
            self.file.size,
            self.file.compressed,
            self.file.storage_type,
            self.file.dag_id,
            self.file.task_id,
            self.file.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelect:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT * FROM `s3file` WHERE id=%s;""",
            "name": """SELECT * FROM `s3file` WHERE name=%s;""",
            "dataset": """SELECT * FROM `s3file` WHERE dataset=%s;""",
            "datasource": """SELECT * FROM `s3file` WHERE datasource=%s;""",
            "stage": """SELECT * FROM `s3file` WHERE stage =%s;""",
            "storage_type": """SELECT * FROM `s3file` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectAll:
    column: str = None
    parameters: tuple = None
    statement: str = """SELECT * FROM `s3file`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileExists:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT EXISTS(SELECT * FROM `s3file` WHERE id=%s);""",
            "name": """SELECT EXISTS(SELECT * FROM `s3file` WHERE name=%s);""",
            "dataset": """SELECT EXISTS(SELECT * FROM `s3file` WHERE dataset=%s);""",
            "datasource": """SELECT EXISTS(SELECT * FROM `s3file` WHERE datasource=%s);""",
            "stage": """SELECT EXISTS(SELECT * FROM `s3file` WHERE stage =%s);""",
            "storage_type": """SELECT EXISTS(SELECT * FROM `s3file` WHERE storage_type=%s);""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileDelete:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """DELETE FROM `s3file` WHERE id=%s;""",
            "name": """DELETE FROM `s3file` WHERE name=%s;""",
            "dataset": """DELETE FROM `s3file` WHERE dataset=%s;""",
            "datasource": """DELETE FROM `s3file` WHERE datasource=%s;""",
            "stage": """DELETE FROM `s3file` WHERE stage =%s;""",
            "storage_type": """DELETE FROM `s3file` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
#                                       LOCAL DATASET                                              #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class LocalDatasetInsert:
    dataset: LocalDataset
    statement: str = """
    INSERT INTO localdataset
    (name, datasource, stage, size, folder, storage_type, dag_id, home, created)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,);
    """
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.parameters = (
            self.dataset.name,
            self.dataset.datasource,
            self.dataset.stage,
            self.dataset.size,
            self.dataset.folder,
            self.dataset.storage_type,
            self.dataset.dag_id,
            self.dataset.home,
            self.dataset.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelect:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT * FROM `localdataset` WHERE id=%s;""",
            "name": """SELECT * FROM `localdataset` WHERE name=%s;""",
            "datasource": """SELECT * FROM `localdataset` WHERE datasource=%s;""",
            "stage": """SELECT * FROM `localdataset` WHERE stage =%s;""",
            "storage_type": """SELECT * FROM `localdataset` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectAll:
    column: str = None
    parameters: tuple = None
    statement: str = """SELECT * FROM `localdataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetDelete:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """DELETE FROM `localdataset` WHERE id=%s;""",
            "name": """DELETE FROM `localdataset` WHERE name=%s;""",
            "datasource": """DELETE FROM `localdataset` WHERE datasource=%s;""",
            "stage": """DELETE FROM `localdataset` WHERE stage =%s;""",
            "storage_type": """DELETE FROM `localdataset` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetExists:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT EXISTS(SELECT * FROM `localdataset` WHERE id=%s);""",
            "name": """SELECT EXISTS(SELECT * FROM `localdataset` WHERE name=%s);""",
            "datasource": """SELECT EXISTS(SELECT * FROM `localdataset` WHERE datasource=%s);""",
            "stage": """SELECT EXISTS(SELECT * FROM `localdataset` WHERE stage =%s);""",
            "storage_type": """SELECT EXISTS(SELECT * FROM `localdataset` WHERE storage_type=%s);""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
#                                       S3 DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3DatasetInsert:
    dataset: S3Dataset
    statement: str = """
    INSERT INTO s3dataset
    (name, datasource, stage, folder, bucket, size, storage_type, dag_id, created)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.parameters = (
            self.dataset.name,
            self.dataset.datasource,
            self.dataset.stage,
            self.dataset.folder,
            self.dataset.bucket,
            self.dataset.size,
            self.dataset.storage_type,
            self.dataset.dag_id,
            self.dataset.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelect:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT * FROM `s3dataset` WHERE id=%s;""",
            "name": """SELECT * FROM `s3dataset` WHERE name=%s;""",
            "datasource": """SELECT * FROM `s3dataset` WHERE datasource=%s;""",
            "stage": """SELECT * FROM `s3dataset` WHERE stage =%s;""",
            "storage_type": """SELECT * FROM `s3dataset` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelectAll:
    column: str = None
    parameters: tuple = None
    statement: str = """SELECT * FROM `s3dataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetDelete:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """DELETE FROM `s3dataset` WHERE id=%s;""",
            "name": """DELETE FROM `s3dataset` WHERE name=%s;""",
            "datasource": """DELETE FROM `s3dataset` WHERE datasource=%s;""",
            "stage": """DELETE FROM `s3dataset` WHERE stage =%s;""",
            "storage_type": """DELETE FROM `s3dataset` WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetExists:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT EXISTS(SELECT * FROM `s3dataset` WHERE id=%s);""",
            "name": """SELECT EXISTS(SELECT * FROM `s3dataset` WHERE name=%s);""",
            "datasource": """SELECT EXISTS(SELECT * FROM `s3dataset` WHERE datasource=%s);""",
            "stage": """SELECT EXISTS(SELECT * FROM `s3dataset` WHERE stage =%s);""",
            "storage_type": """SELECT EXISTS(SELECT * FROM `s3dataset` WHERE storage_type=%s);""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)
