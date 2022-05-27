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
# Modified   : Thursday May 26th 2022 09:32:09 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module contains SQL command objects for each entity type."""
import logging
from dataclasses import dataclass
from typing import Any
from deepctr.dal.entity import LocalFile, LocalDataset, S3File, S3Dataset
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                            FILE                                                  #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class LocalFileInsert:
    entity: LocalFile
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO localfile
            (name, dataset, dataset_id, datasource, stage, storage_type, filename, filepath, format,
            compressed, size, dag_id, task_id, home, created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.dataset,
            self.entity.dataset_id,
            self.entity.datasource,
            self.entity.stage,
            self.entity.storage_type,
            self.entity.filename,
            self.entity.filepath,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.dag_id,
            self.entity.task_id,
            self.entity.home,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectOne:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.parameters = self.value
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
class LocalFileSelectByKey:
    name: str
    dataset: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile` WHERE name=%s AND
                                                            dataset=%s AND
                                                            datasource=%s AND
                                                            stage =%s;"""
        self.parameters(self.name, self.dataset, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localfile`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileDelete:
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `localfile` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalFileExists:
    name: str
    dataset: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `localfile` WHERE name=%s AND
                                                                   dataset=%s AND
                                                                   datasource=%s AND
                                                                   stage =%s);"""
        self.parameters = (self.name, self.dataset, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
#                                           S3 FILE                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3FileInsert:
    entity: S3File
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
                INSERT INTO `s3file`
                (name, dataset, dataset_id, datasource, storage_type, bucket, object_key, format, compressed,
                size, dag_id, task_id, created)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        self.parameters = (
            self.entity.name,
            self.entity.dataset,
            self.entity.dataset_id,
            self.entity.datasource,
            self.entity.storage_type,
            self.entity.bucket,
            self.entity.object_key,
            self.entity.format,
            self.entity.compressed,
            self.entity.size,
            self.entity.dag_id,
            self.entity.task_id,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectOne:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.parameters = self.value
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
class S3FileSelectByKey:
    name: str
    dataset: str
    datasource: str
    bucket: str = "deepctr"
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file` WHERE name=%s AND
                                                            dataset=%s AND
                                                            datasource=%s AND
                                                            bucket =%s;"""
        self.parameters(self.name, self.dataset, self.datasource, self.bucket)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileExists:
    name: str
    dataset: str
    datasource: str
    statement: str = None
    parameters: tuple = None
    bucket: str = "deepctr"

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `s3file` WHERE name=%s AND
                                                                   dataset=%s AND
                                                                   datasource=%s AND
                                                                   bucket =%s);"""
        self.parameters = (self.name, self.dataset, self.datasource, self.bucket)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3FileDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `s3file` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
#                                       LOCAL DATASET                                              #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class LocalDatasetInsert:
    entity: LocalDataset
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
                    INSERT INTO localdataset
                    (name, datasource, stage, storage_type, folder, size, dag_id, home, created)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
        self.parameters = (
            self.entity.name,
            self.entity.datasource,
            self.entity.stage,
            self.entity.storage_type,
            self.entity.folder,
            self.entity.size,
            self.entity.dag_id,
            self.entity.home,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectOne:
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localdataset` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.parameters = self.value
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
class LocalDatasetSelectByKey:
    name: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localdataset` WHERE name=%s AND
                                                            datasource=%s AND
                                                            stage =%s;"""
        self.parameters(self.name, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `localdataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetDelete:
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `localdataset` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LocalDatasetExists:
    name: str
    datasource: str
    stage: str
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `localfile` WHERE name=%s AND
                                                                   datasource=%s AND
                                                                   stage =%s);"""
        self.parameters = (self.name, self.datasource, self.stage)


# ------------------------------------------------------------------------------------------------ #
#                                       S3 DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class S3DatasetInsert:
    entity: S3Dataset
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
                INSERT INTO s3dataset
                (name, datasource, storage_type, bucket, folder, size, dag_id, created)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
        self.parameters = (
            self.entity.name,
            self.entity.datasource,
            self.entity.storage_type,
            self.entity.bucket,
            self.entity.folder,
            self.entity.size,
            self.entity.dag_id,
            self.entity.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelectOne:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3dataset` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelectByColumn:
    column: str
    value: Any
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.parameters = self.value
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


@dataclass
class S3DatasetSelectByKey:
    name: str
    datasource: str
    bucket: str = "deepctr"
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3file` WHERE name=%s AND
                                                            datasource=%s AND
                                                            bucket =%s;"""
        self.parameters(self.name, self.datasource, self.bucket)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `s3dataset`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `s3dataset` WHERE id=%s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class S3DatasetExists:
    name: str
    datasource: str
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """SELECT EXISTS(SELECT * FROM `s3dataset` WHERE name=%s AND
                                                                   datasource=%s);"""
        self.parameters = (self.name, self.datasource)
