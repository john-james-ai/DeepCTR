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
# Modified   : Monday May 23rd 2022 07:53:02 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging
from dataclasses import dataclass
from deepctr.dal.entity import File, Dataset
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                            FILE                                                  #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileInsert:
    file: File
    statement: str = """
    INSERT INTO file
    (name, dataset, dataset_id, datasource, stage, storage_type, filename, filepath, format,
    compressed, bucket, object_key, size, task_id, created)
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
            self.file.storage_type,
            self.file.self.filename,
            self.file.self.filepath,
            self.file.format,
            self.file.compressed,
            self.file.bucket,
            self.file.object_key,
            self.file.size,
            self.file.task_id,
            self.file.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSelect:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT * FROM file WHERE id=%s;""",
            "name": """SELECT * FROM file WHERE name=%s;""",
            "dataset": """SELECT * FROM file WHERE dataset=%s;""",
            "datasource": """SELECT * FROM file WHERE datasource=%s;""",
            "stage": """SELECT * FROM file WHERE stage =%s;""",
            "storage_type": """SELECT * FROM file WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSelectAll:
    column: str = None
    parameters: tuple = None
    statement: str = """SELECT * FROM file;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileExists:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT EXISTS(SELECT * FROM file WHERE id=%s);""",
            "name": """SELECT EXISTS(SELECT * FROM file WHERE name=%s);""",
            "dataset": """SELECT EXISTS(SELECT * FROM file WHERE dataset=%s);""",
            "datasource": """SELECT EXISTS(SELECT * FROM file WHERE datasource=%s);""",
            "stage": """SELECT EXISTS(SELECT * FROM file WHERE stage =%s);""",
            "storage_type": """SELECT EXISTS(SELECT * FROM file WHERE storage_type=%s);""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDelete:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """DELETE FROM file WHERE id=%s;""",
            "name": """DELETE FROM file WHERE name=%s;""",
            "dataset": """DELETE FROM file WHERE dataset=%s;""",
            "datasource": """DELETE FROM file WHERE datasource=%s;""",
            "stage": """DELETE FROM file WHERE stage =%s;""",
            "storage_type": """DELETE FROM file WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
#                                          DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetInsert:
    dataset: Dataset
    statement: str = """
    INSERT INTO dataset
    (name, stage, datasource, storage_type, folder, bucket, size, dag_id, created)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,);
    """
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.parameters = (
            self.dataset.name,
            self.dataset.stage,
            self.dataset.datasource,
            self.dataset.storage_type,
            self.dataset.folder,
            self.dataset.bucket,
            self.dataset.size,
            self.dataset.dag_id,
            self.dataset.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelect:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT * FROM dataset WHERE id=%s;""",
            "name": """SELECT * FROM dataset WHERE name=%s;""",
            "datasource": """SELECT * FROM dataset WHERE datasource=%s;""",
            "stage": """SELECT * FROM dataset WHERE stage =%s;""",
            "storage_type": """SELECT * FROM dataset WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetSelectAll:
    column: str = None
    parameters: tuple = None
    statement: str = """SELECT * FROM dataset;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDelete:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """DELETE FROM dataset WHERE id=%s;""",
            "name": """DELETE FROM dataset WHERE name=%s;""",
            "datasource": """DELETE FROM dataset WHERE datasource=%s;""",
            "stage": """DELETE FROM dataset WHERE stage =%s;""",
            "storage_type": """DELETE FROM dataset WHERE storage_type=%s;""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetExists:
    column: str
    parameters: tuple
    statement: str

    def __post_init__(self) -> None:
        d = {
            "id": """SELECT EXISTS(SELECT * FROM dataset WHERE id=%s);""",
            "name": """SELECT EXISTS(SELECT * FROM dataset WHERE name=%s);""",
            "datasource": """SELECT EXISTS(SELECT * FROM dataset WHERE datasource=%s);""",
            "stage": """SELECT EXISTS(SELECT * FROM dataset WHERE stage =%s);""",
            "storage_type": """SELECT EXISTS(SELECT * FROM dataset WHERE storage_type=%s);""",
        }
        try:
            self.statement = d[self.column]
        except KeyError as e:
            logger.error("{} is not a valid search column.".format(self.by))
            raise ValueError(e)
