#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /file.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday June 28th 2022 04:54:40 am                                                  #
# Modified   : Tuesday June 28th 2022 08:02:23 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import shutil
import logging
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import DataFrame

from deepctr.dal.base import Entity, EntityMapper, Validator
from deepctr.data.local import SparkCSV, SparkParquet
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                         FILE                                                     #
# ------------------------------------------------------------------------------------------------ #
class File(Entity):
    """Defines a file object"""

    def __init__(
        self,
        name: str,
        desc: str,
        folder: str,
        format: str,
        id: int = 0,
        filename: str = None,
        compressed: bool = False,
        filepath: str = None,
        size: int = 0,
        created=None,
        modified=None,
        accessed=None,
    ) -> None:
        super(File, self).__init__(
            name=name, desc=desc, id=id, created=created, modified=modified, accessed=accessed
        )
        self._folder = folder
        self._format = format
        self._filename = filename
        self._compressed = compressed
        self._filepath = filepath
        self._size = size

        self._validate()
        self._set_filepath()
        if not created or not modified or not accessed:
            self._set_file_dates()

        if os.path.exists(self._filepath) and not size:
            self._size = os.path.getsize(self._filepath)

    @property
    def folder(self) -> str:
        return self._folder

    @property
    def format(self) -> str:
        return self._format

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def compressed(self) -> str:
        return self._compressed

    @property
    def filepath(self) -> str:
        return self._filepath

    @property
    def size(self) -> str:
        return self._size

    def read(self) -> DataFrame:
        io = self._get_io()
        data = io.read(self._filepath)
        self._accessed = datetime.now()
        self._set_file_dates()
        return data

    def write(self, data: DataFrame) -> None:
        io = self._get_io()
        io.write(data=data, filepath=self._filepath)
        self._accessed = datetime.now()
        self._set_size()
        self._set_file_dates()

    def delete(self) -> None:
        shutil.rmtree(self._filepath, ignore_errors=True)

    def exists(self) -> bool:
        exists = os.path.exists(self._filepath)
        self._accessed = datetime.now()
        self._set_file_dates()
        return exists

    def to_dict(self) -> dict:
        return {
            "id": self._id,
            "name": self._name,
            "desc": self._desc,
            "folder": self._folder,
            "format": self._format,
            "filename": self._filename,
            "compressed": True if self._compressed else False,
            "filepath": self._filepath,
            "size": self._size,
            "created": self._created,
            "modified": self._modified,
            "accessed": self._accessed,
        }

    def _validate(self) -> None:
        validate = Validator()
        validate.format(self._format)

    def _get_io(self) -> None:
        return SparkCSV() if "csv" in self._format else SparkParquet()

    def _set_filepath(self) -> None:
        if not self._filepath:
            self._filename = self._filename if self._filename else self._name + "." + self._format
            self._filepath = os.path.join(self.folder, self._filename)

    def _set_size(self) -> None:
        if os.path.exists(self._filepath):
            self._size = os.path.getsize(self._filepath)

    def _set_file_dates(self) -> None:
        if os.path.exists(self._filepath):
            result = os.stat(self._filepath)
            self._created = datetime.fromtimestamp(result.st_ctime)
            self._modified = datetime.fromtimestamp(result.st_mtime)
            self._accessed = datetime.fromtimestamp(result.st_atime)


# ------------------------------------------------------------------------------------------------ #
#                                         SEQUEL                                                   #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `file`
            (`name`, `desc`, `folder`, `format`, `filename`, `filepath`,
            `compressed`, `size`, `created`, `modified`,`accessed`)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.folder,
            self.entity.format,
            self.entity.filename,
            self.entity.filepath,
            self.entity.compressed,
            self.entity.size,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `file` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `file`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `file` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `file`
                            SET `name` = %s,
                                `desc` = %s,
                                `folder` = %s,
                                `format` = %s,
                                `filename` = %s,
                                `filepath` = %s,
                                `compressed` = %s,
                                `size` = %s,
                                `created` = %s,
                                `modified` = %s,
                                `accessed` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.folder,
            self.entity.format,
            self.entity.filename,
            self.entity.filepath,
            self.entity.compressed,
            self.entity.size,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
            self.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                        FILE MAPPER                                               #
# ------------------------------------------------------------------------------------------------ #
class FileMapper(EntityMapper):
    """Commands for the file table."""

    def insert(self, entity: Entity) -> FileInsert:
        return FileInsert(entity)

    def select(self, id: int) -> FileSelect:
        return FileSelect(parameters=(id,))

    def select_all(self) -> FileSelectAll:
        return FileSelectAll()

    def update(self, entity: Entity) -> FileUpdate:
        return FileUpdate(entity)

    def delete(self, id: int) -> FileDelete:
        return FileDelete(parameters=(id,))

    def factory(self, record: dict) -> Entity:
        file = File(
            id=record["id"],
            name=record["name"],
            desc=record["desc"],
            folder=record["folder"],
            format=record["format"],
            filename=record["filename"],
            compressed=True if record["compressed"] else False,
            filepath=record["filepath"],
            size=record["size"],
            created=record["created"],
            modified=record["modified"],
            accessed=record["accessed"],
        )

        return file
