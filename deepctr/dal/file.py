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
# Modified   : Tuesday June 28th 2022 12:18:16 pm                                                  #
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

from deepctr.dal.base import Entity, EntityMapper
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
        filename: str = None,
        compressed: bool = False,
        from_database=False,
    ) -> None:
        super(File).__init__(name=name, desc=desc, from_database=from_database)
        self._folder = folder
        self._format = format
        self._filename = filename
        self._compressed = compressed

        self._filepath = None
        self._size = 0
        self._file_created = None
        self._file_modified = None
        self._file_accessed = None
        self._io = None
        # Note: self._name, self._desc, self._created, self._modified and self._accessed are
        # initialized in the base class.

        self._set_io()
        self._set_filepath()
        self._set_size()
        self._set_file_dates()

    @property
    def folder(self) -> str:
        self._accessed_date()
        return self._folder

    @folder.setter
    def folder(self, folder) -> None:
        self._folder = folder
        self._update_dates()

    @property
    def format(self) -> str:
        self._accessed_date()
        return self._format

    @format.setter
    def format(self, format) -> None:
        self._format = format
        self._update_dates()
        self._set_io()

    @property
    def filename(self) -> str:
        self._accessed_date()
        return self._filename

    @filename.setter
    def filename(self, filename) -> None:
        self._filename = filename
        self._update_dates()

    @property
    def compressed(self) -> str:
        self._accessed_date()
        return self._compressed

    @compressed.setter
    def compressed(self, compressed) -> None:
        self._compressed = compressed
        self._update_dates()

    @property
    def filepath(self) -> str:
        self._accessed_date()
        return self._filepath

    @filepath.setter
    def filepath(self, filepath) -> None:
        if self._from_database:
            self._filepath = filepath
            self._update_dates()
        else:
            msg = "The filepath variable is immutable unless loading from database"
            logger.warning(msg)

    @property
    def size(self) -> str:
        self._accessed_date()
        return self._size

    @size.setter
    def size(self, size) -> None:
        if self._from_database:
            self._size = size
            self._update_dates()
        else:
            msg = "The size variable is immutable unless loading from database"
            logger.warning(msg)

    @property
    def file_created(self) -> str:
        self._accessed_date()
        return self._file_created

    @file_created.setter
    def file_created(self, file_created) -> None:
        if self._from_database:
            self._file_created = file_created
            self._update_dates()
        else:
            msg = "This setter is limited to database loading."
            logger.warning(msg)

    @property
    def file_modified(self) -> str:
        self._accessed_date()
        return self._file_modified

    @file_modified.setter
    def file_modified(self, file_modified) -> None:
        if self._from_database:
            self._file_modified = file_modified
            self._update_dates()
        else:
            msg = "This setter is limited to database loading."
            logger.warning(msg)

    @property
    def file_accessed(self) -> str:
        self._accessed_date()
        return self._file_accessed

    @file_accessed.setter
    def file_accessed(self, file_accessed) -> None:
        if self._from_database:
            self._file_accessed = file_accessed
            self._update_dates()
        else:
            msg = "This setter is limited to database loading."
            logger.warning(msg)

    def read(self) -> DataFrame:
        self._set_io()
        data = self._io.read(self._filepath)
        self._accessed_date()
        return data

    def write(self, data: DataFrame) -> None:
        self._set_io()
        self._io.write(data, self._filepath)
        self._update_dates()
        self._set_size()
        self._set_file_dates()

    def delete(self) -> None:
        shutil.rmtree(self._filepath, ignore_errors=True)

    def exists(self) -> bool:
        self._set_d
        exists = os.path.exists(self._filepath)
        self._accessed_date()
        return exists

    def _set_io(self) -> None:
        if not self._io:
            self._io = SparkCSV() if self._format == "csv" else SparkParquet()

    def _set_filepath(self) -> None:
        self._filename = self._filename if self._filename else self._name + "." + self._format
        self._filepath = os.path.join(self.folder, self._filename)

    def _set_size(self) -> None:
        self._size = 0 if not os.path.exists(self._filepath) else os.path.getsize(self._filepath)

    def _set_file_dates(self) -> None:
        if os.path.exists(self._filepath):
            result = os.stat(self._filepath)
            self._file_created = datetime.fromtimestamp(result.st_ctime)
            self._file_modified = datetime.fromtimestamp(result.st_mtime)
            self._file_accessed = datetime.fromtimestamp(result.st_atime)


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
            `compressed`, `size`, `file_created`, `file_modified`,`file_accessed`,
            `created`, `modified`,`accessed`)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s);
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
            self.entity.file_created,
            self.entity.file_modified,
            self.entity.file_accessed,
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
                                `file_created` = %s,
                                `file_modified` = %s,
                                `file_accessed` = %s,
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
            self.entity.file_created,
            self.entity.file_modified,
            self.entity.file_accessed,
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
            name=record["name"],
            desc=record["desc"],
            folder=record["folder"],
            format=record["format"],
            filename=record["filename"],
            compressed=record["compressed"],
            from_database=True,
        )
        file.id = record["id"]
        file.filepath = record["filepath"]
        file.size = record["size"]
        file.file_created = record["file_created"]
        file.file_modified = record["file_modified"]
        file.file_accessed = record["file_accessed"]
        file.created = record["created"]
        file.modified = record["modified"]
        file.accessed = record["accessed"]
        return file
