#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /fao.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Wednesday June 22nd 2022 01:24:33 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module defines the API for data access and management."""
from abc import ABC, abstractmethod
import os
from dataclasses import dataclass, field
from datetime import datetime
import inspect
import logging
import logging.config
from typing import Any, Union
import shutil

from deepctr.dal import STAGES, FORMATS, STORAGE_TYPES, SOURCES, IO
from deepctr.utils.aws import get_size_aws
from deepctr.data.local import SparkCSV, SparkParquet, Pickler
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                        FILE                                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class File:
    """Defines the interface for File objects."""

    name: str  # Name of file without the extension, i.e. user, profile, behavior, ad
    source: str  # The name as the dataset is externally recognized. i.e. alibaba
    dataset: str  # The collection to which the file belongs
    format: str  # The uncompressed format of the data, i.e. csv, parquet
    stage_id: int  # The stage identifier. See lab_book.md for stage_ids
    stage_name: str = None  # Associated with stage_id. See STAGES in deepctr/dal/base
    storage_type: str = None  # Either 'local' or 's3'
    bucket: str = None  # The bucket containing the file. Only relevant to s3 storage_type.
    filepath: str = None  # Path to the file. Synonymous with object_key for s3.
    compressed: bool = False  # Indicates if the file is compressed
    size: int = 0  # The size of the file in bytes
    id: int = 0  # The id assigned by the database
    home: str = "data"  # The home directory for all data. Can be overidden for testing.
    created: datetime = datetime.now()  # Should be overwritten if file exists.

    def __post_init__(self) -> None:
        self._validate()
        self._set_size()
        self._set_stage_name()
        self._set_filepath()

    def _validate(self) -> None:
        validate = Validator()
        self.source = validate.source(self.source)
        self.format = validate.format(self.format)
        self.stage_id = validate.stage(self.stage_id)
        self.storage_type = validate.storage_type(self.storage_type)
        if self.storage_type == "s3":
            self.filepath = validate.filepath(self.filepath)

    def _set_size(self) -> None:
        if self.storage_type == "local":
            if self.filepath:
                if os.path.exists(self.filepath):
                    self.size = os.path.getsize(self.filepath)
        else:
            self.size = get_size_aws(bucket=self.bucket, object_key=self.filepath)

    def _set_stage_name(self) -> None:
        self.stage_name = STAGES.get(self.stage_id)

    def _set_filepath(self) -> None:
        if self.storage_type == "local":
            stage_name = str(self.stage_id) + "_" + STAGES.get(self.stage_id)
            if not self.filepath:
                self.filename = (self.name + "." + self.format).replace(" ", "").lower()
                self.folder = os.path.join(self.home, self.source, self.dataset, stage_name)
                self.filepath = os.path.join(self.folder, self.filename)


# ------------------------------------------------------------------------------------------------ #
#                                          DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class Dataset:
    """Defines the parameters of local datasets to which the Files belong"""

    name: str  # The name of the dataset, i.e. vesuvio
    source: str  # The external data source
    stage_id: int  # The data processing stage number
    storage_type: str  # Where the data are stored. Either 's3', or 'local'
    folder: str  # Folder on hard drive or within an s3 bucket.
    bucket: str = None  # Required for s3 datasets

    files: list = field(default_factory=list)  # List of file objects

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        validate = Validator()
        self.source = validate.source(self.source)
        self.storage_type = validate.storage_type(self.storage_type)
        self.stage_id = validate.stage(self.stage_id)

    def add_file(self, file: File) -> None:
        self.files.append(file)


# ------------------------------------------------------------------------------------------------ #
#                                     LOCAL FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class LocalFile(File):
    """Defines the file objects stored locally."""

    def __post_init__(self) -> None:
        self._validate()  # See base class
        self._set_filepath()
        self._set_size()

    def _set_filepath(self) -> None:
        stage = str(self.stage_id) + "_" + STAGES.get(self.stage_id)
        if not self.filepath:
            self.filename = (self.name + "." + self.format).replace(" ", "").lower()
            self.folder = os.path.join(self.home, self.source, self.dataset, stage)
            self.filepath = os.path.join(self.folder, self.filename)

    def _set_size(self) -> None:
        # Get size from the file if filepath isn't None and the file is present
        if not self.size:
            if self.filepath:
                if os.path.exists(self.filepath):
                    self.size = os.path.getsize(self.filepath)

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "source": self.source,
            "dataset": self.dataset,
            "stage_id": self.stage_id,
            "filepath": self.filepath,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                        S3 FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class S3File(File):
    """Defines the S3 file objects."""

    bucket: str  # The bucket into which the file is stored.
    object_key: str  # The path to the resource within the bucket.
    compressed: bool = True  # Indicates if the file is compressed
    size: int = 0  # The size of the file in bytes
    id: int = 0  # Assigned by the database. A zero value indicates the id has not be assigned.
    created: datetime = datetime.now()

    def __post_init__(self) -> None:
        self._validate()  # See base class
        self._set_size()

    def _set_size(self) -> None:
        self.size = get_size_aws(bucket=self.bucket, object_key=self.object_key)

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "source": self.source,
            "dataset": self.dataset,
            "stage_id": self.stage_id,
            "bucket": self.bucket,
            "object_key": self.object_key,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "created": self.created,
        }
        return d


# ================================================================================================ #
#                                            FAO                                                   #
# ================================================================================================ #


class FAOBase(ABC):
    """Base class for file managers."""

    __io = {"csv": SparkCSV, "parquet": SparkParquet, "pickle": Pickler}

    @abstractmethod
    def create(self, file: File, data: Any, force: bool = False) -> None:
        pass
        """Persists a new data table to storage.

        Args:
            file (File): File object
        """
        pass

    @abstractmethod
    def read(self, file: File) -> Any:
        """Obtains an object from persisted storage

        Args:
            file (File): Parameter object for file read operations

        Returns (DataFrame)
        """
        pass

    @abstractmethod
    def delete(self, file: File) -> None:
        """Removes a data table from persisted storage

        Args:
            file (File): Parameter object for dataasets or data files
        """
        pass

    @abstractmethod
    def exists(self, file: File) -> None:
        """Checks existence of Dataset

        Args:
            file (File): Parameter object for dataasets or data files
        """
        pass

    def _get_io(self, format: str) -> Union[SparkCSV, SparkParquet]:
        try:
            return IO[format.replace(".", "")]
        except KeyError as e:
            classname = self.__class__.__name__
            method = inspect.stack()[1][3]
            msg = "Error in {}: {}. Invalid format: {}\n{}".format(classname, method, format, e)
            logger.error(msg)
            raise ValueError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                    LOCAL FILE MANAGER                                            #
# ------------------------------------------------------------------------------------------------ #


class FAO(FAOBase):
    """File operations for local files."""

    # -------------------------------------------------------------------------------------------- #
    def create(self, file: File, data: Any, force: bool = False) -> None:
        """Persists a new data table to storage.

        Args:
            file (File): Parameter object for create operations
        """
        if os.path.exists(file.filepath) and not force:
            msg = "{} already exists. Create aborted. To overwrite, set force = True.".format(
                file.filepath
            )
            logger.error(msg)
            raise FileExistsError(msg)

        io = self._get_io(format=file.format)
        io.write(data=data, filepath=file.filepath)

    # -------------------------------------------------------------------------------------------- #
    def read(self, file: File) -> Any:
        """Obtains an object from persisted storage

        Args:
            file (File): Parameter object for file read operations

        Returns (DataFrame)
        """

        io = self._get_io(format=file.format)
        return io.read(filepath=file.filepath)

    # -------------------------------------------------------------------------------------------- #
    def delete(self, file: File) -> None:
        """Removes a data table from persisted storage

        Args:
            file (File): Parameter object for dataasets or data files
        """
        shutil.rmtree(file.filepath, ignore_errors=True)

    # -------------------------------------------------------------------------------------------- #
    def exists(self, file: File) -> None:
        """Checks existence of Dataset

        Args:
            file (File): Parameter object for dataasets or data files
        """
        return os.path.exists(file.filepath)


# ------------------------------------------------------------------------------------------------ #
#                                       VALIDATOR                                                  #
# ------------------------------------------------------------------------------------------------ #
class Validator:
    """Provides validation for Dataset and File objects"""

    def source(self, value: str) -> bool:
        if value not in SOURCES:
            self._fail(value)
        else:
            return value

    def format(self, value: str) -> bool:
        value = value.replace(".", "")
        if value not in FORMATS:
            self._fail(value)
        else:
            return value

    def storage_type(self, value: str) -> bool:
        if value not in STORAGE_TYPES:
            self._fail(value)
        else:
            return value

    def stage(self, value: int) -> bool:
        if value not in STAGES.keys():
            self._fail(value)
        else:
            return value

    def filepath(self, value: int) -> bool:
        # This is called for S3 files. Filepath is the object key and must not be None.
        # Could have done this in the File object, but chose to keep all validation
        # and exception handling for validation errors in the validation object.
        if value is None:
            self._fail(value)
        else:
            return value

    def _fail(self, value: Any):
        variable = inspect.stack()[1][3]
        caller_method = inspect.stack()[0][3]
        caller_classname = caller_method.__class__.__name__
        msg = "Error in {}: {}. Invalid {}: {}. Valid values are: {}".format(
            caller_classname, caller_method, variable, value, SOURCES
        )
        logger.error(msg)
        raise ValueError(msg)
