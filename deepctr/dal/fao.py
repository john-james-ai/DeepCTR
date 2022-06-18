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
# Modified   : Saturday June 18th 2022 07:40:02 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module defines the API for data access and management."""
from abc import ABC, abstractmethod
import os
from dataclasses import dataclass
from datetime import datetime
import logging
import logging.config
from typing import Any, Union, ClassVar
import shutil
from deepctr.utils.aws import get_size_aws
from deepctr.data.datastore import SparkCSV, SparkParquet, Pickler
from deepctr.data.web import S3
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                        FILE                                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class File(ABC):
    """Defines the interface for File objects."""

    name: str  # Name of file without the extension, i.e. user, profile, behavior, ad
    source: str  # The name as the dataset is externally recognized. i.e. alibaba
    dataset: str  # The collection to which the file belongs
    stage_id: int  # The stage identifier. See lab_book.md for stage_ids
    stage_name: str  # The human readable name of the stage
    format: str  # The format of the data, i.e. csv, parquet
    dag_id: int  # The dag_id for the dag in which the file was created.
    task_id: int  # The task_id for the task that created the file.

    __sources: ClassVar[list[str]] = ["alibaba", "avazu", "criteo"]
    __stage_ids: ClassVar[list[int]] = list(range(0, 8))
    __stage_names: ClassVar[list[str]] = [
        "external",
        "raw",
        "loaded",
        "prepared",
        "clean",
        "transformed",
        "enriched",
        "processed",
    ]
    __formats: ClassVar[list[str]] = ["csv", "parquet"]

    def _validate(self) -> None:
        if self.source not in File.__sources:
            raise ValueError(
                "{} is not a valid data source. Valid values are {}.".format(
                    self.source, File.__sources
                )
            )
        if self.stage_id not in File.__stage_ids:
            raise ValueError(
                "{} is not a valid stage_id. Valid values are {}.".format(
                    self.stage_id, File.__stage_ids
                )
            )
        if self.stage_name not in File.__stage_names:
            raise ValueError(
                "{} is not a valid stage name. Valid values are {}.".format(
                    self.stage_name, File.__stage_names
                )
            )

    @abstractmethod
    def _set_size(self) -> None:
        pass

    @abstractmethod
    def to_dict(self) -> dict:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                     LOCAL FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class LocalFile(File):
    """Defines the file objects stored locally."""

    filepath: str = None  # Path to the file
    directory: str = None  # The folder containing the file
    filename: str = None  # The name of the file i.e basename from filepath
    compressed: bool = False  # Indicates if the file is compressed
    size: int = 0  # The size of the file in bytes
    id: int = 0
    created: datetime = datetime.now()

    def __post_init__(self) -> None:
        self._validate()
        self._set_filepath()
        self._set_size()

    def _set_filepath(self) -> None:
        stage = str(self.stage_id) + "_" + self.stage_name
        if not self.filepath:
            self.filename = self.name + "." + self.format.replace(".", "").lower()
            self.directory = os.path.join("data", self.source, self.dataset, stage)
            self.filepath = os.path.join(self.directory, self.filename)
        super(LocalFile, self).__post_init__()

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
            "stage_name": self.stage_name,
            "filepath": self.filepath,
            "directory": self.directory,
            "filename": self.filename,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
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
        self._validate()
        self.filename = os.path.basename(self.object_key)

    def _set_size(self) -> None:
        self.size = get_size_aws(bucket=self.bucket, object_key=self.object_key)

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "source": self.source,
            "dataset": self.dataset,
            "stage_id": self.stage_id,
            "stage_name": self.stage_name,
            "bucket": self.bucket,
            "object_key": self.object_key,
            "filename": self.filename,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "created": self.created,
        }
        return d


# ================================================================================================ #
#                                            FAO                                                   #
# ================================================================================================ #


class FAO(ABC):
    """Base class for file managers."""

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


# ------------------------------------------------------------------------------------------------ #
#                                    LOCAL FILE MANAGER                                            #
# ------------------------------------------------------------------------------------------------ #


class LocalFileManager(FAO):
    """File operations for local files."""

    # -------------------------------------------------------------------------------------------- #
    def create(self, file: File, data: Any, force: bool = False) -> None:
        """Persists a new data table to storage.

        Args:
            file (File): Parameter object for create operations
        """
        if os.path.exists(file.filepath) and not force:
            raise FileExistsError(
                "{} already exists. Create aborted. To overwrite, set force = True.".format(
                    file.filepath
                )
            )

        io = self._get_io(file_format=file.format)
        io.write(data=data, filepath=file.filepath)

    # -------------------------------------------------------------------------------------------- #
    def read(self, file: File) -> Any:
        """Obtains an object from persisted storage

        Args:
            file (File): Parameter object for file read operations

        Returns (DataFrame)
        """

        try:
            io = self._get_io(file_format=file.format)
            return io.read(filepath=file.filepath)
        except FileNotFoundError as e:
            logger.error("File {} not found.".format(file.filepath))
            raise FileNotFoundError(e)

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

    # -------------------------------------------------------------------------------------------- #
    def _get_io(self, file_format: str) -> Union[SparkCSV, SparkParquet, Pickler]:
        if "csv" in file_format:
            io = SparkCSV()
        elif "parquet" in file_format:
            io = SparkParquet()
        elif "pickle" in file_format:
            io = Pickler()
        else:
            raise ValueError("File format {} is not supported.".format(file_format))
        return io


# ------------------------------------------------------------------------------------------------ #
#                                        RAO                                                       #
# ------------------------------------------------------------------------------------------------ #


class RAO(ABC):
    """Defines interface for remote access objects accessing cloud services."""

    @abstractmethod
    def download(
        source: S3File, destination: File, expand: bool = True, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def upload(
        self, source: File, destination: S3File, compress: bool = True, force: bool = False
    ) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                    REMOTE ACCESS OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #


class RemoteAccessObject(RAO):
    """Remote access object for Amazon S3 web resources."""

    # -------------------------------------------------------------------------------------------- #
    def download(
        source: S3File, destination: File, expand: bool = True, force: bool = False
    ) -> None:
        """Downloads data entity from an S3 Resource

        Args:
            source (S3File): An S3File object
            destination (File): A File object
            expand (bool): If True, the data is decompressed.
            force (bool): If True, existing data will be overwritten.
        """

        io = S3()
        io.download_file(
            bucket=source.bucket,
            object_key=source.object_key,
            filepath=destination.filepath,
            expand=expand,
            force=force,
        )

    # -------------------------------------------------------------------------------------------- #
    def upload(
        self, source: File, destination: S3File, compress: bool = True, force: bool = False
    ) -> None:
        """Uploads a entity to an S3 bucket.

        Args:
            source (File): A File object
            destination (S3File): An S3File object
            expand (bool): If True, the data is decompressed.
            force (bool): If True, existing data will be overwritten.
        """

        io = S3()
        io.upload_file(
            filepath=source.filepath,
            bucket=destination.bucket,
            object_key=destination.object_key,
            compress=compress,
            force=force,
        )

    # -------------------------------------------------------------------------------------------- #
    def delete(self, file: S3File) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str, force: str = False): The S3 bucket name
            object (str, force: str = False): The S3 object key
        """
        io = S3()
        io.delete_object(bucket=file.bucket, object_key=file.object_key)

    # -------------------------------------------------------------------------------------------- #
    def exists(self, file: S3File) -> bool:
        """Checks if a entity exists in an S3 bucket

        Args:
            bucket (str, force: str = False): The S3 bucket containing the resource
            object (str, force: str = False): The path to the object

        """

        io = S3()
        return io.exists(bucket=file.bucket, object_key=file.object_key)
