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
# Modified   : Wednesday June 22nd 2022 10:56:17 am                                                #
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
from deepctr.data.io import SparkCSV, SparkParquet, Pickler
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
    format: str  # The format of the data, i.e. csv, parquet

    def _validate(self) -> None:
        validate = Validator()
        self.source = validate.source(self.source)
        self.format = validate.format(self.format)
        self.stage_id = validate.stage(self.stage_id)


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

    filepath: str = None  # Path to the file
    compressed: bool = False  # Indicates if the file is compressed
    size: int = 0  # The size of the file in bytes
    id: int = 0  # The id assigned by the database
    home: str = "data"  # The home directory for all data. Can be overidden for testing.
    created: datetime = datetime.now()  # Should be overwritten if file exists.

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
#                                        RAO                                                       #
# ------------------------------------------------------------------------------------------------ #


class RAO(ABC):
    """Defines interface for remote access objects accessing cloud services."""

    @abstractmethod
    def download_file(
        source: S3File, destination: File, expand: bool = True, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def download_dataset(
        source: Dataset, destination: Dataset, expand: bool = True, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def upload_file(
        self, source: File, destination: S3File, compress: bool = True, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def upload_dataset(
        self, source: Dataset, destination: Dataset, compress: bool = True, force: bool = False
    ) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                    REMOTE ACCESS OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #


class RemoteAccessObject(RAO):
    """Remote access object for Amazon S3 web resources."""

    # -------------------------------------------------------------------------------------------- #
    def download_file(
        source: S3File, destination: LocalFile, expand: bool = True, force: bool = False
    ) -> None:
        """Downloads data entity from an S3 Resource

        Args:
            source (S3File): An S3File object
            destination (LocalFile): A local file object
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
    def download_dataset(
        self, source: Dataset, destination: Dataset, expand: bool = True, force: bool = False
    ) -> None:
        """Downloads data entity from an S3 Resource

        Args:
            source (Dataset): The Dataset containing the files to be downloaded
            destination (Dataset): The downloaded Dataset
            expand (bool): If True, the data is decompressed.
            force (bool): If True, existing data will be overwritten.
        """

        io = S3()

        object_keys = io.list_objects(bucket=source.bucket, folder=source.folder)

        for object_key in object_keys:
            source_file = File.factory(dataset=source, filepath=object_key)
            # destination filepath is constructed from the destination folder and the base of the object_key
            destination_file = File.factory(
                dataset=destination,
                filepath=os.path.join(destination.folder, os.path.basename(object_key)),
            )
            self.download_file(
                source=source_file, destination=destination_file, expand=expand, force=force
            )

    # -------------------------------------------------------------------------------------------- #
    def upload_file(
        self, source: LocalFile, destination: S3File, compress: bool = True, force: bool = False
    ) -> None:
        """Uploads a entity to an S3 bucket.

        Args:
            source (LocalFile): A File object
            destination (S3File): An S3File object
            compress (bool): If True, the data is decompressed.
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
    def upload_dataset(
        self, source: Dataset, destination: Dataset, compress: bool = True, force: bool = False
    ) -> None:
        """Uploads all files of a dataset to an S3 bucket

        Args:
            source (Dataset): The Dataset to be uploaded
            destination (Dataset): The target S3 Dataset
            compress (bool): If True, the data is decompressed.
            force (bool): If True, existing data will be overwritten.
        """
        filepaths = os.listdir(source.folder)
        for filepath in filepaths:
            source_file = File.factory(dataset=source, filepath=filepath)

            state = str(destination.stage_id) + "_" + destination.stage_name

            # The S3 object_key is formed by joining the bucket, dataset name,
            # the state, i.e. (2_external), and the basename from the filepath.

            object_key = os.path.join(
                destination.bucket, destination.name, state, os.path.basename(filepath)
            )
            destination_file = File.factory(dataset=destination, filepath=object_key)
            self.upload_file(
                source=source_file, destination=destination_file, compress=compress, force=force
            )

    # -------------------------------------------------------------------------------------------- #
    def delete_object(self, file: S3File) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str, force: str = False): The S3 bucket name
            object (str, force: str = False): The S3 object key
        """
        io = S3()
        io.delete_object(bucket=file.bucket, object_key=file.object_key)

    # -------------------------------------------------------------------------------------------- #
    def delete_dataset(self, dataset: Dataset) -> None:
        """Deletes a dataset and the files it contains.

        Args:
            dataset (Dataset): Dataset to delete
        """
        if dataset.storage_type == "local":
            shutil.rmtree(dataset.folder, ignore_errors=True)
        else:
            io = S3()
            object_keys = io.list_objects(bucket=dataset.bucket, folder=dataset.folder)
            for object_key in object_keys:
                file = File.factory(dataset, object_key)
                self.delete_object(file)

    # -------------------------------------------------------------------------------------------- #
    def exists(self, file: S3File) -> bool:
        """Checks if a entity exists in an S3 bucket

        Args:
            bucket (str, force: str = False): The S3 bucket containing the resource
            object (str, force: str = False): The path to the object

        """

        io = S3()
        return io.exists(bucket=file.bucket, object_key=file.object_key)


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

    def _fail(self, value: Any):
        variable = inspect.stack()[1][3]
        caller_method = inspect.stack()[0][3]
        caller_classname = caller_method.__class__.__name__
        msg = "Error in {}: {}. Invalid {}: {}. Valid values are: {}".format(
            caller_classname, caller_method, variable, value, SOURCES
        )
        logger.error(msg)
        raise ValueError(msg)
