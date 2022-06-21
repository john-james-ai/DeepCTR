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
# Modified   : Sunday June 19th 2022 04:27:06 pm                                                   #
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

from deepctr.dal import STATES
from deepctr.utils.aws import get_size_aws
from deepctr.data.datastore import SparkCSV, SparkParquet, Pickler
from deepctr.data.web import S3
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                          DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class Dataset:
    """Defines the parameters of local datasets to which the Files belong"""

    name: str  # The name of the dataset, i.e. vesuvio
    source: str  # The external data source
    stage_id: int  # The data processing stage number
    stage_name: str  # The data processing stage name
    storage_type: str  # Where the data are stored. Either 's3', or 'local'
    format: str  # The format, either csv or parquet
    compressed: bool  # Indicates whether the data are compressed.
    bucket: str = None  # The bucket containing the files. Used for aws datasets.
    folder: str = None  # The folder containing the files. Used for aws datasets.


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
    size: int = 0  # The size of the file in bytes
    id: int = 0  # The id assigned by the database
    created: datetime = datetime.now()  # Should be overwritten if file exists.

    __sources: ClassVar[list[str]] = ["alibaba", "avazu", "criteo"]
    __stage_ids: ClassVar[list[int]] = list(STATES.keys())
    __stage_names: ClassVar[list[str]] = list(STATES.values())
    __formats: ClassVar[list[str]] = ["csv", "parquet", "tar.gz"]

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

        if self.format not in File.__formats:
            raise ValueError(
                "{} is not a valid format. Valid values are {}.".format(self.format, File.__formats)
            )

    @abstractmethod
    def _set_size(self) -> None:
        pass

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @staticmethod
    def factory(dataset: Dataset, filepath: str):
        """Creates an object of the LocalFile or S3File class from a Dataset object

        Args:
            dataset (Dataset): The dataset containing the file
            filepath (str): The path to the file if local. If an S3File, the filepath
                is synonymous with object_key.
        """
        # The name of the File is given by the filepath with the extension removed.
        name = os.path.splitext(os.path.basename(filepath))[0]

        if dataset.storage_type == "local":
            return LocalFile(
                name=name,
                source=dataset.source,
                dataset=dataset.name,
                stage_id=dataset.stage_id,
                stage_name=dataset.stage_name,
                format=dataset.format,
                dag_id=dataset.dag_id,
                task_id=dataset.task_id,
                filepath=filepath,
                folder=os.path.dirname(filepath),
                filename=os.path.basename(filepath),
                compressed=dataset.compressed,
            )

        else:
            return S3File(
                name=name,
                source=dataset.source,
                dataset=dataset.name,
                stage_id=dataset.stage_id,
                stage_name=dataset.stage_name,
                format=dataset.format,
                dag_id=dataset.dag_id,
                task_id=dataset.task_id,
                folder=os.path.dirname(filepath),
                bucket=dataset.bucket,
                object_key=filepath,
                compressed=dataset.compressed,
            )


# ------------------------------------------------------------------------------------------------ #
#                                     LOCAL FILE                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class LocalFile(File):
    """Defines the file objects stored locally."""

    filepath: str = None  # Path to the file
    compressed: bool = False  # Indicates if the file is compressed

    def __post_init__(self) -> None:
        self._validate()
        self._set_filepath()
        self._set_size()

    def _set_filepath(self) -> None:
        stage = str(self.stage_id) + "_" + self.stage_name
        if not self.filepath:
            self.filename = self.name + "." + self.format.replace(".", "").lower()
            self.folder = os.path.join("data", self.source, self.dataset, stage)
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
            "stage_name": self.stage_name,
            "filepath": self.filepath,
            "folder": self.folder,
            "filename": self.filename,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "created": self.created,
        }
        return d

    @staticmethod
    def factory(dataset: Dataset, filepath: str):
        """Creates an object of the LocalFile or S3File class.

        Args:
            dataset (Dataset): The dataset containing the file
            filepath (str): The path to the file if local. If an S3File, the filepath
                is synonymous with object_key.
        """
        # The name of the File is given by the filepath with the extension removed.
        name = os.path.splitext(os.path.basename(filepath))[0]


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
        self._set_filepath()
        self._set_size()

    def _set_filepath(self) -> None:
        self.folder = os.path.dirname(self.object_key)
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
            "folder": self.folder,
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

            state = str(destination.state_id) + "_" + destination.stage_name

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
