#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /rao.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Wednesday June 22nd 2022 12:15:32 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module defines the API for data access and management."""
from abc import ABC, abstractmethod
import os
import logging
import logging.config
import shutil

from deepctr.data.remote import S3
from deepctr.dal.fao import S3File, LocalFile, Dataset
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                        RAO                                                       #
# ------------------------------------------------------------------------------------------------ #


class RAO(ABC):
    """Defines interface for remote access objects accessing cloud services."""

    @abstractmethod
    def download_file(
        source: S3File, destination: LocalFile, expand: bool = True, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def download_dataset(
        source: Dataset, destination: Dataset, expand: bool = True, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def upload_file(
        self, source: LocalFile, destination: S3File, compress: bool = True, force: bool = False
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
