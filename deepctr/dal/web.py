#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /web.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Sunday May 15th 2022 11:22:36 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module defines the API for data access and management."""
import os
import logging
import logging.config

from deepctr.data.base import RAO
from deepctr.data.web import S3
from deepctr.data.params import S3Params, DatasetParams, FileParams
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
#                                     DATA REPOSITORY                                              #
# ------------------------------------------------------------------------------------------------ #


class RemoteAccessObject(RAO):
    """Remote access object for Amazon S3 web resources."""

    # -------------------------------------------------------------------------------------------- #
    def download_entity(
        self, source: S3Params, destination: FileParams, expand: bool = True, force: bool = False,
    ) -> None:
        """Downloads data entity from an S3 Resource

        Args:
            source (S3Params): Parameter object for the S3 object
            destination (FileParams): Parameter object for a file
            expand (bool): Whether the resouce should be expanded from Tar GZip archive.
            force (bool): Overwrite existing data, if it exists IFF True.
        """
        filepath = RAO.__filepath.get_path(destination)
        io = S3()
        io.download_file(
            bucket=source.bucket,
            object=source.object,
            filepath=filepath,
            expand=expand,
            force=force,
        )

    # -------------------------------------------------------------------------------------------- #
    def download_dataset(
        self, source: S3Params, destination: DatasetParams, expand: bool = True, force: bool = False
    ) -> None:
        """Downloads a dataset contained in an S3 folder to a local directory.

        Args:
            source (S3Params): Parameter object for the S3 folder
            destination (FileParams): Parameter object for a local directory
            expand (bool): Whether the resouce should be expanded from Tar GZip archive.
            force (bool): Overwrite existing data, if it exists IFF True.
        """

        directory = RAO.__directory.get_path(params=destination)
        io = S3()
        objects = io.list_objects(bucket=source.bucket, folder=source.folder)
        for object in objects:
            filepath = os.path.join(directory, object)
            io.download_file(
                bucket=source.bucket, object=object, filepath=filepath, expand=expand, force=force
            )

    # -------------------------------------------------------------------------------------------- #
    def upload_entity(
        self, source: FileParams, destination: S3Params, compress: bool = True, force: bool = False,
    ) -> None:
        """Uploads a file to an S3 bucket.

        Args:
            source (FileParams): Parameter object for a local file
            destination (S3Params): Parameter object for an S3 resource
            compress (bool): Whether the data entity should be compressed before uploading.
            force (bool): Overwrite existing data, if it exists IFF True.
        """
        filepath = RAO.__filepath.get_path(params=source)

        io = S3()
        io.upload_file(
            filepath=filepath,
            bucket=destination.bucket,
            object=destination.object,
            compress=compress,
            force=force,
        )

    # -------------------------------------------------------------------------------------------- #
    def upload_dataset(
        self,
        source: DatasetParams,
        destination: S3Params,
        compress: bool = True,
        force: bool = False,
    ) -> None:
        """Uploads a dataset to an S3 folder.

        Args:
            source (DatasetParams): Parameter object for the S3 folder
            destination (S3Params): Parameter object for a local directory
            compress (bool): Whether the dataset should be compressed before uploading.
            force (bool): Overwrite existing data, if it exists IFF True.
        """

        directory = RAO.__directory.get_path(params=destination)
        io = S3()
        files = os.listdir(directory)
        for file in files:
            object = os.path.join(destination.folder, file)
            filepath = os.path.join(directory, file)
            io.upload_file(
                filepath=filepath,
                bucket=destination.bucket,
                object=object,
                compress=compress,
                force=force,
            )
