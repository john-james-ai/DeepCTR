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
# Modified   : Wednesday May 25th 2022 12:49:02 am                                                 #
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
from deepctr.data.params import S3Params, DatasetParams, EntityParams
from deepctr.dal.base import EntityPathFinder, DatasetPathFinder
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                    REMOTE ACCESS OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #


class RemoteAccessObject(RAO):
    """Remote access object for Amazon S3 web resources."""

    # -------------------------------------------------------------------------------------------- #
    def download_file(
        self, source: S3Params, destination: EntityParams, expand: bool = True, force: bool = False,
    ) -> None:
        """Downloads data entity from an S3 Resource

        Args:
            source (S3Params): Parameter object for the S3 object
            destination (EntityParams): Parameter object for a entity
            expand (bool): Whether the resouce should be expanded from Tar GZip archive.
            force (bool): Overwrite existing data, if it exists IFF True.
        """
        entitypath = EntityPathFinder().get_path(destination)
        io = S3()
        io.download_entity(
            bucket=source.bucket,
            object=source.object,
            entitypath=entitypath,
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
            destination (EntityParams): Parameter object for a local directory
            expand (bool): Whether the resouce should be expanded from Tar GZip archive.
            force (bool): Overwrite existing data, if it exists IFF True.
        """

        directory = DatasetPathFinder().get_path(destination)
        io = S3()
        objects = io.list_objects(bucket=source.bucket, folder=source.folder)
        for object in objects:
            entitypath = os.path.join(directory, object)
            io.download_entity(
                bucket=source.bucket,
                object=object,
                entitypath=entitypath,
                expand=expand,
                force=force,
            )

    # -------------------------------------------------------------------------------------------- #
    def upload_entity(
        self,
        source: EntityParams,
        destination: S3Params,
        compress: bool = True,
        force: bool = False,
    ) -> None:
        """Uploads a entity to an S3 bucket.

        Args:
            source (EntityParams): Parameter object for a local entity
            destination (S3Params): Parameter object for an S3 resource
            compress (bool): Whether the data entity should be compressed before uploading.
            force (bool): Overwrite existing data, if it exists IFF True.
        """
        entitypath = EntityPathFinder().get_path(source)

        io = S3()
        io.upload_entity(
            entitypath=entitypath,
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
            source (EntityParams): Parameter object for the S3 folder
            destination (S3Params): Parameter object for a local directory
            compress (bool): Whether the dataset should be compressed before uploading.
            force (bool): Overwrite existing data, if it exists IFF True.
        """

        directory = DatasetPathFinder().get_path(destination)
        io = S3()
        entitys = os.listdir(directory)
        for entity in entitys:
            object = os.path.join(destination.folder, entity)
            entitypath = os.path.join(directory, entity)
            io.upload_entity(
                entitypath=entitypath,
                bucket=destination.bucket,
                object=object,
                compress=compress,
                force=force,
            )

    # -------------------------------------------------------------------------------------------- #
    def delete_entity(self, object: S3Params) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str, force: str = False): The S3 bucket name
            object (str, force: str = False): The S3 object key
        """
        io = S3()
        io.delete_object(bucket=object.bucket, object=object.object)

    # -------------------------------------------------------------------------------------------- #
    def exists(self, object: S3Params) -> bool:
        """Checks if a entity exists in an S3 bucket

        Args:
            bucket (str, force: str = False): The S3 bucket containing the resource
            object (str, force: str = False): The path to the object

        """

        io = S3()
        return io.exists(bucket=object.bucket, object=object.object)
