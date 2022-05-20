#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /io.py                                                                                #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 6:41:17 pm                                              #
# Modified : Tuesday, May 3rd 2022, 8:48:33 pm                                                     #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv
import logging
import tempfile
import tarfile
import logging.config
import progressbar
import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import NoCredentialsError

from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                          COMPRESSION                                             #
# ------------------------------------------------------------------------------------------------ #


class Compression(ABC):
    """Abstract base class for data compression classes."""

    @abstractmethod
    @staticmethod
    def compress(self, source: str, destination: str, force: str = False) -> None:
        pass

    @abstractmethod
    @staticmethod
    def expand(self, source: str, destination: str, force: str = False) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                             CLOUD                                                #
# ------------------------------------------------------------------------------------------------ #


class Cloud(ABC):
    """Base class for Upload / Download operations with cloud storage providers."""

    @abstractmethod
    def upload_file(self, filepath: str, bucket: str, object: str, force: str = False) -> None:
        pass

    @abstractmethod
    def download_file(self, bucket: str, object: str, filepath: str, force: str = False) -> None:
        pass

    @abstractmethod
    def delete_object(self, bucket: str, object: str, force: str = False) -> None:
        pass

    @abstractmethod
    def delete_folder(self, bucket: str, folder: str, force: str = False) -> None:
        pass

    @abstractmethod
    def list_objects(self, bucket: str, folder: str = None) -> list:
        pass

    @abstractmethod
    def exists(self, bucket: str, object: str, force: str = False) -> bool:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                              S3                                                  #
# ------------------------------------------------------------------------------------------------ #
class S3(Cloud):
    """Base class for S3 uploading and downloading"""

    # Transfer Configuration controls parallelism, and other factors affecting throughput
    __MB = 1024 ** 2
    __transfer_config = TransferConfig(
        max_concurrency=20,
        multipart_chunksize=16 * __MB,
        multipart_threshold=64 * __MB,
        max_bandwidth=50 * __MB,
    )

    def upload_file(
        self, filepath: str, bucket: str, object: str, compress: bool = True, force: str = False
    ) -> None:
        """Uploads a file to an S3 resource

        Args:
            filepath (str, force: str = False): The path to the file to be uploaded
            bucket (str, force: str = False): The name of the S3 bucket
            object (str, force: str = False): The S3 folder or key, including prefix, to the object

        """

        if self.exists(bucket, object) and not force:
            logger.warning(
                "S3 Resource {} in {} already exists. Upload aborted. To overwrite the object, set force = True.".format(
                    object, bucket
                )
            )
        else:
            with tempfile.TemporaryFile() as tempfilename:
                if compress:
                    with tarfile.open(tempfilename, "w:gz") as tar:
                        tar.add(filepath, arcname=os.path.basename(filepath))
                    filepath = tempfilename

                s3 = self._get_s3_resource()

                # Get size of file and provision the progress monitor.

                size = os.path.getsize(filepath)
                self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
                self._progressbar.start()

                try:
                    s3.upload_file(
                        Filename=filepath,
                        Bucket=bucket,
                        Key=object,
                        Callback=self._callback,
                        Config=S3.__transfer_config,
                    )

                except NoCredentialsError:
                    msg = "Credentials not available for {} bucket".format(bucket)
                    raise NoCredentialsError(msg)

                except FileNotFoundError as e:
                    logger.error("File {} was not found.".format(filepath))
                    raise FileExistsError(e)

    def download_file(
        self, bucket: str, object: str, filepath: str, expand: bool = True, force: str = False
    ) -> None:
        """Downloads a file from an S3 resource

        Args:
            bucket (str): The S3 bucket from which the file will be downloaded
            object (str): The object name w/o the bucket name
            filepath (str): The destination for the file. If expand is False, filepath will be
                a path to a file. Otherwise, filepath will actually be a directory in to which thee
                archive will be expanded.
            expand (bool): True if the resource should be expanded, False otherwise.
            force (bool): If True, overwrite the object data if it exists; otherwise, upload only if
                the object doesn't already exist.
        """

        if os.path.exists(filepath) and not force:
            logger.warning(
                "File {} already exists. Download aborted. To overwrite the file, set force = True.".format(
                    filepath
                )
            )
        else:

            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            with tempfile.TemporaryFile() as tempfilepath:

                download_filepath = tempfilepath if expand else filepath

                s3 = self._get_s3_resource()

                # Get the size of the resource and configure the progress monitor
                response = s3.head_object(Bucket=bucket, Key=object)
                size = response["ContentLength"]
                self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
                self._progressbar.start()

                try:
                    s3.download_file(
                        bucket,
                        object,
                        download_filepath,
                        Callback=self._callback,
                        Config=S3.__transfer_config,
                    )

                except NoCredentialsError:
                    msg = "Credentials not available for {} bucket".format(bucket)
                    raise NoCredentialsError(msg)

                if expand:
                    with tarfile.open(download_filepath, "r:gz") as tar:
                        names = tar.getnames()
                        for name in names:
                            member_filepath = os.path.join(filepath, name)
                            if os.path.exists(member_filepath) and not force:
                                logger.warning(
                                    "\tFile {} already exists. To overwrite, set force = True".format(
                                        name
                                    )
                                )
                            else:
                                tar.extract(member=name, path=filepath)

    def delete_object(self, bucket: str, object: str, force: str = False) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str, force: str = False): The S3 bucket name
            object (str, force: str = False): The S3 object key
        """
        s3 = self._get_s3_resource()
        s3.Object(bucket, object).delete()

    def delete_folder(self, bucket: str, folder: str, force: str = False) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str, force: str = False): The S3 bucket name
            folder (str, force: str = False): The S3 folder with trailing backslash
        """
        s3 = self._get_s3_resource()
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=folder).delete()

    def exists(self, bucket: str, object: str, force: str = False) -> bool:
        """Checks if a file exists in an S3 bucket

        Args:
            bucket (str, force: str = False): The S3 bucket containing the resource
            object (str, force: str = False): The path to the object

        """
        s3 = self._get_s3_resource()

        try:
            s3.head_object(Bucket=bucket, Key=object)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                logger.error(e)

        return True

    def list_objects(self, bucket: str, folder: str = None) -> list:
        """Returns a list of object keys in the designated bucket and folder"""

        s3 = self._get_s3_resource()

        objects = []

        bucket = s3.Bucket(bucket)
        for object in bucket.objects.filter(Delimiter="/t", Prefix=folder):
            if not object.key.endswith("/"):  # Skip objects that are just the folder name
                objects.append(object.key)

        return objects

    def _callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)

    def _get_s3_resource(self) -> boto3.resource:
        """Obtains an S3 boto3.resource object."""
        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")

        s3 = boto3.resource("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)
        return s3
