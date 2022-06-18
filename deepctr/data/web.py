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
import uuid
import inspect
import tarfile
import shutil
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
#                                             CLOUD                                                #
# ------------------------------------------------------------------------------------------------ #


class Cloud(ABC):
    """Base class for Upload / Download operations with cloud storage providers."""

    def __init__(self) -> None:
        self._tempfile = None
        self._tempdir = None

    def __enter__(self):
        return self

    def __exit__(self):
        shutil.rmtree(self._tempfile, ignore_errors=True)
        shutil.rmtree(self._tempdir, ignore_errors=True)
        self._tempfile = None
        self._tempdir = None

    @abstractmethod
    def upload_file(
        self,
        filepath: str,
        bucket: str,
        object_key: str = None,
        compress: bool = True,
        force: str = False,
    ) -> None:
        pass

    @abstractmethod
    def download_file(
        self, bucket: str, object_key: str, filepath: str, expand: bool = True, force: str = False
    ) -> None:
        pass

    @abstractmethod
    def delete_object(self, bucket: str, object_key: str, force: str = False) -> None:
        pass

    @abstractmethod
    def delete_folder(self, bucket: str, folder: str, force: str = False) -> None:
        pass

    @abstractmethod
    def list_objects(self, bucket: str, folder: str = None) -> list:
        pass

    @abstractmethod
    def exists(self, bucket: str, object_key: str) -> bool:
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
        self,
        filepath: str,
        bucket: str,
        object_key: str = None,
        compress: bool = True,
        force: str = False,
    ) -> None:
        """Uploads a file to an S3 resource

        Args:
            filepath (str): The path to the file to be uploaded
            bucket (str): The name of the S3 bucket
            object_key (str): The optional path to the object. Defaults to basename of filepath.
            compress (bool): True to compress, otherwise False. Default = True
            force (bool): True to upload even if exists, False otherwise. Default = False

        """
        object_key = self._get_object_key(
            filepath=filepath, object_key=object_key, compress=compress
        )

        if self.exists(bucket, object_key) and not force:
            logger.warning(
                "S3 Resource {} in {} already exists. Upload aborted. To overwrite the object, set force = True.".format(
                    object_key, bucket
                )
            )
        else:

            # If compress is True, we create a temporary tar.gz file archive, upload it, then delete it.
            if compress:
                filepath_compressed = filepath + ".tar.gz"
                try:
                    with tarfile.open(filepath_compressed, "w:gz") as tar:
                        tar.add(filepath, arcname=os.path.basename(filepath))

                    self._upload_file(
                        filepath=filepath_compressed, bucket=bucket, object_key=object_key
                    )

                except tarfile.TarError as e:
                    logger.error(e)
                    raise
                finally:
                    shutil.rmtree(filepath_compressed, ignore_errors=True)

            else:
                self._upload_file(filepath=filepath, bucket=bucket, object_key=object_key)

    def _upload_file(self, filepath: str, bucket: str, object_key: str) -> None:
        """Wraps all S3 upload related operations."""

        s3 = self._get_s3_connection(connection_type="client")

        size = os.path.getsize(filepath)
        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        try:
            s3.upload_file(
                Filename=filepath,
                Bucket=bucket,
                Key=object_key,
                Callback=self._callback,
                Config=S3.__transfer_config,
            )

        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(bucket)
            raise NoCredentialsError(msg)

    def download_file(
        self, bucket: str, object_key: str, filepath: str, expand: bool = True, force: str = False
    ) -> None:
        """Downloads a file from an S3 resource

        Args:
            bucket (str): The S3 bucket from which the file will be downloaded
            object_key (str): The path to the object within the bucket
            filepath (str): The destination for the file. If expand is False, filepath will be
                a path to a file. Otherwise, filepath will actually be a folder in to which thee
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

            # If we expand, we create a temporary file, create the folder for it, then
            # download the archive to this file which is then extracted to the client provided filepath
            if expand:
                download_filepath = os.path.join(
                    os.path.dirname(filepath), str(uuid.uuid4()), os.path.basename(filepath)
                )
                os.makedirs(os.path.dirname(download_filepath), exist_ok=True)
                self._download_file(
                    bucket=bucket, object_key=object_key, filepath=download_filepath
                )

                try:
                    with tarfile.open(download_filepath, "r:gz") as tar:
                        names = tar.getnames()
                        for name in names:
                            member_expand_filepath = os.path.join(filepath, name)
                            # We don't download if data already exists, unless force is True
                            if os.path.exists(member_expand_filepath) and not force:
                                logger.warning(
                                    "\tArchive member {} already exists. To overwrite, set force = True".format(
                                        name
                                    )
                                )
                            else:
                                tar.extract(member=name, path=member_expand_filepath)

                except tarfile.TarError as e:
                    logger.error(e)
                    raise
                finally:
                    # Dispose of the temporary download file
                    shutil.rmtree(os.path.dirname(download_filepath), ignore_errors=True)

            else:
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                self._download_file(bucket=bucket, object_key=object_key, filepath=filepath)

    def _download_file(self, bucket: str, object_key: str, filepath: str) -> None:
        """Wraps all S3 download related operations."""

        try:
            # Get the size of the resource
            s3 = self._get_s3_connection(connection_type="client")
            response = s3.head_object(Bucket=bucket, Key=object_key)
            size = response["ContentLength"]

            # Configure the progress monitor
            self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
            self._progressbar.start()

            # Download the file (or file object for compressed files)
            if "tar.gz" in filepath:
                with open(filepath, "wb") as f:
                    s3.download_fileobj(
                        bucket, object_key, f, Callback=self._callback, Config=S3.__transfer_config,
                    )
            else:
                s3.download_file(
                    bucket,
                    object_key,
                    filepath,
                    Callback=self._callback,
                    Config=S3.__transfer_config,
                )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                msg = "Object {} does not exist.".format(object_key)
                logger.error(msg)
                raise ValueError(msg)
            else:
                operation_name = "{}: {}".format(self.__class__.__name__, inspect.stack()[0][3])
                raise botocore.exceptions.ClientError(
                    error_response=e, operation_name=operation_name
                )

        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(bucket)
            raise NoCredentialsError(msg)

    def delete_object(self, bucket: str, object_key: str, force: str = False) -> None:
        """Deletes a object_name from S3 storage

        Args:
            bucket (str, force: str = False): The S3 bucket name
            object_key (str, force: str = False): The path to the object
        """
        s3 = self._get_s3_connection(connection_type="resource")
        s3.Object(bucket, object_key).delete()

    def delete_folder(self, bucket: str, folder: str, force: str = False) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str, force: str = False): The S3 bucket name
            folder (str, force: str = False): The S3 folder with trailing backslash
        """
        s3 = self._get_s3_connection(connection_type="resource")
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=folder).delete()

    def exists(self, bucket: str, object_key: str) -> bool:
        """Checks if a file exists in an S3 bucket

        Args:
            bucket (str, force: str = False): The S3 bucket containing the resource
            object_key (str, force: str = False): The path of the object

        """
        s3 = self._get_s3_connection(connection_type="client")

        try:
            s3.head_object(Bucket=bucket, Key=object_key)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                logger.error(e)

        return True

    def list_objects(self, bucket: str, folder: str = None) -> list:
        """Returns a list of object keys in the designated bucket and folder

        Args:
            bucket (str, force: str = False): The S3 bucket containing the resource
            folder (str, force: str = False): The path of the object

        """

        s3 = self._get_s3_connection(connection_type="resource")

        objects = []

        bucket = s3.Bucket(bucket)
        for object in bucket.objects.filter(Delimiter="/t", Prefix=folder):
            if not object.key.endswith("/"):  # Skip objects that are just the folder name
                objects.append(object.key)

        return objects

    def _get_object_key(self, filepath: str, object_key, compress: bool) -> str:
        """Returns the object_name name given a filepath, folder and compress flag"""
        object_key = os.path.basename(filepath) if not object_key else object_key
        object_key = (
            object_key + ".tar.gz" if compress and ".tar.gz" not in object_key else object_key
        )
        return object_key

    def _callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)

    def _get_s3_connection(self, connection_type: str = "resource") -> boto3.resource:
        """Obtains an S3 boto3.resource object."""
        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")

        if connection_type == "resource":
            s3 = boto3.resource(
                "s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD
            )
        else:
            s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)
        return s3
