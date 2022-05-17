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
import logging.config
import pandas as pd
import progressbar
import pyspark
from pyspark.sql import SparkSession
import findspark
from typing import Any
import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import NoCredentialsError

from deepctr.utils.log_config import LOG_CONFIG

findspark.init()

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                              S3                                                  #
# ------------------------------------------------------------------------------------------------ #


class Cloud(ABC):
    """Base class for Upload / Download operations with cloud storage providers."""

    @abstractmethod
    def upload_file(self, filepath: str, bucket: str, object: str, force: bool = False) -> None:
        pass

    @abstractmethod
    def upload_directory(
        self, directory: str, bucket: str, folder: str, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def download_file(self, bucket: str, object: str, filepath: str, force: bool = False) -> None:
        pass

    @abstractmethod
    def download_directory(
        self, bucket: str, folder: str, directory: str, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def exists(self, bucket: str, object: str) -> bool:
        pass


class S3(Cloud):
    """Base class for S3 uploading and downloading"""

    def upload_file(self, filepath: str, bucket: str, object: str, force: bool = False) -> None:
        """Uploads a file to an S3 resource

        Args:
            filepath (str): The path to the file to be uploaded
            bucket (str): The name of the S3 bucket
            object (str): The S3 folder or key, including prefix, to the object
            force (bool): If True, overwrite the object data if it exists; otherwise, upload only if
                the object doesn't already exist.
        """

        if self.exists(bucket, object) and not force:
            logger.warning(
                "S3 Resource {}/{} already exists and force = False; therefore, the upload was skipped.".format(
                    bucket, object
                )
            )

        else:
            load_dotenv()
            S3_ACCESS = os.getenv("S3_ACCESS")
            S3_PASSWORD = os.getenv("S3_PASSWORD")

            s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)

            size = os.path.getsize(filepath)
            self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
            self._progressbar.start()

            MB = 1024 ** 2
            config = TransferConfig(
                max_concurrency=20,
                multipart_chunksize=16 * MB,
                multipart_threshold=64 * MB,
                max_bandwidth=50 * MB,
            )

            try:
                s3.upload_file(
                    Filename=filepath,
                    Bucket=bucket,
                    Key=object,
                    Callback=self._callback,
                    Config=config,
                )

            except NoCredentialsError:
                msg = "Credentials not available for {} bucket".format(bucket)
                raise NoCredentialsError(msg)

            except FileNotFoundError as e:
                logger.error("File {} was not found.".format(filepath))
                raise FileExistsError(e)

    def upload_directory(
        self, directory: str, bucket: str, folder: str, force: bool = False
    ) -> None:
        """Uploads files in a directory to an S3 bucket / folder

        Args:
            directory  (str): The origin directory containing files to upload
            bucket (str): The S3 bucket
            folder (str): The S3 folder to which the files will be uploaded.
            force (bool): If True, overwrite the object data if it exists; otherwise, upload only if
                the object doesn't already exist.
        """
        files = os.listdir(directory)
        for file in files:
            filepath = os.path.join(os.path.dirname(directory), file)
            object = os.path.join(folder, file)
            self.upload_file(filepath=filepath, bucket=bucket, object=object, force=force)

    def download_file(self, bucket: str, object: str, filepath: str, force: bool = False) -> None:
        """Downloads a file from an S3 resource

        Args:
            bucket (str): The S3 bucket from which the file will be downloaded
            object (str): The object name w/o the bucket name
            filepath (str): The destination file path to which the resource will be downloaded
            force (bool): If True, overwrite the object data if it exists; otherwise, upload only if
                the object doesn't already exist.
        """

        if os.path.exists(filepath) and not force:
            logger.warning(
                "S3 File {} already exists and force = False; therefore, the upload was skipped.".format(
                    filepath
                )
            )

        elif not self.exists(bucket, object):
            logger.error("S3 Resource {}\\{} does not exist.".format(bucket, object))
            raise FileNotFoundError()

        else:

            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            load_dotenv()

            S3_ACCESS = os.getenv("S3_ACCESS")
            S3_PASSWORD = os.getenv("S3_PASSWORD")

            self._s3 = boto3.client(
                "s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD
            )

            response = self._s3.head_object(Bucket=bucket, Key=object)
            size = response["ContentLength"]

            self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
            self._progressbar.start()

            MB = 1024 ** 2
            config = TransferConfig(
                max_concurrency=20,
                multipart_chunksize=16 * MB,
                multipart_threshold=64 * MB,
                max_bandwidth=50 * MB,
            )

            try:
                self._s3.download_file(
                    Bucket=bucket,
                    Key=object,
                    Filename=filepath,
                    Callback=self._callback,
                    Config=config,
                )

            except NoCredentialsError:
                msg = "Credentials not available for {} bucket".format(self._bucket)
                raise NoCredentialsError(msg)

    def download_directory(
        self, bucket: str, folder: str, directory: str, force: bool = False
    ) -> None:
        """Downloads a group of files from an S3 folder to a local directory

        Args:
            bucket (str): The S3 bucket from which the file will be downloaded
            folder (str): The S3 folder containing the files to download.
            directory (str): The destination directory to which the files will be downloaded.
        """

        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")

        object_keys = self._list_bucket_contents(bucket, folder)

        s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)

        os.makedirs(directory, exist_ok=True)

        for object_key in object_keys:
            filepath = os.path.join(directory, os.path.basename(object_key))

            if force or not os.path.exists(filepath):
                self._download(s3, bucket, object_key, filepath)
            else:
                logger.info(
                    "File {} not downloaded. It already exists".format(os.path.basename(filepath))
                )

    def exists(self, bucket: str, object: str) -> bool:
        """Checks if a file exists in an S3 bucket

        Args:
            bucket (str): The S3 bucket containing the resource
            object (str): The path to the object

        """

        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")

        s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)

        try:
            s3.head_object(Bucket=bucket, Key=object)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                logger.error(e)

        return True

    def _download(self, s3, bucket: str, object: str, filepath: str) -> None:
        """Downloads object designated by the object ke if not exists or force is True"""

        response = s3.head_object(Bucket=bucket, Key=object)
        size = response["ContentLength"]

        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        MB = 1024 ** 2
        config = TransferConfig(
            max_concurrency=20,
            multipart_chunksize=16 * MB,
            multipart_threshold=64 * MB,
            max_bandwidth=50 * MB,
        )

        try:
            s3.download_file(
                bucket, object, filepath, Callback=self._download_callback, Config=config
            )

        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(bucket)
            raise NoCredentialsError(msg)

    def _list_bucket_contents(self, bucket, folder) -> list:
        """Returns a list of objects in the designated bucket"""

        objects = []
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket)
        for object in bucket.objects.filter(Delimiter="/t", Prefix=folder):
            if not object.key.endswith("/"):  # Skip objects that are just the folder name
                objects.append(object.key)

        return objects

    def _callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)


# ------------------------------------------------------------------------------------------------ #
#                                              IO                                                  #
# ------------------------------------------------------------------------------------------------ #


class IO(ABC):
    """Base class for IO classes"""

    @abstractmethod
    def read(self, filepath: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: Any, filepath: str, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class SparkParquet(IO):
    """Reads, and writes Spark DataFrames to / from Parquet storage format.."""

    def read(self, filepath: str, cores: int = 18) -> pyspark.sql.DataFrame:
        """Reads a Spark DataFrame from Parquet file resource

        Args:
            filepath (str): The path to the parquet file resource

        Returns:
            Spark DataFrame
        """

        if os.path.exists(filepath):
            local = "local[" + str(cores) + "]"
            spark = SparkSession.builder.master(local).appName("Read SparkParquet").getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            return spark.read.parquet(filepath)

        else:
            logger.error("File {} was not found.".format(filepath))
            raise FileNotFoundError()

    def write(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        header: bool = True,
        mode: str = "overwrite",
        **kwargs,
    ) -> None:
        """Writes Spark DataFrame to Parquet file resource

        Args:
            data (pyspark.sql.DataFrame): Spark DataFrame to write
            filepath (str): The path to the parquet file to be written
            header (bool): True if data contains header row. False otherwise.
            mode (str): 'overwrite' or 'append'. Default is 'overwrite'.
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        data.write.option("header", header).mode(mode).parquet(filepath)


# ------------------------------------------------------------------------------------------------ #
#                                        SPARK                                                     #
# ------------------------------------------------------------------------------------------------ #
class SparkCSV(IO):
    """IO using the Spark API"""

    def read(
        self,
        filepath: str,
        cores: int = 18,
        header: bool = True,
        infer_schema: bool = True,
        sep: str = ",",
    ) -> pyspark.sql.DataFrame:
        """Reads a Spark DataFrame from Parquet file resource

        Args:
            filepath (str): The path to the parquet file resource
            cores (int): The number of CPU cores to designated to the operation. Default = 18
            header (bool): True if the data contains a header row. Default = True
            infer_schema (bool): True if the data types should be inferred. Default = False
            sep (str): Column delimiter. Default = ","

        Returns:
            Spark DataFrame
        """

        if os.path.exists(filepath):
            local = "local[" + str(cores) + "]"
            spark = SparkSession.builder.master(local).appName("Read SparkCSV").getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            return spark.read.options(header=header, delimiter=sep, inferSchema=infer_schema).csv(
                filepath
            )
        else:
            logger.error("File {} was not found.".format(filepath))
            raise FileNotFoundError()

    def write(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        header: bool = True,
        sep: str = ",",
        mode: str = "overwrite",
    ) -> None:
        """Writes Spark DataFrame to Parquet file resource

        Args:
            data (pyspark.sql.DataFrame): Spark DataFrame to write
            filepath (str): The path to the parquet file to be written
            header (bool): True if data contains header. Default = True
            sep (str): Column delimiter. Default = ","
            mode (str): 'overwrite' or 'append'. Default = 'overwrite'.
        """

        data.write.csv(path=filepath, header=header, sep=sep, mode=mode)
