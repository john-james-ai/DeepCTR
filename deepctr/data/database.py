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
import tarfile
import pickle
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
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                          COMPRESSION                                             #
# ------------------------------------------------------------------------------------------------ #


class Compression(ABC):
    """Abstract base class for data compression classes."""

    def compress(self, source: str, destination: str) -> None:
        pass

    def expand(self, source: str, destination: str) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
class TarGZ(Compression):
    """Batch compress and expand Tar and Gzip files. """

    def compress(self, source: str, destination: str) -> None:
        """Compress file or directorinto a Tar GZ archive

        Args:
            source (str): Filepath or directory of files to compress.
            destination (str): The path for the output Tar GZ File
        """

        if os.path.isfile(source):
            self._compress_file(source, destination)
        else:
            self._compress_directory(source, destination)

    def _compress_file(self, source: str, destination: str) -> None:

        with tarfile.open(destination, "w:gz") as tar:
            tar.add(source, arcname=os.path.basename(source))

    def _compress_directory(self, source: str, destination: str) -> None:
        files = os.listdir(source)
        with tarfile.open(destination, "w:gz") as tar:
            for file in files:
                file = os.path.join(os.path.dirname(source), file)
                tar.add(file, arcname=os.path.basename(file))

    def expand(self, source: str, destination: str, force: str = False) -> None:
        """Expands a Tar Gz archive to the designated destination directory

        Args:
            source (str): A tar gzip archive
            destination (str): The directory into which the files will be expanded.
        """
        os.makedirs(destination, exist_ok=True)
        with tarfile.open(source, "r:gz") as tar:
            names = tar.getnames()
            for name in names:
                filepath = os.path.join(destination, name)
                if os.path.exists(filepath) and not force:
                    logger.warning(
                        "\tFile {} already exists. To overwrite, set force = True".format(name)
                    )
                else:
                    tar.extract(member=name, path=destination)

    def _exists(self, destination) -> bool:
        """Returns true if the directory exists and is non-empty, returns False otherwise."""
        num_files = len(os.listdir(destination))
        return num_files > 0


# ------------------------------------------------------------------------------------------------ #
#                                             CLOUD                                                #
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


# ------------------------------------------------------------------------------------------------ #
#                                              S3                                                  #
# ------------------------------------------------------------------------------------------------ #
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

        load_dotenv()
        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")

        s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)

        MB = 1024 ** 2
        config = TransferConfig(
            max_concurrency=20,
            multipart_chunksize=16 * MB,
            multipart_threshold=64 * MB,
            max_bandwidth=50 * MB,
        )

        size = os.path.getsize(filepath)
        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

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
            filepath = os.path.join(directory, file)
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
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if os.path.exists(filepath) and not force:
            logger.warning(
                "\tS3 File {} already exists and force = False; therefore, the upload was skipped.".format(
                    filepath
                )
            )

        else:

            load_dotenv()

            S3_ACCESS = os.getenv("S3_ACCESS")
            S3_PASSWORD = os.getenv("S3_PASSWORD")

            s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)

            self._download(s3, bucket, object, filepath)

    def download_directory(
        self, bucket: str, folder: str, directory: str, force: bool = False
    ) -> None:
        """Downloads a group of files from an S3 folder to a local directory

        Args:
            bucket (str): The S3 bucket from which the file will be downloaded
            folder (str): The S3 folder containing the files to download.
            directory (str): The destination directory to which the files will be downloaded.
        """
        os.makedirs(directory, exist_ok=True)

        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")

        object_keys = self._list_bucket_contents(bucket, folder)

        s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)

        for object_key in object_keys:
            filepath = os.path.join(directory, os.path.basename(object_key))
            if os.path.exists(filepath) and not force:
                logger.warning(
                    "\tObject {} already exists. To force download, set force=True.".format(
                        object_key
                    )
                )
            else:
                self._download(s3, bucket, object_key, filepath)

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
            s3.download_file(bucket, object, filepath, Callback=self._callback, Config=config)

        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(bucket)
            raise NoCredentialsError(msg)

    def delete_object(self, bucket: str, object: str) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str): The S3 bucket name
            object (str): The S3 object key
        """
        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")
        s3 = boto3.resource("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD,)
        s3.Object(bucket, object).delete()

    def delete_folder(self, bucket: str, folder: str) -> None:
        """Deletes a object from S3 storage

        Args:
            bucket (str): The S3 bucket name
            folder (str): The S3 folder with trailing backslash
        """
        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")
        s3 = boto3.resource("s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD)
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=folder).delete()

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
#                                         SPARK PARQUET                                            #
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
#                                          SPARK CSV                                               #
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


# ------------------------------------------------------------------------------------------------ #
#                                       PICKLER                                                    #
# ------------------------------------------------------------------------------------------------ #
class Pickler(IO):
    """Wrapper for Pickle class"""

    def read(self, filepath: str) -> Any:
        """Loads serialized data

        Args:
            filepath (str): Path to the serialized resource
        """

        with open(filepath, "rb") as f:
            return pickle.load(f)

    def write(self, data: Any, filepath: str) -> None:
        """Serializes the data using the python pickle module.

        Args:
            data (Any): The data to be serialized
            filepath (str): Path to the serialized resource
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "wb") as f:
            pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
