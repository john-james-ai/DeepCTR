#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /data_operators.py                                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 10th 2022 03:30:15 pm                                                   #
# Modified   : Friday May 13th 2022 10:31:59 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import os
from typing import Any
from dotenv import load_dotenv
import tarfile
import pandas as pd
import logging
import logging.config
import progressbar
import boto3
from botocore.exceptions import NoCredentialsError


from deepctr.utils.decorators import operator
from deepctr.dag.base import Operator
from deepctr.persistence.dal import DataTableDTO, DataTableDAO
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                    EXTRACT S3                                                    #
# ------------------------------------------------------------------------------------------------ #


class ExtractS3(Operator):
    """ExtractS3 operator downloads data from Amazon S3 Resources.

    Args:
        task_no (int): Task sequence in dag.
        task_name (str): name of task
        params (dict): Parameters required by the task, including:
          bucket (str): The Amazon S3 bucket name
          key (str): The access key to the S3 bucket
          password (str): The secret access key to the S3 bucket
          folder (str): The folder within the bucket for the data
          destination (str): The folder to which the data is downloaded
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: dict) -> None:
        super(ExtractS3, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

        self._task_no = task_no
        self._task_name = task_name
        self._task_description = task_description

        self._bucket = params.get("bucket")
        self._folder = params.get("folder")
        self._destination = params.get("destination")
        self._force = params["force"]

        self._progressbar = None

    @operator
    def execute(self, data: Any = None) -> pd.DataFrame:
        """Extracts data from an Amazon AWS S3 resource and persists it."""

        load_dotenv()

        S3_ACCESS = os.getenv("S3_ACCESS")
        S3_PASSWORD = os.getenv("S3_PASSWORD")

        object_keys = self._list_bucket_contents()
        self._s3 = boto3.client(
            "s3", aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_PASSWORD
        )

        os.makedirs(self._destination, exist_ok=True)

        for object_key in object_keys:
            filepath = os.path.join(self._destination, os.path.basename(object_key))
            if self._force or not os.path.exists(filepath):
                self._download(object_key, filepath)
            else:
                logger.info(
                    "File {} not downloaded. It already exists".format(os.path.basename(filepath))
                )

    def _list_bucket_contents(self) -> list:
        """Returns a list of objects in the designated bucket"""

        objects = []
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self._bucket)
        for object in bucket.objects.filter(Delimiter="/t", Prefix=self._folder):
            if not object.key.endswith("/"):  # Skip objects that are just the folder name
                objects.append(object.key)

        return objects

    def _download(self, object_key: str, filepath: str) -> None:
        """Downloads object designated by the object ke if not exists or force is True"""

        response = self._s3.head_object(Bucket=self._bucket, Key=object_key)
        size = response["ContentLength"]

        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        try:
            self._s3.download_file(
                self._bucket, object_key, filepath, Callback=self._download_callback
            )

        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(self._bucket)
            raise NoCredentialsError(msg)

    def _download_callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)


# ------------------------------------------------------------------------------------------------ #
#                                    EXPAND GZ                                                     #
# ------------------------------------------------------------------------------------------------ #


class ExpandGZ(Operator):
    """Expandes a gzip archive, stores the raw data

    Args:
        task_no (int): Task sequence in dag.
        task_name (str): name of task
        params (dict): Parameters required by the task, including:
          source (str): The source directory containing the gzip files
          destination (str): The directory into which the Expanded data is to be stored
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, task_no: int, task_name: str, task_description: str, params: dict) -> None:
        super(ExpandGZ, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

        self._source = params["source"]
        self._destination = params["destination"]
        self._force = params["force"]

    @operator
    def execute(self, data: Any = None) -> pd.DataFrame:
        """Executes the Expand operation.

        Args:
            data (pd.DataFrame): None. This method takes no parameter
        """

        # Create destination if it doesn't exist
        os.makedirs(self._destination, exist_ok=True)

        # Only runs if destination directory is empty, unless force is True
        if self._destination_empty_or_force():
            filenames = os.listdir(self._source)
            for filename in filenames:
                filepath = os.path.join(self._source, filename)
                tar = tarfile.open(filepath, "r:gz")
                tar.extractall(self._destination)
                tar.close()
        else:
            logger.info(
                "Files not expanded. Raw data already exists at {}".format(self._destination)
            )

    def _destination_empty_or_force(self) -> bool:
        """Returns true if the file doesn't exist or force is True."""
        num_files = len(os.listdir(self._destination))
        return num_files == 0 or self._force


# ------------------------------------------------------------------------------------------------ #
#                                     DATA READER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataReader(Operator):
    """Reads data from the Data Repository"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(DataReader, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None) -> Any:
        """Reads from the designated resource"""
        dto = DataTableDTO(
            name=self._params["name"],
            dataset=self._params["dataset"],
            asset=self._params["asset"],
            stage=self._params["stage"],
            env=self._params["env"],
            format=self._params["format"],
        )
        dao = DataTableDAO()
        return dao.read(dto=dto)


# ------------------------------------------------------------------------------------------------ #
#                                     DATA WRITER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataWriter(Operator):
    """Reads data from the Data Repository"""

    def __init__(self, task_no: int, task_name: str, task_description: str, params: list) -> None:
        super(DataWriter, self).__init__(
            task_no=task_no, task_name=task_name, task_description=task_description, params=params
        )

    @operator
    def execute(self, data: Any = None) -> Any:
        """Reads from the designated resource"""
        dto = DataTableDTO(
            name=self._params["name"],
            dataset=self._params["dataset"],
            asset=self._params["asset"],
            stage=self._params["stage"],
            env=self._params["env"],
            format=self._params["format"],
        )
        dao = DataTableDAO()
        dao.create(dto=dto, data=data, force=self._params["force"])
