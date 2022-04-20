#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /extractor.py                                                                         #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, February 14th 2022, 12:32:13 pm                                               #
# Modified : Tuesday, April 19th 2022, 8:08:15 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
"""Alibaba data acquisition and preparation module. """
import os
import boto3
import pandas as pd
import progressbar
from typing import Any
from botocore.exceptions import NoCredentialsError

from deepctr.operators.base import Operator
from deepctr.utils.decorators import operator

# ------------------------------------------------------------------------------------------------ #
CREDENTIALS_FILEPATH = "config/credentials.yml"


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

        self._bucket = params["bucket"]
        self._folder = params["folder"]
        self._destination = params["destination"]
        self._resource = params["resource"]
        self._force = params["force"]

        self._progressbar = None

    @operator
    def execute(self, data: Any = None, context: dict = None) -> pd.DataFrame:

        if len(os.listdir(self._destination)) != 4 or self._force:

            resource_type, resource = self._params['resource']

            s3access = context.get(self._resource_type).get("key")
            s3password = context.get(self._resource_type).get("password")

            object_keys = self._list_bucket_contents()
            self._s3 = boto3.client(
                "s3", aws_access_key_id=s3access, aws_secret_access_key=s3password
            )

            os.makedirs(self._destination, exist_ok=True)

            for object_key in object_keys:
                destination = os.path.join(self._destination, os.path.basename(object_key))

                if not os.path.exists(destination) or self._force:
                    self._download(object_key, destination)

    def _list_bucket_contents(self) -> list:
        """Returns a list of objects in the designated bucket"""

        objects = []
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self._bucket)
        for object in bucket.objects.filter(Delimiter="/t", Prefix=self._folder):
            if not object.key.endswith("/"):  # Skip objects that are just the folder name
                objects.append(object.key)

        return objects

    def _download(self, object_key: str, destination: str) -> None:
        """Downloads object designated by the object ke if not exists or force is True"""

        response = self._s3.head_object(Bucket=self._bucket, Key=object_key)
        size = response["ContentLength"]

        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        try:
            self._s3.download_file(
                self._bucket, object_key, destination, Callback=self._download_callback
            )

        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(self._bucket)
            raise NoCredentialsError(msg)

    def _download_callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)
