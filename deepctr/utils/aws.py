#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /aws.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 25th 2022 12:05:22 am                                                 #
# Modified   : Friday June 17th 2022 06:31:18 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging
import boto3
from botocore.exceptions import ClientError
import os

# ------------------------------------------------------------------------------------------------ #
def upload_file(filepath, bucket, object_key=None):
    """Upload a file to an S3 bucket

    Args:
        filepath (str): Path to file to upload
        bucket (str): The bucket to upload into
        object_key (str): Path to the object in the bucket (optional, defaults to base of filepath)

    Returns:
        True if file was uploaded, else False

    Source: "https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html"
    """

    # If S3 object_key was not specified, use filepath
    if object_key is None:
        object_key = os.path.basename(filepath)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        _ = s3_client.upload_file(filepath, bucket, object_key)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_file(bucket, object_key) -> None:
    """Deletes an S3 file.

    Args:
        bucket (str): The name of the bucket
        object_key (str): The path to the object
    """

    s3 = boto3.resource("s3")
    s3.Object(bucket, object_key).delete()
