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
# Modified   : Saturday June 18th 2022 06:38:16 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging
import boto3
import botocore
from botocore.exceptions import ClientError, NoCredentialsError
import os
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
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


# ------------------------------------------------------------------------------------------------ #
def delete_file(bucket, object_key) -> None:
    """Deletes an S3 file.

    Args:
        bucket (str): The name of the bucket
        object_key (str): The path to the object
    """

    s3 = boto3.resource("s3")
    s3.Object(bucket, object_key).delete()


# ------------------------------------------------------------------------------------------------ #
def get_size_aws(bucket: str, object_key: str) -> int:
    """Checks the existence of an S3 object, then returns its size.

    Args:
        bucket (str): The name of the S3 bucket containing the target resource.
        object_key (str): The path to the resource within the bucket.

    Returns:
        int: The size of the resource in bytes if it exists.
    """
    s3 = boto3.client("s3")
    try:
        response = s3.head_object(Bucket=bucket, Key=object_key)
        return response["ContentLength"]
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            msg = "Object {} does not exist.".format(object_key)
            logger.error(msg)
            return 0
        else:
            logger.error(e)
            return 0

    except NoCredentialsError:
        msg = "Credentials not available for {} bucket".format(bucket)
        raise NoCredentialsError(msg)
