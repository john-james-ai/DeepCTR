#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /params.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 19th 2022 06:43:34 pm                                                  #
# Modified   : Thursday May 19th 2022 06:43:35 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #
#                                DATA PARAMETER OBJECT                                             #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetParams:
    """Base class for application parameter objects used to communicate with the DAL."""

    datasource: str  # data source, i.e. 'alibaba', 'criteo', 'avazu'
    dataset: str  # The specific data collection
    stage: str  # Data processing stage, i.e 'raw', 'staged', 'interim', 'clean', 'processed'
    home: str  # The top most directory containing data.


@dataclass
class FileParams(DatasetParams):
    """Base class for application parameter objects used to communicate with the DAL."""

    filename: str = None  # The name of the file within the dataset.
    format: str = None  # Storage format, either 'csv', 'pickle', or 'parquet' supported


@dataclass
class S3Params:
    """Parameter object for Amazon S3 operations."""

    bucket: str  # The name of the S3 bucket
    folder: str  # The folder within the S3 bucket containing the data.
    object: str = None  # The object key for an S3 resource
