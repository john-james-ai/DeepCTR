#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /file.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Sunday May 15th 2022 11:22:36 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module defines the API for data access and management."""
import os
import logging
import logging.config
from typing import Any, Union
import shutil

from deepctr.dal.base import DAO
from deepctr.dal.params import DatasetParams
from deepctr.data.file import SparkCSV, SparkParquet, Pickler
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                  FILE ACCESS OBJECT                                              #
# ------------------------------------------------------------------------------------------------ #


class FileAccessObject(DAO):
    """Access object for files."""

    # -------------------------------------------------------------------------------------------- #
    def create(self, params: DatasetParams, data: Any, force: bool = False) -> None:
        """Persists a new data table to storage.

        Args:
            params (DatasetParams): Parameter object for create operations
        """
        filepath = DAO.__filepath.get_path(params)
        if os.path.exists(filepath) and not force:
            raise FileExistsError("{} already exists.".format(filepath))

        io = self._get_io(file_format=params.format)
        io.write(data=data, filepath=filepath)

    # -------------------------------------------------------------------------------------------- #
    def read(self, params: DatasetParams) -> Any:
        """Obtains an object from persisted storage

        Args:
            params (DatasetParams): Parameter object for file read operations

        Returns (DataFrame)
        """
        filepath = DAO.__filepath.get_path(params)

        try:
            io = self._get_io(file_format=params.format)
            return io.read(filepath=filepath)
        except FileNotFoundError as e:
            logger.error("File {} not found.".format(filepath))
            raise FileNotFoundError(e)

    # -------------------------------------------------------------------------------------------- #
    def delete(self, params: DatasetParams) -> None:
        """Removes a data table from persisted storage

        Args:
            params (DatasetParams): Parameter object for dataasets or data files
        """
        filepath = DAO.__filepath.get_path(params)
        shutil.rmtree(filepath, ignore_errors=True)

    # -------------------------------------------------------------------------------------------- #
    def _get_io(self, file_format: str) -> Union[SparkCSV, SparkParquet, Pickler]:
        if "csv" in file_format:
            io = SparkCSV()
        elif "parquet" in file_format:
            io = SparkParquet()
        elif "pickle" in file_format:
            io = Pickler()
        else:
            raise ValueError("File format {} is not supported.".format(file_format))
        return io
