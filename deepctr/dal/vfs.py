#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /vfs.py                                                                             #
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

from deepctr.dal.base import FAO
from deepctr.dal.file import File
from deepctr.data.datastore import SparkCSV, SparkParquet, Pickler
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                       FILE MANAGER                                               #
# ------------------------------------------------------------------------------------------------ #


class FileManager(FAO):
    """File operations including, creating, reading/loading, and deleting."""

    # -------------------------------------------------------------------------------------------- #
    def create(self, file: File, data: Any, force: bool = False) -> None:
        """Persists a new data table to storage.

        Args:
            file (File): Parameter object for create operations
        """
        if os.path.exists(file.filepath) and not force:
            raise FileExistsError(
                "{} already exists. Create aborted. To overwrite, set force = True.".format(
                    file.filepath
                )
            )

        io = self._get_io(file_format=file.format)
        io.write(data=data, filepath=file.filepath)

    # -------------------------------------------------------------------------------------------- #
    def read(self, file: File) -> Any:
        """Obtains an object from persisted storage

        Args:
            file (File): Parameter object for file read operations

        Returns (DataFrame)
        """

        try:
            io = self._get_io(file_format=file.format)
            return io.read(filepath=file.filepath)
        except FileNotFoundError as e:
            logger.error("File {} not found.".format(file.filepath))
            raise FileNotFoundError(e)

    # -------------------------------------------------------------------------------------------- #
    def delete(self, file: File) -> None:
        """Removes a data table from persisted storage

        Args:
            file (File): Parameter object for dataasets or data files
        """
        shutil.rmtree(file.filepath, ignore_errors=True)

    # -------------------------------------------------------------------------------------------- #
    def exists(self, file: File) -> None:
        """Checks existence of Dataset

        Args:
            file (File): Parameter object for dataasets or data files
        """
        return os.path.exists(file.filepath)

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
