#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /fao.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Thursday June 23rd 2022 09:31:57 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module defines the API for data access and management."""
from abc import ABC, abstractmethod
import os
import inspect
import logging
import logging.config
from typing import Any, Union
import shutil

from deepctr.dal import IO
from deepctr.dal.base import File  # , Dataset
from deepctr.data.local import SparkCSV, SparkParquet, Pickler
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ================================================================================================ #
#                                            FAO                                                   #
# ================================================================================================ #


class FAOBase(ABC):
    """Base class for file managers."""

    __io = {"csv": SparkCSV, "parquet": SparkParquet, "pickle": Pickler}

    @abstractmethod
    def create(self, file: File, data: Any, force: bool = False) -> None:
        pass
        """Persists a new data table to storage.

        Args:
            file (File): File object
        """
        pass

    @abstractmethod
    def read(self, file: File) -> Any:
        """Obtains an object from persisted storage

        Args:
            file (File): Parameter object for file read operations

        Returns (DataFrame)
        """
        pass

    @abstractmethod
    def delete(self, file: File) -> None:
        """Removes a data table from persisted storage

        Args:
            file (File): Parameter object for dataasets or data files
        """
        pass

    @abstractmethod
    def exists(self, file: File) -> None:
        """Checks existence of Dataset

        Args:
            file (File): Parameter object for dataasets or data files
        """
        pass

    def _get_io(self, format: str) -> Union[SparkCSV, SparkParquet]:
        try:
            return IO[format.replace(".", "")]
        except KeyError as e:
            classname = self.__class__.__name__
            method = inspect.stack()[1][3]
            msg = "Error in {}: {}. Invalid format: {}\n{}".format(classname, method, format, e)
            logger.error(msg)
            raise ValueError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                    LOCAL FILE MANAGER                                            #
# ------------------------------------------------------------------------------------------------ #


class FAO(FAOBase):
    """File operations for local files."""

    # -------------------------------------------------------------------------------------------- #
    def create(self, file: File, data: Any, force: bool = False) -> None:
        """Persists a new data table to storage.

        Args:
            file (File): Parameter object for create operations
        """
        if os.path.exists(file.filepath) and not force:
            msg = "{} already exists. Create aborted. To overwrite, set force = True.".format(
                file.filepath
            )
            logger.error(msg)
            raise FileExistsError(msg)

        io = self._get_io(format=file.format)
        io.write(data=data, filepath=file.filepath)

    # -------------------------------------------------------------------------------------------- #
    def read(self, file: File) -> Any:
        """Obtains an object from persisted storage

        Args:
            file (File): Parameter object for file read operations

        Returns (DataFrame)
        """

        io = self._get_io(format=file.format)
        return io.read(filepath=file.filepath)

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
