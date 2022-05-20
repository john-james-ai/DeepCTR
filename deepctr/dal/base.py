#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /base.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 19th 2022 07:48:15 pm                                                  #
# Modified   : Thursday May 19th 2022 07:48:15 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from abc import ABC, abstractmethod
from typing import Any
from difflib import get_close_matches
from deepctr.dal.params import DatasetParams, S3Params, FileParams

# ------------------------------------------------------------------------------------------------ #
#                                          PATH                                                    #
# ------------------------------------------------------------------------------------------------ #


class Path(ABC):
    "Base class for file and directory path sublasses responsible for mapping objects to files."

    __stages = ["raw", "staged", "interim", "clean", "processed", "extract"]
    __datasource = ["alibaba", "avazu", "criteo"]
    __formats = ["csv", "parquet", "pickle"]

    @staticmethod
    def get_path(self, params: DatasetParams) -> str:
        pass


# ------------------------------------------------------------------------------------------------ #
class FilePath(Path):
    """Responsible for mapping file parameter objects to filepaths."""

    @staticmethod
    def get_path(params: DatasetParams) -> str:
        try:
            datasource = get_close_matches(params.datasource, Path.__datasource)[0]
            stage = get_close_matches(params.stage, Path.__stages)[0]
            format = get_close_matches(params.format, Path.__formats)[0]
            return (
                os.path.join(params.home, datasource, params.dataset, stage, params.entity)
                + "."
                + format
            )

        except IndexError as e:
            raise ValueError("Unable to parse dataset configuration. {}".format(e))


# ------------------------------------------------------------------------------------------------ #
class Directory(Path):
    """Responsible for mapping directory parameter objects to directories."""

    @staticmethod
    def get_path(params: DatasetParams) -> str:
        try:
            datasource = get_close_matches(params.datasource, Path.__datasource)[0]
            stage = get_close_matches(params.stage, Path.__stages)[0]
            return os.path.join(params.home, datasource, params.dataset, stage)

        except IndexError as e:
            raise ValueError("Unable to parse dataset configuration. {}".format(e))


# ------------------------------------------------------------------------------------------------ #
#                                          DAO                                                     #
# ------------------------------------------------------------------------------------------------ #


class DAO(ABC):
    """Defines base class for data access objects."""

    __filepath = FilePath
    __directory = Directory

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def create(self, params: DatasetParams, data: Any, force: bool = False) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def read(self, params: DatasetParams) -> Any:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def delete(self, params: DatasetParams) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def exists(self, params: Any) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                        RAO                                                       #
# ------------------------------------------------------------------------------------------------ #


class RAO(ABC):
    """Defines interface for remote access objects accessing cloud services."""

    __filepath = FilePath
    __directory = Directory

    @abstractmethod
    def download_entity(
        self, source: S3Params, destination: FileParams, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def download_dataset(
        self, source: S3Params, destination: DatasetParams, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def upload_entity(self, source: FileParams, destination: S3Params, force: bool = False) -> None:
        pass

    @abstractmethod
    def upload_dataset(
        self, source: DatasetParams, destination: S3Params, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def compress(
        self, source: DatasetParams, destination: DatasetParams, force: bool = False
    ) -> None:
        pass

    @abstractmethod
    def expand(
        self, source: DatasetParams, destination: DatasetParams, force: bool = False
    ) -> None:
        pass
