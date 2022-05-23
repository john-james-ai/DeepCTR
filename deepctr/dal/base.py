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
from deepctr.dal.dto import S3DTO, EntityDTO, DatasetDTO
from deepctr.dal.entity import File, Entity

# ------------------------------------------------------------------------------------------------ #
#                                          PATH                                                    #
# ------------------------------------------------------------------------------------------------ #


class PathFinder(ABC):
    "Base class for file and directory path sublasses responsible for mapping objects to files."

    @staticmethod
    def get_path(self, dto: DatasetDTO) -> str:
        pass


# ------------------------------------------------------------------------------------------------ #
class DatasetPathFinder(PathFinder):
    """Responsible for mapping directory parameter objects to directories."""

    @staticmethod
    def get_path(name: str, datasource: str, stage: str, home: str = "data") -> str:
        return os.path.join(home, datasource, name, stage)


# ------------------------------------------------------------------------------------------------ #
class FilePathFinder(PathFinder):
    """Responsible for mapping directory parameter objects to directories."""

    @staticmethod
    def get_path(
        name: str, datasource: str, dataset: str, stage: str, format: str, home: str = "data"
    ) -> str:
        return os.path.join(home, datasource, dataset, stage, name) + "." + format


# ------------------------------------------------------------------------------------------------ #
class ObjectPathFinder(PathFinder):
    """Responsible for mapping file parameters to S3 object paths."""

    @staticmethod
    def get_path(object: str, bucket: str = "deepctr") -> str:
        return os.path.join(bucket, object)


# ------------------------------------------------------------------------------------------------ #
#                                            FAO                                                   #
# ------------------------------------------------------------------------------------------------ #


class FAO:
    """Base class for file managers."""

    @abstractmethod
    def create(self, file: File, data: Any, force: bool = False) -> None:
        pass
        """Persists a new data table to storage.

        Args:
            file (File): Parameter object for create operations
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


# ------------------------------------------------------------------------------------------------ #
#                                          DAO                                                     #
# ------------------------------------------------------------------------------------------------ #


class DAO(ABC):
    """Collection of Entity objects of a single type, along with database operations."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def add(self, entity: Entity) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def find(self, id: int) -> Any:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def findall(self) -> Any:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def remove(self, id: int) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def exists(self, id: int) -> bool:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                        RAO                                                       #
# ------------------------------------------------------------------------------------------------ #


class RAO(ABC):
    """Defines interface for remote access objects accessing cloud services."""

    @abstractmethod
    def download(self, source: S3DTO, destination: EntityDTO, force: bool = False) -> None:
        pass

    @abstractmethod
    def upload(self, source: EntityDTO, destination: S3DTO, force: bool = False) -> None:
        pass
