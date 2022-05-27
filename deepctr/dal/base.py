#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Entityname   : /base.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 19th 2022 07:48:15 pm                                                  #
# Modified   : Thursday May 26th 2022 08:55:25 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Union
from deepctr.dal.dto import DTO


# ------------------------------------------------------------------------------------------------ #
#                                     ABSTRACT COMMAND                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AbstractCommand:
    command: str
    table: str
    parameters: list
    sequel: str


# ------------------------------------------------------------------------------------------------ #
#                                    ENTITY CLASSES                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Entity(ABC):
    """Base class for File classes including the members and validation common to all subclasses."""

    @abstractmethod
    def to_dict(self) -> dict:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                            FAO                                                   #
# ------------------------------------------------------------------------------------------------ #


class FAO:
    """Base class for file managers."""

    @abstractmethod
    def create(self, file: Entity, data: Any, force: bool = False) -> None:
        pass
        """Persists a new data table to storage.

        Args:
            file (Entity): Parameter object for create operations
        """
        pass

    @abstractmethod
    def read(self, file: Entity) -> Any:
        """Obtains an object from persisted storage

        Args:
            file (Entity): Parameter object for file read operations

        Returns (DataFrame)
        """
        pass

    @abstractmethod
    def delete(self, file: Entity) -> None:
        """Removes a data table from persisted storage

        Args:
            file (Entity): Parameter object for dataasets or data files
        """
        pass

    @abstractmethod
    def exists(self, file: Entity) -> None:
        """Checks existence of Dataset

        Args:
            file (Entity): Parameter object for dataasets or data files
        """
        pass


# ------------------------------------------------------------------------------------------------ #
#                                          DAO                                                     #
# ------------------------------------------------------------------------------------------------ #


class DAO(ABC):
    """Table level access to the database."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def create(self, data: Union[dict, DTO]) -> Entity:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def add(self, entity: Entity) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def find(self, id: int) -> Entity:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def find_by_key(self, name: str, datasource: str, **kwargs) -> Entity:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def find_by_column(self, column: str, value: Any) -> Union[list, Entity]:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def findall(self) -> dict:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def remove(self, id: int) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def exists(self, name: str, datasource: str, **kwargs) -> bool:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def rollback(self) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def save(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                        RAO                                                       #
# ------------------------------------------------------------------------------------------------ #


class RAO(ABC):
    """Defines interface for remote access objects accessing cloud services."""

    @abstractmethod
    def download(self, source: Entity, destination: Entity, force: bool = False) -> None:
        pass

    @abstractmethod
    def upload(self, source: Entity, destination: Entity, force: bool = False) -> None:
        pass
