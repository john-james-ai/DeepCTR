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
# Modified   : Saturday May 28th 2022 12:29:38 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Union
from pymysql.connections import Connection
import logging
import logging.config

from deepctr.dal.dto import DTO
from deepctr.utils.decorators import tracer
from deepctr.data.database import Database
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


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

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._database = Database(connection)
        self._name = self.__class__.__name__.lower()
        logger.info("Instantiated {}".format(self._name))

    # -------------------------------------------------------------------------------------------- #
    @property
    def connection(self) -> Connection:
        return self._connection

    # -------------------------------------------------------------------------------------------- #
    @property
    def name(self) -> str:
        return self._name

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def create(self, data: Union[dict, DTO]) -> Entity:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def add(self, entity: Entity) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def find(self, id: int, todf: bool = False) -> Entity:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def find_by_column(self, column: str, value: Any, todf: bool = False) -> Union[list, Entity]:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def findall(self, todf: bool = False) -> dict:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def remove(self, id: int) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def exists(self, name: str, datasource: str, **kwargs) -> bool:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
    def rollback(self) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    @tracer
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
