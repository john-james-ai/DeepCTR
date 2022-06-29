#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /dao.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 21st 2022 11:10:43 pm                                                  #
# Modified   : Tuesday June 28th 2022 06:28:23 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging

from deepctr.dal.base import Entity
from deepctr.dal.context import DBContext
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ================================================================================================ #
#                                          DAO                                                     #
# ================================================================================================ #


class DAOBase(ABC):
    """Base class for the DAO class.

    Args:
        database (Database): Database instantiated with a connection.
        mapper (EntityMapper): Maps Entity objects to Database (SQL) and back to Entity objects.
    """

    def __init__(self, dbcontext: DBContext) -> None:
        self._dbcontext = dbcontext

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def add(self, entity: Entity) -> None:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def find(self, id: int) -> dict:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def findall(self, todf: bool = False) -> list:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def update(self, entity: Entity) -> int:
        pass

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def delete(self, id: int) -> int:
        pass


# ================================================================================================ #
#                                           DAO                                                    #
# ================================================================================================ #
class DAO(DAOBase):
    """Provides access to localfile table ."""

    def __init__(self, dbcontext: DBContext) -> None:
        super(DAO, self).__init__(dbcontext=dbcontext)
        self._database = self._dbcontext.database
        self._mapper = self._dbcontext.mapper

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the database

        Args:
            entity (Entity): The entity to add to the database

        Returns
            Entity with the id updated with id from the database.
        """
        command = self._mapper.insert(entity)
        entity.id = self._database.insert(command.statement, command.parameters)
        return entity

    def find(self, id: int) -> dict:
        """Finds an entity based on the id and returns it as an entity dataclass.

        Args:
            id (int): The id that uniquely identifies an entity

        Returns a dictionary containing data for the specified row.
        """

        command = self._mapper.select(id)
        record = self._database.select_one(command.statement, command.parameters)
        if record is None:
            return record
        else:
            return self._mapper.factory(record)

    def findall(self, todf: bool = False) -> list:
        """Returns all entities from the designated entity table."""
        command = self._mapper.select_all()
        entities = []
        records = self._database.select_all(command.statement)
        for record in records:
            entity = self._mapper.factory(record)
            entities.append(entity)
        return entities

    def update(self, entity: Entity) -> None:
        """Updates the entity

        Args:
            entity (Entity): The entity to update. The entity is overwritten.

        """
        command = self._mapper.update(entity)
        return self._database.execute(command.statement, command.parameters)

    def delete(self, id: int) -> None:
        """Removes an entity from the database based upon id

        Args:
            id (int): The unique identifier for the entity

        """
        command = self._mapper.delete(id)
        return self._database.execute(command.statement, command.parameters)

    def exists(self, id: int) -> None:
        """Returns true if the entity with id exists in the database and returns False otherwise.

        Args:
            id (int): The unique identifier for the entity

        """
        command = self._mapper.select(id)
        result = self._database.execute(command.statement, command.parameters)
        return len(result) == 0
