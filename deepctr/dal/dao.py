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
# Modified   : Friday June 24th 2022 01:25:00 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging
import pandas as pd
from pymysql.connections import Connection

from deepctr import Entity
from deepctr.dal.base import File
from deepctr.data.database import Database
from deepctr.dal.sequel import FileSQL  # , DagSQL, TaskSQL,  S3FileSQL
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ================================================================================================ #
#                                          DAO                                                     #
# ================================================================================================ #


class DAO(ABC):
    """Table level access to the database."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._connection.begin()
        self._database = Database(self._connection)

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

    # -------------------------------------------------------------------------------------------- #
    def begin_transaction(self) -> None:
        self._connection.begin()

    # -------------------------------------------------------------------------------------------- #
    def rollback(self) -> None:
        self._connection.rollback()

    # -------------------------------------------------------------------------------------------- #
    def commit(self) -> None:
        self._connection.commit()


# # ================================================================================================ #
# #                                         DAGDAO                                                   #
# # ================================================================================================ #


# class DagDAO(DAO):
#     """Provides access to dag table ."""

#     def __init__(self, connection: Connection) -> None:
#         self.sql = DagSQL()
#         super(DagDAO, self).__init__(connection)

#     def add(self, entity: Entity) -> Entity:
#         """Adds an entity to the database

#         Args:
#             entity (Entity): The entity to add to the database

#         Returns
#             Entity with the id updated with id from the database.
#         """
#         command = self.sql.insert(entity)
#         entity.id = self._database.insert(command.statement, command.parameters)
#         return entity

#     def find(self, id: int) -> Entity:
#         """Finds an entity based on the id and returns it as an entity dataclass.

#         Args:
#             id (int): The id that uniquely identifies an entity

#         Returns
#             Entity
#         """

#         command = self.sql.select(id)
#         entity = self._database.select_one(command.statement, command.parameters)
#         return self.factory(entity)

#     def findall(self, todf: bool = False) -> list:
#         """Returns all entities from the designated entity table."""
#         command = self.sql.select_all()
#         entities = self._database.select_all(command.statement)
#         if todf:
#             return pd.DataFrame.from_dict(entities)
#         return [self.factory(entity) for entity in entities]

#     def update(self, entity: Entity) -> None:
#         """Updates the entity

#         Args:
#             entity (Entity): The entity to update. The entity is overwritten.

#         """
#         command = self.sql.update(entity)
#         return self._database.execute(command.statement, command.parameters)

#     def delete(self, id: int) -> None:
#         """Removes an entity from the database based upon id

#         Args:
#             id (int): The unique identifier for the entity

#         """
#         command = self.sql.delete(id)
#         return self._database.execute(command.statement, command.parameters)

#     def factory(self, entity: Entity) -> Entity:
#         """Factory method to convert database result to an entity object.

#         Args:
#             entity (Entity): An entity object of the derived type
#         """
#         return DagEntity(
#             id=entity["id"],
#             name=entity["name"],
#             desc=entity["desc"],
#             n_tasks=entity["n_tasks"],
#             n_tasks_done=entity["n_tasks_done"],
#             created=entity["created"],
#             modified=entity["modified"],
#             started=entity["started"],
#             stopped=entity["stopped"],
#             duration=entity["duration"],
#             return_code=entity["return_code"],
#         )


# # ================================================================================================ #
# #                                        TASKDAO                                                   #
# # ================================================================================================ #
# class TaskDAO(DAO):
#     """Provides access to task table ."""

#     def __init__(self, connection: Connection) -> None:
#         self.sql = TaskSQL()
#         super(TaskDAO, self).__init__(connection)

#     def add(self, entity: Entity) -> Entity:
#         """Adds an entity to the database

#         Args:
#             entity (Entity): The entity to add to the database

#         Returns
#             Entity with the id updated with id from the database.
#         """
#         command = self.sql.insert(entity)
#         entity.id = self._database.insert(command.statement, command.parameters)
#         return entity

#     def find(self, id: int) -> Entity:
#         """Finds an entity based on the id and returns it as an entity dataclass.

#         Args:
#             id (int): The id that uniquely identifies an entity

#         Returns
#             Entity
#         """

#         command = self.sql.select(id)
#         entity = self._database.select_one(command.statement, command.parameters)
#         return self.factory(entity)

#     def findall(self, todf: bool = False) -> list:
#         """Returns all entities from the designated entity table."""
#         command = self.sql.select_all()
#         entities = self._database.select_all(command.statement)
#         if todf:
#             return pd.DataFrame.from_dict(entities)
#         return [self.factory(entity) for entity in entities]

#     def update(self, entity: Entity) -> None:
#         """Updates the entity

#         Args:
#             entity (Entity): The entity to update. The entity is overwritten.

#         """
#         command = self.sql.update(entity)
#         return self._database.execute(command.statement, command.parameters)

#     def delete(self, id: int) -> None:
#         """Removes an entity from the database based upon id

#         Args:
#             id (int): The unique identifier for the entity

#         """
#         command = self.sql.delete(id)
#         return self._database.execute(command.statement, command.parameters)

#     def factory(self, entity: Entity) -> Entity:
#         """Factory method to convert database result to an entity object.

#         Args:
#             entity (Entity): An entity object of the derived type
#         """
#         return TaskEntity(
#             id=entity["id"],
#             name=entity["name"],
#             desc=entity["desc"],
#             seq=entity["seq"],
#             dag_id=entity["dag_id"],
#             created=entity["created"],
#             modified=entity["modified"],
#             started=entity["started"],
#             stopped=entity["stopped"],
#             duration=entity["duration"],
#             return_code=entity["return_code"],
#         )


# ================================================================================================ #
#                                      LOCALFILE DAO                                               #
# ================================================================================================ #
class FileDAO(DAO):
    """Provides access to localfile table ."""

    def __init__(self, connection: Connection) -> None:
        self.sql = FileSQL()
        super(FileDAO, self).__init__(connection)

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the database

        Args:
            entity (Entity): The entity to add to the database

        Returns
            Entity with the id updated with id from the database.
        """
        command = self.sql.insert(entity)
        entity.id = self._database.insert(command.statement, command.parameters)
        return entity

    def find(self, id: int) -> Entity:
        """Finds an entity based on the id and returns it as an entity dataclass.

        Args:
            id (int): The id that uniquely identifies an entity

        Returns
            Entity
        """

        command = self.sql.select(id)
        entity = self._database.select_one(command.statement, command.parameters)
        return self.factory(entity)

    def findall(self, todf: bool = False) -> list:
        """Returns all entities from the designated entity table."""
        command = self.sql.select_all()
        entities = self._database.select_all(command.statement)
        if todf:
            return pd.DataFrame.from_dict(entities)
        return [self.factory(entity) for entity in entities]

    def update(self, entity: Entity) -> None:
        """Updates the entity

        Args:
            entity (Entity): The entity to update. The entity is overwritten.

        """
        command = self.sql.update(entity)
        return self._database.execute(command.statement, command.parameters)

    def delete(self, id: int) -> None:
        """Removes an entity from the database based upon id

        Args:
            id (int): The unique identifier for the entity

        """
        command = self.sql.delete(id)
        return self._database.execute(command.statement, command.parameters)

    def factory(self, entity: Entity) -> Entity:
        """Factory method to convert database result to an entity object.

        Args:
            entity (Entity): An entity object of the derived type
        """
        file = File(
            id=entity["id"],
            name=entity["name"],
            source=entity["source"],
            dataset=entity["dataset"],
            storage_type=entity["storage_type"],
            format=entity["format"],
            stage_id=entity["stage_id"],
            stage_name=entity["stage_name"],
            home=entity["home"],
            bucket=entity["bucket"],
            filepath=entity["filepath"],
            compressed=entity["compressed"],
            size=entity["size"],
            created=entity["created"],
        )
        if file.compressed == 1:
            file.compressed = True
        else:
            file.compressed = False
        return file
