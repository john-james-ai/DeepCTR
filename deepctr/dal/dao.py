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
# Modified   : Sunday June 26th 2022 01:02:39 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging
import pandas as pd
from pymysql.connections import Connection

from deepctr import Entity
from deepctr.data.database import Database
from deepctr.dal.mapper import FileSQL, DatasetSQL  # , DagSQL, TaskSQL,  S3FileSQL
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ================================================================================================ #
#                                          DAO                                                     #
# ================================================================================================ #


class DAO(ABC):
    """Table level access to the database.

    Args:
        database (Database): Database instantiated with a connection.
    """

    def __init__(self, database: Database) -> None:
        self._database = database

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


# # ================================================================================================ #
# #                                         DAGDAO                                                   #
# # ================================================================================================ #


# class DagDAO(DAO):
#     """Provides access to dag table ."""

#     def __init__(self, connection: Connection) -> None:
#         self._sql = DagSQL()
#         super(DagDAO, self).__init__(connection)

#     def add(self, entity: Entity) -> Entity:
#         """Adds an entity to the database

#         Args:
#             entity (Entity): The entity to add to the database

#         Returns
#             Entity with the id updated with id from the database.
#         """
#         command = self._sql.insert(entity)
#         entity.id = self._database.insert(command.statement, command.parameters)
#         return entity

#     def find(self, id: int) -> Entity:
#         """Finds an entity based on the id and returns it as an entity dataclass.

#         Args:
#             id (int): The id that uniquely identifies an entity

#         Returns
#             Entity
#         """

#         command = self._sql.select(id)
#         entity = self._database.select_one(command.statement, command.parameters)
#         return self.factory(entity)

#     def findall(self, todf: bool = False) -> list:
#         """Returns all entities from the designated entity table."""
#         command = self._sql.select_all()
#         entities = self._database.select_all(command.statement)
#         if todf:
#             return pd.DataFrame.from_dict(entities)
#         return [self.factory(entity) for entity in entities]

#     def update(self, entity: Entity) -> None:
#         """Updates the entity

#         Args:
#             entity (Entity): The entity to update. The entity is overwritten.

#         """
#         command = self._sql.update(entity)
#         return self._database.execute(command.statement, command.parameters)

#     def delete(self, id: int) -> None:
#         """Removes an entity from the database based upon id

#         Args:
#             id (int): The unique identifier for the entity

#         """
#         command = self._sql.delete(id)
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
#         self._sql = TaskSQL()
#         super(TaskDAO, self).__init__(connection)

#     def add(self, entity: Entity) -> Entity:
#         """Adds an entity to the database

#         Args:
#             entity (Entity): The entity to add to the database

#         Returns
#             Entity with the id updated with id from the database.
#         """
#         command = self._sql.insert(entity)
#         entity.id = self._database.insert(command.statement, command.parameters)
#         return entity

#     def find(self, id: int) -> Entity:
#         """Finds an entity based on the id and returns it as an entity dataclass.

#         Args:
#             id (int): The id that uniquely identifies an entity

#         Returns
#             Entity
#         """

#         command = self._sql.select(id)
#         entity = self._database.select_one(command.statement, command.parameters)
#         return self.factory(entity)

#     def findall(self, todf: bool = False) -> list:
#         """Returns all entities from the designated entity table."""
#         command = self._sql.select_all()
#         entities = self._database.select_all(command.statement)
#         if todf:
#             return pd.DataFrame.from_dict(entities)
#         return [self.factory(entity) for entity in entities]

#     def update(self, entity: Entity) -> None:
#         """Updates the entity

#         Args:
#             entity (Entity): The entity to update. The entity is overwritten.

#         """
#         command = self._sql.update(entity)
#         return self._database.execute(command.statement, command.parameters)

#     def delete(self, id: int) -> None:
#         """Removes an entity from the database based upon id

#         Args:
#             id (int): The unique identifier for the entity

#         """
#         command = self._sql.delete(id)
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
#                                        FILE DAO                                                  #
# ================================================================================================ #
class FileDAO(DAO):
    """Provides access to localfile table ."""

    def __init__(self, database: Database) -> None:
        self._sql = FileSQL()
        super(FileDAO, self).__init__(database)

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the database

        Args:
            entity (Entity): The entity to add to the database

        Returns
            Entity with the id updated with id from the database.
        """
        command = self._sql.insert(entity)
        entity.id = self._database.insert(command.statement, command.parameters)
        return entity

    def find(self, id: int) -> dict:
        """Finds an entity based on the id and returns it as an entity dataclass.

        Args:
            id (int): The id that uniquely identifies an entity

        Returns a dictionary containing data for the specified row.
        """

        command = self._sql.select(id)
        return self._database.select_one(command.statement, command.parameters)

    def find_by_dataset_id(self, dataset_id: int) -> Entity:
        """Finds an entity based on dataset and returns it as an entity dataclass.

        Args:
            dataset (str): The dataset name

        Returns list of dictionaries containing the
            Entity
        """

        command = self._sql.select_by_dataset_id(dataset_id)
        return self._database.select(command.statement, command.parameters)

    def findall(self, todf: bool = False) -> list:
        """Returns all entities from the designated entity table."""
        command = self._sql.select_all()
        return self._database.select_all(command.statement)

    def update(self, entity: Entity) -> None:
        """Updates the entity

        Args:
            entity (Entity): The entity to update. The entity is overwritten.

        """
        command = self._sql.update(entity)
        return self._database.execute(command.statement, command.parameters)

    def delete(self, id: int) -> None:
        """Removes an entity from the database based upon id

        Args:
            id (int): The unique identifier for the entity

        """
        command = self._sql.delete(id)
        return self._database.execute(command.statement, command.parameters)


# ================================================================================================ #
#                                        DATASET DAO                                               #
# ================================================================================================ #
class DatasetDAO(DAO):
    """Provides access to dataset table ."""

    def __init__(self, connection: Connection) -> None:
        self._sql = DatasetSQL()
        super(DatasetDAO, self).__init__(connection)

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the database

        Args:
            entity (Entity): The entity to add to the database

        Returns
            Entity with the id updated with id from the database.
        """
        command = self._sql.insert(entity)
        entity.id = self._database.insert(command.statement, command.parameters)
        return entity

    def find(self, id: int) -> dict:
        """Finds an entity based on the id and returns it as an entity dataclass.

        Args:
            id (int): The id that uniquely identifies an entity

        Returns a dictionary containing the results of the query.
        """

        command = self._sql.select(id)
        return self._database.select_one(command.statement, command.parameters)

    def findall(self, todf: bool = False) -> list:
        """Returns all entities from the designated entity table."""
        command = self._sql.select_all()
        return self._database.select_all(command.statement)

    def update(self, entity: Entity) -> None:
        """Updates the entity

        Args:
            entity (Entity): The entity to update. The entity is overwritten.

        """
        command = self._sql.update(entity)
        return self._database.execute(command.statement, command.parameters)

    def delete(self, id: int) -> None:
        """Removes an entity from the database based upon id

        Args:
            id (int): The unique identifier for the entity

        """
        command = self._sql.delete(id)
        return self._database.execute(command.statement, command.parameters)
