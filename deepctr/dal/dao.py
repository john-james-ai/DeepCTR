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
# Modified   : Saturday June 25th 2022 03:03:47 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging
import pandas as pd
from pymysql.connections import Connection

from deepctr import Entity
from deepctr.dal.base import File, Dataset
from deepctr.data.database import Database
from deepctr.dal.sequel import FileSQL, DatasetSQL  # , DagSQL, TaskSQL,  S3FileSQL
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
    @abstractmethod
    def factory(self, entity) -> None:
        self._connection.begin()

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

    def __init__(self, connection: Connection) -> None:
        self._sql = FileSQL()
        super(FileDAO, self).__init__(connection)

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

    def find(self, id: int) -> Entity:
        """Finds an entity based on the id and returns it as an entity dataclass.

        Args:
            id (int): The id that uniquely identifies an entity

        Returns
            Entity
        """

        command = self._sql.select(id)
        entity = self._database.select_one(command.statement, command.parameters)
        return self.factory(entity)

    def find_by_dataset(self, dataset: int) -> Entity:
        """Finds an entity based on dataset and returns it as an entity dataclass.

        Args:
            dataset (str): The dataset name

        Returns
            Entity
        """

        command = self._sql.select_by_dataset_name(dataset)
        files = self._database.select(command.statement, command.parameters)
        file_objects = [self.factory(file) for file in files]
        return file_objects

    def findall(self, todf: bool = False) -> list:
        """Returns all entities from the designated entity table."""
        command = self._sql.select_all()
        entities = self._database.select_all(command.statement)
        if todf:
            return pd.DataFrame.from_dict(entities)
        return [self.factory(entity) for entity in entities]

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
            rows=entity["rows"],
            cols=entity["cols"],
            size=entity["size"],
            created=entity["created"],
            modified=entity["modified"],
            accessed=entity["accessed"],
        )
        if file.compressed == 1:
            file.compressed = True
        else:
            file.compressed = False
        return file


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
        for file in entity.files:
            file = self._file_dao.add(file)
            entity.files[file.name] = file  # We re-add the file with id to the dataset entity.
        command = self._sql.insert(entity)
        entity.id = self._database.insert(command.statement, command.parameters)
        return entity

    def find(self, id: int) -> Entity:
        """Finds an entity based on the id and returns it as an entity dataclass.

        Args:
            id (int): The id that uniquely identifies an entity

        Returns
            Entity
        """

        command = self._sql.select(id)
        entity = self._database.select_one(command.statement, command.parameters)
        entity = self.factory(entity)
        return entity

    def findall(self, todf: bool = False) -> list:
        """Returns all entities from the designated entity table."""
        command = self._sql.select_all()
        entities = self._database.select_all(command.statement)
        if todf:
            return pd.DataFrame.from_dict(entities)
        return [self.factory(entity) for entity in entities]

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

    def factory(self, entity: Entity) -> Entity:
        """Factory method to convert database result to an entity object.

        Args:
            entity (Entity): An entity object of the derived type
        """
        compute_size = False
        dataset = Dataset(
            id=entity["id"],
            name=entity["name"],
            source=entity["source"],
            storage_type=entity["storage_type"],
            stage_id=entity["stage_id"],
            stage_name=entity["stage_name"],
            home=entity["home"],
            bucket=entity["bucket"],
            folder=entity["folder"],
            size=entity["size"],
            created=entity["created"],
            modified=entity["modified"],
            accessed=entity["accessed"],
        )
        files = self._file_dao.find_by_dataset_name(dataset.name)
        compute_size = dataset.size == 0
        for file in files:
            dataset.files[file.name] = file
            if compute_size:
                dataset.size = dataset.size + file.size
        return dataset
