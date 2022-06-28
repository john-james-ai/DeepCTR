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
# Created    : Thursday June 23rd 2022 09:28:39 pm                                                 #
# Modified   : Tuesday June 28th 2022 11:27:54 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Defines classes used in several modules in the dal package."""
from abc import ABC, abstractmethod
import inspect
import logging
import logging.config
from typing import Any
from datetime import datetime

from deepctr.dal import STAGES, FORMATS
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                     ENTITY                                                       #
# ------------------------------------------------------------------------------------------------ #
class Entity(ABC):
    """All entities, File, Dataset, Model, Task, Dags descend from this class."""

    def __init__(self, name, desc, from_database: bool = False, **kwargs) -> None:
        self._id = 0
        self._name = name
        self._desc = desc
        self._from_database = from_database
        # If from_database is true, then changes do not effect the change, modified, and
        # accessed dates. Rather these dates are assigned from values in the database. If
        # from_database is False (Default), access and changes will result in updated
        # modified and accessed dates.
        self._created = datetime.now()
        self._modified = datetime.now()
        self._accessed = datetime.now()

    @property
    def id(self) -> int:
        self._accessed_date()
        return self._id

    @id.setter
    def id(self, id) -> None:
        self._id = id
        self._update_dates()

    @property
    def name(self) -> str:
        self._accessed_date()
        return self._name

    @name.setter
    def name(self, name) -> None:
        self._name = name
        self._update_dates()

    @property
    def desc(self) -> str:
        self._accessed_date()
        return self._desc

    @desc.setter
    def desc(self, desc) -> None:
        self._desc = desc
        self._update_dates()

    @property
    def created(self) -> datetime:
        return self._created

    @created.setter
    def created(self, created) -> None:
        self._created = created

    @property
    def modified(self) -> datetime:
        return self._modified

    @modified.setter
    def modified(self, modified) -> None:
        self._modified = modified

    @property
    def accessed(self) -> datetime:
        return self._accessed

    @accessed.setter
    def accessed(self, accessed) -> None:
        self._accessed = accessed

    def _accessed_date(self) -> None:
        if not self._from_database:
            self._accessed = datetime.now()

    def _update_dates(self) -> None:
        if not self._from_database:
            self._modified = datetime.now()
            self._accessed = self._modified


# ------------------------------------------------------------------------------------------------ #
#                                          MAPPER                                                  #
# ------------------------------------------------------------------------------------------------ #
class EntityMapper(ABC):
    """Abstract base class for command classes, one for each entity."""

    @abstractmethod
    def insert(self, entity: Entity):
        pass

    @abstractmethod
    def select(self, id: int):
        pass

    @abstractmethod
    def select_all(self):
        pass

    @abstractmethod
    def update(self, entity: Entity):
        pass

    @abstractmethod
    def delete(self, id: int):
        pass

    @abstractmethod
    def factory(self, record: dict) -> Entity:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                       VALIDATOR                                                  #
# ------------------------------------------------------------------------------------------------ #
class Validator:
    """Provides validation for Dataset and File objects"""

    def format(self, value: str) -> bool:
        value = value.replace(".", "")
        if value not in FORMATS:
            self._fail(value, FORMATS)
        else:
            return value

    def stage(self, value: int) -> bool:
        if value not in STAGES.keys():
            self._fail(value, STAGES)
        else:
            return value

    def _fail(self, value: Any, valid_values: list):
        variable = inspect.stack()[1][3]
        caller_method = inspect.stack()[0][3]
        caller_classname = caller_method.__class__.__name__
        msg = "Error in {}: {}. Invalid {}: {}. Valid values are: {}".format(
            caller_classname, caller_method, variable, value, valid_values
        )
        logger.error(msg)
        raise ValueError(msg)
