#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /source.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday June 28th 2022 04:54:40 am                                                  #
# Modified   : Tuesday June 28th 2022 09:48:36 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

from deepctr.dal.base import Entity, EntityMapper

# ------------------------------------------------------------------------------------------------ #
class Source(Entity):
    """Defines a data source"""

    def __init__(self, name: str, desc: str, url: str) -> None:
        super(Source, self).__init__(name=name, desc=desc)
        self._url = url

    @property
    def url(self) -> str:
        return self._url


# ------------------------------------------------------------------------------------------------ #
#                                         SEQUEL                                                   #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class SourceInsert:
    entity: Entity
    statement: str = None
    parameters: tuple = None

    def __post_init__(self) -> None:
        self.statement = """
            INSERT INTO `source`
            (`name`, `desc`, `url`, `created`, `modified`, `accessed`)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.url,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceSelect:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `source` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceSelectAll:
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """SELECT * FROM `source`;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceDelete:
    parameters: tuple
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """DELETE FROM `source` WHERE `id`= %s;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceUpdate:
    entity: Entity
    parameters: tuple = None
    statement: str = None

    def __post_init__(self) -> None:
        self.statement = """UPDATE `source`
                            SET `name` = %s,
                                `desc` = %s,
                                `url` = %s,
                                `created` = %s,
                                `modified` = %s,
                                `accessed` = %s
                            WHERE `id`= %s;"""

        self.parameters = (
            self.entity.name,
            self.entity.desc,
            self.entity.url,
            self.entity.created,
            self.entity.modified,
            self.entity.accessed,
            self.entity.id,
        )


# ------------------------------------------------------------------------------------------------ #
#                                       SOURCE MAPPER                                              #
# ------------------------------------------------------------------------------------------------ #
class SourceMapper(EntityMapper):
    """Commands for the source table."""

    def insert(self, entity: Entity) -> SourceInsert:
        return SourceInsert(entity)

    def select(self, id: int) -> SourceSelect:
        return SourceSelect(parameters=(id,))

    def select_all(self) -> SourceSelectAll:
        return SourceSelectAll()

    def update(self, entity: Entity) -> SourceUpdate:
        return SourceUpdate(entity)

    def delete(self, id: int) -> SourceDelete:
        return SourceDelete(parameters=(id,))

    def factory(self, record: dict) -> Entity:
        source = Source(name=record["name"], desc=record["desc"], url=record["url"])
        source.id = record["id"]
        source.created = record["created"]
        source.modified = record["modified"]
        source.accessed = record["accessed"]
        return source
