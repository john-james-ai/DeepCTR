#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /database.py                                                                          #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Wednesday, May 4th 2022, 12:40:42 am                                                  #
# Modified :                                                                                       #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from abc import ABC
import pandas as pd
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #


@dataclass
class Query(ABC):
    name: str
    statement: str
    database: str
    table: str
    parameters: tuple = None
    is_parameterized: bool = False


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatabaseExists(Query):
    name: str
    statement: str
    database: str
    table: str
    parameters: tuple = None
    is_parameterized: bool = False

    def exists(self, df: pd.DataFrame) -> bool:
        return df.shape[0] > 0


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TableExists(Query):
    name: str
    statement: str
    database: str
    table: str
    parameters: tuple = None
    is_parameterized: bool = False

    def exists(self, df: pd.DataFrame) -> bool:
        return df.iat[0, 0] == 1


# ------------------------------------------------------------------------------------------------ #


@dataclass
class AlibabaDatabaseExists(DatabaseExists):
    name: str = "alibaba_database_existence"
    description: str = "Alibaba Database Existence"
    database: str = "alibaba"
    table: str = ""
    statement: str = "SHOW DATABASES LIKE 'alibaba';"
    is_parameterized: bool = False


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UserTableExists(TableExists):
    name: str = "alibaba_user_table_existence"
    description: str = "Alibaba User Table Existence"
    database: str = "alibaba"
    table: str = "user"
    statement: str = "SELECT EXISTS \
        (SELECT TABLE_NAME \
        FROM information_schema.TABLES \
            WHERE TABLE_SCHEMA LIKE 'alibaba' AND TABLE_NAME = 'user');"
    is_parameterized: bool = False


@dataclass
class AdTableExists(TableExists):
    name: str = "alibaba_ad_table_existence"
    description: str = "Alibaba Ad Table Existence"
    database: str = "alibaba"
    table: str = "ad"
    statement: str = "SELECT EXISTS \
        (SELECT TABLE_NAME \
    FROM information_schema.TABLES \
        WHERE TABLE_SCHEMA LIKE 'alibaba' AND TABLE_NAME = 'ad');"
    is_parameterized: bool = False


@dataclass
class ImpressionTableExists(TableExists):
    name: str = "alibaba_impression_table_existence"
    description: str = "Alibaba Impression Table Existence"
    database: str = "alibaba"
    table: str = "impression"
    statement: str = "SELECT EXISTS \
        (SELECT TABLE_NAME \
            FROM information_schema.TABLES \
                WHERE TABLE_SCHEMA LIKE 'alibaba' AND TABLE_NAME = 'impression');"
    is_parameterized: bool = False


@dataclass
class BehaviorTableExists(TableExists):
    name: str = "alibaba_behavior_table_existence"
    description: str = "Alibaba Behavior Table Existence"
    database: str = "alibaba"
    table: str = "behavior"
    statement: str = "SELECT EXISTS \
        (SELECT TABLE_NAME \
            FROM information_schema.TABLES \
                WHERE TABLE_SCHEMA LIKE 'alibaba' AND TABLE_NAME = 'behavior');"
    is_parameterized: bool = False
