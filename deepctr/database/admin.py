#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /factory.py                                                                           #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Wednesday, April 20th 2022, 7:43:04 pm                                                #
# Modified : Wednesday, April 20th 2022, 11:40:19 pm                                               #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Database Administration"""
import os
from pymysql import connect
from pymysql.cursors import DictCursor
from datetime import datetime
from dotenv import load_dotenv
import logging
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@dataclass
class SQLStatement:
    """Represents a single sql statement, its parameters and some metadata"""

    name: str
    description: str
    query: str
    parameters: tuple = ()
    is_parameterized: bool = False


# ------------------------------------------------------------------------------------------------ #
class SQLTransaction:
    """Represents a sequence of SQLStatement Objects to be executed as part of a transaction"""

    def __init__(self) -> None:
        self._statements = []

    def add_statement(self, statement: SQLStatement) -> None:
        """Adds the statement to the transaction list.

        Args:
            statement (SQLStatement): Statement to add to transaction
        """
        self._statements.append(statement)

    @property
    def statements(self) -> list:
        return self._statements


# ------------------------------------------------------------------------------------------------ #
class SQLEngine:
    """Executes SQL stored in .sql files."""

    def execute(self, transaction: SQLTransaction) -> None:

        connection = self._get_connection()

        with connection:
            with connection.cursor() as cursor:
                for idx, statement in enumerate(transaction.statements):
                    dt = datetime.now()
                    date = dt.strftime("%d/%m/%y")
                    time = dt.stftime("%I:%M%p")
                    logger.info(
                        "Statement {}: {}\t{}\t{} at {}".format(
                            str(idx), statement.name, statement.description, date, time
                        )
                    )
                    if statement.is_parameterized:
                        cursor.execute(statement.query, parameters)
                    else:
                        cursor.execute(statement.query)

    def _get_connection(self) -> connect:
        """Return a MySQL database connection."""

        load_dotenv()

        USER = os.getenv("USER")
        PASSWORD = os.getenv("PASSWORD")
        HOST = os.getenv("HOST")
        DATABASE = os.getenv("DATABASE")

        connection = connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            charset="utf8",
            cursorclass=DictCursor,
        )
        return connection
