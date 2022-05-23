#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /database.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 19th 2022 06:39:17 pm                                                  #
# Modified   : Monday May 23rd 2022 07:45:02 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
import pymysql

from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class DBConfig:
    """Encapsulates MySQL database credentials"""

    load_dotenv()
    host = os.getenv("HOST")
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    port = os.getenv("PORT")
    database = os.getenv("DATABASE")


# ------------------------------------------------------------------------------------------------ #


class Connection:
    """MySQL database connections context manager."""

    def __init__(self) -> None:
        # self._user = config.user
        # self._password = config.password
        # self._port = config.port
        # self._host = config.host
        # self._database = config.database
        self._connection = None

        # print(self._database)
        # print(self._user)

    def __enter__(self):
        load_dotenv()
        string = os.getenv("MYSQL_CONNECTION_STRING")
        try:
            if self._connection is None:
                self._connection = pymysql.connections.Connection(string)

        except pymysql.err.MySQLError as e:
            logger.error(e)
            raise
        now = datetime.now().strftime(("%m/%d/%Y, %-H:%M:%S %p"))
        logger.info("Connection to {} successful at {}".format(self._database, now))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_tb is None:
            self._connection.commit()
        else:
            self._connection.rollback()
        self._connection.close()


class Database:
    """Class responsible for direct access to database."""

    def __init__(self, connection: pymysql.connector.Connection) -> None:
        self._connection = connection

    def _execute(self, statement, parameters=()):
        try:
            cursor = self._connection.cursor()
            cursor.execute(statement, parameters)
            return cursor
        except pymysql.err.MySQLError as e:
            print("Execute error %d: %s" % (e.args[0], e.args[1]))

    def select(self, statement: str, parameters: tuple):
        return self._execute(statement, parameters)

    def execute(self, statement: str, parameters: tuple):
        return self._execute(statement, parameters).rowcount

    def select_all(self, statement: str, parameters: tuple):
        cursor = self._select(statement, parameters)
        return cursor.fetchall()

    def select_one(self, statement: str, parameters: tuple):
        cursor = self._select(statement, parameters)
        return cursor.fetchone()

    def exists(self, statement: str, parameters: tuple):
        return self._execute(statement, parameters)
