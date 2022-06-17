#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_database.py                                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 22nd 2022 01:40:01 am                                                    #
# Modified   : Friday May 27th 2022 06:32:14 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pytest
import logging
import numpy as np
import logging.config

from deepctr.utils.log_config import LOG_CONFIG
from deepctr.data.database import DBConfig, Database

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.db
class TestDatabase:
    def test_config(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        assert DBConfig.user == "john", logger.error("Database configuration error")
        assert DBConfig.database == "deepctr", logger.error("Database configuration error")
        assert DBConfig.host == "localhost", logger.error("Database configuration error")
        assert DBConfig.port == 3306 or DBConfig.port == "3306", logger.error(
            "Database configuration error"
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_insert(self, caplog, connection_data) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        connection = connection_data

        assert connection.open, logger.error("Connection error")
        database = Database(connection)
        add_data = """INSERT INTO `testtable` (number, letters) VALUES (%s, %s);"""
        for i in range(100):
            number = np.random.randint(0, 1000)
            letters = "some_text_" + str(number)
            parameters = (number, letters)
            database.execute(add_data, parameters)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_database_select_all(self, caplog, connection_data) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        connection = connection_data
        database = Database(connection)

        statement = """SELECT * FROM `testtable`;"""
        result = database.select_all(statement)
        assert isinstance(result, list), logger.error("Select_all error")
        print("# ", 76 * "=", " #")
        print(result)
        print("# ", 76 * "=", " #")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_database_select_one(self, caplog, connection_data) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        connection = connection_data
        database = Database(connection)

        statement = """SELECT * FROM `testtable` WHERE `id`= %s;"""
        result = database.select_one(statement, parameters=(3))
        assert isinstance(result, dict), logger.error("Select_one error")
        print("# ", 76 * "=", " #")
        print(result)
        print("# ", 76 * "=", " #")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_database_execute(self, caplog, connection_data) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        connection = connection_data
        database = Database(connection)

        statement = """INSERT INTO `testtable` (number, letters) VALUES (%s, %s);"""
        parameters = (999, "this is the test row")
        database.execute(statement, parameters)

        statement = """SELECT * FROM `testtable` WHERE id=%s;"""

        result = database.select_one(statement, (101))
        assert isinstance(result, dict), logger.error("Execute error")
        print("# ", 76 * "=", " #")
        print(result)
        print("# ", 76 * "=", " #")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_database_exists(self, caplog, connection_data) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        connection = connection_data
        database = Database(connection)

        statement = """SELECT EXISTS(SELECT * FROM `testtable` WHERE id=%s);"""
        parameters = 5
        result = database.exists(statement, parameters)
        # assert isinstance(result, bool), logger.error("Execute error")
        assert result is True, logger.error("Execute error")

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
