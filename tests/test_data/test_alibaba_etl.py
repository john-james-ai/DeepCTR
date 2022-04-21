#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /test_alibaba_etl.py                                                                  #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, April 8th 2022, 3:48:38 pm                                                    #
# Modified : Thursday, April 21st 2022, 11:27:39 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import inspect
import pytest
import logging
from deepctr.data.dag import DagRunner

from deepctr.database.sequel import AlibabaDatabaseExists, AdTableExists, UserTableExists
from deepctr.database.sequel import BehaviorTableExists, ImpressionTableExists
from deepctr.database.access import DAO

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.alibaba_db
class TestAlibabaDatabaseDAG:
    def test_database(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        config_filepath = "config/alibaba.yml"

        dr = DagRunner()
        dr.run(config_filepath=config_filepath)

        queries = [
            AlibabaDatabaseExists(),
            UserTableExists(),
            AdTableExists(),
            BehaviorTableExists(),
            ImpressionTableExists(),
        ]

        dao = DAO("alibaba")
        for query in queries:
            assert dao.exists(query), "{}: {} does not exist.".format(query.database, query.table)

        dao.close()

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
