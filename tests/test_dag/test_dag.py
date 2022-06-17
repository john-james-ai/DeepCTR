#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_dag.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 27th 2022 02:10:21 pm                                                    #
# Modified   : Saturday May 28th 2022 06:06:22 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import inspect
import pytest
import logging
import logging.config

from deepctr.utils.config import YamlIO
from deepctr.dag.orchestrator import DataDAGBuilder
from deepctr.dal.context import DataContext
from deepctr.data.database import Database
from deepctr.dal.sequel import TaskSelectAll
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.skip()
class TestDAG:
    def test_dag(self, caplog, connection_dal) -> None:
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        template_file = "tests/config/testdag.yml"

        connection = connection_dal
        context = DataContext(connection)
        template = YamlIO().read(filepath=template_file)
        builder = DataDAGBuilder()
        dag = (
            builder.datasource(datasource="cakes")
            .at("tests/data")
            .with_template(template)
            .and_context(context)
            .build()
            .dag
        )

        dag.run()

        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


def test_check_dag(self, caplog, connection_dal) -> None:
    logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    connection = connection_dal
    database = Database(connection)
    sequel = TaskSelectAll()
    print(database.select_all(sequel.statement))

    logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
