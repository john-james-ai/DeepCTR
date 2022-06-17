#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_dag_metadata.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 27th 2022 02:10:21 pm                                                    #
# Modified   : Saturday May 28th 2022 06:47:46 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import inspect
import pytest
import logging
import logging.config

from deepctr.utils.config import YamlIO
from deepctr.dal.dao import DatasetDAO
from deepctr.dal.context import DataContext
from deepctr.data.database import Database
from tests.dag.test_metadata_operators import DatasetHandler, S3FileHandler, LocalFileHandler
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.meta
class TestDAG:
    def test_dag(self, caplog, connection_dal) -> None:
        logger.info("\n\n\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        template_file = "tests/config/testmetadata_dag.yml"

        connection = connection_dal
        context = DataContext(connection)
        context.datasets = DatasetDAO(connection)
        template = YamlIO().read(filepath=template_file)
        for k, v in template.items():
            if v["asset_type"] == "dataset":
                handler = DatasetHandler(seq=1, name="dataset", desc="dataset", params=template)
            elif v.asset_type == "file":
                handler = LocalFileHandler(seq=2, name="local", desc="local", params=template)

            handler.execute(data=template, context=context)

