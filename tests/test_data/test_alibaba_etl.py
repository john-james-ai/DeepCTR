#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepNeuralCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /test_alibaba_etl.py                                                                  #
# Language : Python 3.10.4                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepNeuralCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, April 8th 2022, 3:48:38 pm                                                    #
# Modified : Saturday, April 9th 2022, 2:18:20 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import inspect
import pytest
import logging
from deepctr.data.base import DagBuilder
from deepctr.utils.io import YamlIO

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.alibaba_etl
class TestAlibabaETL:
    def test_extract(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        mode = "production"
        config_filepath = "config/alibaba.yml"
        destination = "data/alibaba/production/raw"
        yaml = YamlIO()
        config = yaml.read(config_filepath)
        builder = DagBuilder(config=config[mode])
        dag = builder.build()
        dag.run()

        assert len(os.listdir(destination)) == 4, "Files did not make it to destination"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
