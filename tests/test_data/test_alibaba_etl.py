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
# Modified : Wednesday, April 20th 2022, 3:00:51 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import inspect
import pytest
from dotenv import load_dotenv
import logging
from deepctr.data.dag import DagBuilder
from deepctr.utils.io import YamlIO

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
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
        transformed = "data/alibaba/production/transformed"

        load_dotenv()

        yaml = YamlIO()
        config = yaml.read(config_filepath)

        builder = DagBuilder()
        dag = builder.build(config=config[mode])
        dag.run()

        assert len(os.listdir(destination)) == 4, "Files did not make it to destination"
        assert len(os.listdir(transformed)) == 4, "Files did not make it to destination"

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
