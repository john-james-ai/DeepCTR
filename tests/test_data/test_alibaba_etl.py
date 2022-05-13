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
# Modified : Wednesday, May 4th 2022, 12:45:42 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import inspect
import pytest
import logging
import shutil
from deepctr.orchestration.dag import DagRunner


# from deepctr.database.access import DAO

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.alibaba
class TestAlibabaDatabaseDAG:
    def cleanup(self):

        filepath = "data/alibaba/prod/staged"
        shutil.rmtree(filepath, ignore_errors=True)

    def test_alibaba_extract(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        config_filepath = "data/alibaba/prod/alibaba.yml"
        mode = "prod"

        dr = DagRunner()
        dr.run(config_filepath=config_filepath, start=0, stop=2, mode=mode)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_alibaba_samples(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        self.cleanup()

        config_filepath = "data/alibaba/prod/alibaba.yml"
        mode = "prod"

        dr = DagRunner()
        dr.run(config_filepath=config_filepath, start=3, stop=6, mode=mode)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
