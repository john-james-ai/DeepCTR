#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 1.0.0                                                                                 #
# File     : /test_elt.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCVR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, April 3rd 2022, 5:20:56 pm                                                    #
# Modified : Tuesday, April 5th 2022, 8:38:06 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import sys
import pytest
import logging
import inspect

# import shutil
import warnings

from deepcvr.data.dag import DagBuilder
from deepcvr.utils.config import config_dag
from pprint import pprint

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.elt
class TestELT:
    def test_extract(self) -> None:
        #caplog.set_level(logging.DEBUG)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain the dag configuration
        configfile = "config/extract.yaml"
        config = config_dag(configfile)

        print(config['tasks'])
        pprint(sys.path)

        # Construct the builder for the extract dag
        builder = DagBuilder(config=config)
        dag = builder.build()

        dag.run()
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

