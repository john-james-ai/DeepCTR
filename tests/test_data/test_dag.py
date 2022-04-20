#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /test_dag.py                                                                          #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, April 19th 2022, 8:41:38 pm                                                  #
# Modified : Tuesday, April 19th 2022, 9:14:49 pm                                                  #
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
from deepctr.data.dag import Context
from deepctr.utils.io import YamlIO

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.context
class TestContext:
    def test_context(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        load_dotenv()
        credentials_filepath = os.getenv("credentials_filepath")
        io = YamlIO()
        credentials = io.read(filepath=credentials_filepath)

        test_resources = {"database": ["alibaba", "mysql"], "cloud": ["amazon"]}

        context = Context()
        for resource_type, resources in test_resources.items():
            for resource in resources:
                logger.info("Adding context for {} {}".format(resource_type, resource))
                context.add_context(resource_type=resource_type, resource=resource)

        context.print_context()

        for resource_type, resources in test_resources.items():
            for resource in resources:
                logger.info("Getting context for {} {}".format(resource_type, resource))
                credential = context.get_context(resource_type=resource_type, resource=resource)
                assert credential == credentials[resource_type].get(
                    resource
                ), "Context failed for {} {}".format(resource_type, resource)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
