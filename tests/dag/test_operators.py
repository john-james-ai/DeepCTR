#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_operators.py                                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 27th 2022 07:11:18 am                                                    #
# Modified   : Friday May 27th 2022 11:09:57 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Any
import logging
import logging.config
import pandas as pd

from deepctr.utils.decorators import operator
from deepctr.dag.base import Operator
from deepctr.dal.context import Context
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
class MixIngredients(Operator):
    """Test Operator"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(MixIngredients, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        logger.info("I'm mixing the ingredients.")


# ------------------------------------------------------------------------------------------------ #
class PreheatOven(Operator):
    """Test Operator"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(PreheatOven, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        assert isinstance(context.dag.id, int), logger.error(
            "Test for context failed in {}".format(self.__class__.__name__)
        )
        logger.info("I'm preheating the oven.")


# ------------------------------------------------------------------------------------------------ #
class OilPan(Operator):
    """Test Operator"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(OilPan, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        assert isinstance(context.dag.id, int), logger.error(
            "Test for context failed in {}".format(self.__class__.__name__)
        )
        logger.info("I'm oiling the pan.")


# ------------------------------------------------------------------------------------------------ #
class Bake(Operator):
    """Test Operator"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(Bake, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        assert isinstance(context.dag.id, int), logger.error(
            "Test for context failed in {}".format(self.__class__.__name__)
        )
        logger.info("I'm baking.")


# ------------------------------------------------------------------------------------------------ #
class Cool(Operator):
    """Test Operator"""

    def __init__(self, seq: int, name: str, desc: str, params: dict) -> None:
        super(Cool, self).__init__(seq=seq, name=name, desc=desc, params=params)

    @operator
    def execute(self, data: Any = None, context: Context = None) -> pd.DataFrame:
        assert isinstance(context.dag.id, int), logger.error(
            "Test for context failed in {}".format(self.__class__.__name__)
        )
        logger.info("It's cooling now.")
