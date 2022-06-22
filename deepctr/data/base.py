#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /base.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday June 22nd 2022 11:50:05 am                                                #
# Modified   : Wednesday June 22nd 2022 11:52:08 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from abc import ABC, abstractmethod
import pandas as pd
from pyspark.sql import DataFrame
from typing import Union

# ------------------------------------------------------------------------------------------------ #
#                                              IO                                                  #
# ------------------------------------------------------------------------------------------------ #


class IO(ABC):
    """Base class for IO classes"""

    @abstractmethod
    def read(self, filepath: str, **kwargs) -> Union[pd.DataFrame, DataFrame]:
        pass

    @abstractmethod
    def write(self, data: Union[pd.DataFrame, DataFrame], filepath: str, **kwargs) -> None:
        pass
