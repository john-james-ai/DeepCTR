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
# Modified   : Friday June 24th 2022 08:50:35 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from pyspark.sql import DataFrame
from typing import Union

# ------------------------------------------------------------------------------------------------ #
#                                          METADATA                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Metadata:
    """Metadata for Files and Datasets Dataset objects only use the dates and size."""

    rows: int = 0
    cols: int = 0
    size: int = 0
    created: datetime = None
    modified: datetime = None
    accessed: datetime = None


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

    @abstractmethod
    def metadata(self, filepath: str, **kwargs) -> dict:
        pass
