#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /dataset.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 15th 2022 06:40:03 am                                                    #
# Modified   : Sunday May 15th 2022 06:47:18 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Classes"""
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from pyspark.sql import DataFrame
from deepctr.persistence.repository import DataRepository

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class DataSet(ABC):
    """Abstract base class for datasets, specific versions of data assets."""

    def __init__(self, name: str, mode: str, stage: str) -> None:
        self._name = name
        self._mode = mode
        self._stage = stage
        self._created = datetime.now()
        self._updated = None
        self._repo = DataRepository()

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def stage(self) -> str:
        return self._stage

    @abstractmethod
    def read(self, table: str) -> DataFrame:
        pass

    @abstractmethod
    def write(self, table: str, data: DataFrame, force: bool = False) -> None:
        pass

    @abstractmethod
    def get_schema(self, table) -> Any:
        pass

    @abstractmethod
    def set_schema(self, table, schema: Any) -> None:
        pass

    @abstractmethod
    def profile(self, table: str) ->




# ------------------------------------------------------------------------------------------------ #


class AlibabaDataSet(DataSet):
    """Alibaba Dataset """

    __asset = "alibaba"

    def __init__(self, name: str, mode: str, stage: str) -> None:
        super(AlibabaDataSet, self).__init__(name=name, mode=mode, stage=stage)

    def read(self, table: str) -> DataFrame:
        return self._repo.get(
            name=table, stage=self._stage, asset=AlibabaDataSet.__asset, mode=self._mode,
        )

    def write(self, table: str, data: DataFrame, force: bool = False) -> None:
        self._repo.add(
            name=table,
            data=data,
            asset=AlibabaDataSet.__asset,
            stage=self._stage,
            mode=self._mode,
            force=self._force,
        )

# ------------------------------------------------------------------------------------------------ #
#                                    DATASET BUILDER                                               #
# ------------------------------------------------------------------------------------------------ #
class SubsampleDataSetFactory(ABC):
    """Abstract base class defining the interface for DataSet construction."""
    @abstractmethod
    def get_dataset(self, )