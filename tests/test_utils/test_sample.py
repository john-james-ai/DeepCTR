#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction     #
# Version  : 0.1.0                                                                                 #
# File     : /test_io.py                                                                           #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                        #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, April 10th 2022, 5:26:12 pm                                                   #
# Modified : Sunday, April 24th 2022, 5:32:21 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import inspect
import pytest
import logging
from deepctr.utils.sample import sample_from_file

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.sample
class TestSample:
    def test_sample_from_file(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = "data/alibaba/raw/raw_sample.csv"
        size = 1000

        df = sample_from_file(source=source, size=size, header=True)
        print(df.head())

        assert df.shape[0] == size, logger.error("Sample size incorrect.")
