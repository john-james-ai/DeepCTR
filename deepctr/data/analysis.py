#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /analysis.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 15th 2022 09:23:59 am                                                    #
# Modified   : Sunday May 15th 2022 09:23:59 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Analysis"""
import logging
from deepctr.data.dataset import AlibabaDataSet

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
class Profile(ABC):
    """Base Class defining interface for classes that profile data tables."""
    @property
    self data(self) -> DataFrame

