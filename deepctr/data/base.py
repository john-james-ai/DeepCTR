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
# Modified   : Sunday June 26th 2022 01:33:01 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from dataclasses import dataclass
from datetime import datetime

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
