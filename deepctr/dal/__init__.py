#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /__init__.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 10th 2022 03:30:15 pm                                                   #
# Modified   : Tuesday June 28th 2022 02:25:46 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from deepctr.data.local import SparkCSV, SparkParquet

# ------------------------------------------------------------------------------------------------ #
STAGES = {
    0: "external",
    1: "raw",
    2: "loaded",
    3: "interim",
    4: "clean",
    5: "features",
    6: "processed",
}
FORMATS = ["csv", "parquet"]
SOURCES = ["alibaba", "avazu", "criteo"]
FILE_SYSTEMS = ["local", "s3"]
IO = {"csv": SparkCSV(), "parquet": SparkParquet()}
