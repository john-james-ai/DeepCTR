#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /spark.py                                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday June 18th 2022 05:54:32 am                                                 #
# Modified   : Saturday June 18th 2022 07:45:02 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from pyspark.sql import DataFrame

# ------------------------------------------------------------------------------------------------ #
def get_size_spark(data: DataFrame, fraction: float = 0.001, seed=None) -> int:
    """Estimates size of a Spark DataFrame

    This function takes a sample from the Spark Dataframe, converts it to a pandas DataFrame,
    then uses pandas memory_usage method to obtain the size of the sample in memory. We
    multiply that value by the inverse of our  sampling fraction to get an estimate for the
    size of the entire Spark DataFrame.

    Args:
        data: (DataFrame) The Spark DataFrame
        fraction (float): The fraction of the Spark DataFrame to sample
    Returns:
        (int) Size of Spark DataFrame in bytes.
    """
    df_sample = data.sample(withReplacement=False, fraction=fraction, seed=seed).toPandas()
    sample_size = df_sample.memory_usage(deep=True).sum()
    return sample_size / fraction
