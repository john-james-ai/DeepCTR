#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /io.py                                                                                #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 6:41:17 pm                                              #
# Modified : Sunday, April 10th 2022, 3:45:11 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Spark Utilities"""
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType  # noqa: F401
from pyspark.sql.types import FloatType, DoubleType  # noqa: F401
from pyspark.sql.types import BooleanType, TimestampType  # noqa: F401
from pyspark.sql.types import IntegerType, LongType  # noqa: F401

# ------------------------------------------------------------------------------------------------ #


def to_spark(df: pd.DataFrame) -> pyspark.sql.DataFrame:
    """Convert pandas DataFrame to Spark DataFrame

    Spark provides a createDataFrame(pandas_dataframe) method to convert pandas to
    Spark DataFrame, Spark by default infers the schema based on the pandas data
    types to PySpark data types.

    Args:
        df (pd.DataFrame): pandas DataFrame
    Returns:
        pyspark.sql.DataFrame
    """
    spark = SparkSession.builder.master("local[6]").appName("DeepCTR: to_spark").getOrCreate()
    # Enable Apache Arrow to convert Pandas to PySpark DataFrame
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    # Create PySpark DataFrame from Pandas
    sparkDF = spark.createDataFrame(df)
    return sparkDF
