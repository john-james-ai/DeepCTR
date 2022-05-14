#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deepctr: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /conftest.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, March 14th 2022, 7:17:14 pm                                                   #
# Modified : Thursday, April 21st 2022, 5:42:37 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import pytest
import pandas as pd
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------------------ #
#                                     SPARK FIXTURES                                               #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def spark_dataframe():

    filepath = "tests/data/test.csv"
    pdf = pd.read_csv(filepath, header=0)
    spark = SparkSession.builder.master("local[18]").appName("Spark DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    return spark.createDataFrame(pdf)
