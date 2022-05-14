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
# Modified : Tuesday, May 3rd 2022, 8:48:33 pm                                                     #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv
import pandas as pd
import yaml
import yamlordereddictloader
import pyspark
import logging
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from typing import Any


findspark.init()

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
#                                              IO                                                  #
# ------------------------------------------------------------------------------------------------ #


class IO(ABC):
    """Base class for IO classes"""

    @abstractmethod
    def read(self, filepath: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: Any, filepath: str, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class SparkParquet(IO):
    """Reads, and writes Spark DataFrames to / from Parquet storage format.."""

    def read(self, filepath: str, cores: int = 18) -> pyspark.sql.DataFrame:
        """Reads a Spark DataFrame from Parquet file resource

        Args:
            filepath (str): The path to the parquet file resource

        Returns:
            Spark DataFrame
        """

        if os.path.exists(filepath):
            local = "local[" + str(cores) + "]"
            spark = SparkSession.builder.master(local).appName("Read SparkParquet").getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            return spark.read.parquet(filepath)

        else:
            logger.error("File {} was not found.".format(filepath))
            raise FileNotFoundError()

    def write(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        header: bool = True,
        mode: str = "overwrite",
        **kwargs,
    ) -> None:
        """Writes Spark DataFrame to Parquet file resource

        Args:
            data (pyspark.sql.DataFrame): Spark DataFrame to write
            filepath (str): The path to the parquet file to be written
            header (bool): True if data contains header row. False otherwise.
            mode (str): 'overwrite' or 'append'. Default is 'overwrite'.
        """
        data.write.option("header", header).mode(mode).parquet(filepath)


# ------------------------------------------------------------------------------------------------ #
#                                        SPARK                                                     #
# ------------------------------------------------------------------------------------------------ #
class SparkCSV(IO):
    """IO using the Spark API"""

    def read(
        self,
        filepath: str,
        cores: int = 18,
        header: bool = True,
        infer_schema: bool = True,
        sep: str = ",",
    ) -> pyspark.sql.DataFrame:
        """Reads a Spark DataFrame from Parquet file resource

        Args:
            filepath (str): The path to the parquet file resource
            cores (int): The number of CPU cores to designated to the operation. Default = 18
            header (bool): True if the data contains a header row. Default = True
            infer_schema (bool): True if the data types should be inferred. Default = False
            sep (str): Column delimiter. Default = ","

        Returns:
            Spark DataFrame
        """

        if os.path.exists(filepath):
            local = "local[" + str(cores) + "]"
            spark = SparkSession.builder.master(local).appName("Read SparkCSV").getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            return spark.read.options(header=header, delimiter=sep, inferSchema=infer_schema).csv(
                filepath
            )
        else:
            logger.error("File {} was not found.".format(filepath))
            raise FileNotFoundError()

    def write(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        header: bool = True,
        sep: str = ",",
        mode: str = "overwrite",
    ) -> None:
        """Writes Spark DataFrame to Parquet file resource

        Args:
            data (pyspark.sql.DataFrame): Spark DataFrame to write
            filepath (str): The path to the parquet file to be written
            header (bool): True if data contains header. Default = True
            sep (str): Column delimiter. Default = ","
            mode (str): 'overwrite' or 'append'. Default = 'overwrite'.
        """

        data.write.csv(path=filepath, header=header, sep=sep, mode=mode)


# ------------------------------------------------------------------------------------------------ #
#                                      SPARK S3                                                    #
# ------------------------------------------------------------------------------------------------ #


class SparkS3(IO):
    """Read/Write utility between Spark and AWS S3

    Source: https://towardsai.net/p/programming/pyspark-aws-s3-read-write-operations

    """

    def read(self, filepath: str, **kwargs) -> pyspark.sql.DataFrame:
        """Reads a csv file from Amazon S3 bucket via Spark and returns pandas DataFrame

        Args:
            filepath (str): The path to the resource within the bucket, i.e. path/to/file.csv
            kwargs (dict): Contains the key/value pair 'bucket': 'bucket_name'

        Returns:
            pandas DataFrame
        """

        bucket = kwargs.get("bucket", None)
        spark = self._create_spark_session()
        sdf = spark.read.csv(f"s3a://{bucket}/{filepath}", header=True, inferSchema=True)

        pdf = sdf.toPandas()
        return pdf

    def write(self, data: Any, filepath: str, **kwargs) -> None:
        """Writes a pandas DataFrame to Amazon S3 via SparkSession

        Args:
            data (pd.DataFrame): The pandas Dataframe to write
            filepath (str): The path to the resource within the bucket, i.e. path/to/file.csv
            kwargs (dict): Contains the key/value pair 'bucket': 'bucket_name'
        """

        bucket = kwargs.get("bucket", None)
        # Convert pandas DataFrame to a Spark DataFrame object
        sdf = data.to_spark()
        sdf.write.format("csv").option("header", "true").save(
            f"s3a://{bucket}/{filepath}", mode="overwrite"
        )

    def _create_spark_session(self) -> pyspark.sql.SparkSession:

        # Set up Spark session on Spark Standalone Cluster
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "-- packages com.amazonaws:aws-java-sdk:1.7.4,org."
            "apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
        )

        # Spark Configuration
        conf = (
            SparkConf()
            .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true",)
            .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true",)
            .setAppName("pyspark_aws")
            .setMaster("local[*]")
        )

        sc = SparkContext(conf=conf).getOrCreate()
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

        # Obtain credentials for Amazon S3
        load_dotenv()
        credentials_filepath = os.getenv("credentials_filepath")
        io = YamlIO()
        credentials = io.read(filepath=credentials_filepath)
        aws_credentials = credentials["cloud"].get("amazon")
        AWS_ACCESS_KEY_ID = aws_credentials.get("key")
        AWS_SECRET_ACCESS_KEY = aws_credentials.get("password")

        # Set Spark Hadoop properties for all worker nodes
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        hadoopConf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        hadoopConf.set("fs.s3a.endpoint", "s3-us-east-1.amazonaws.com")
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        spark = SparkSession(sc)

        return spark


# ------------------------------------------------------------------------------------------------ #
class YamlIO(IO):
    """Reads and writes from and to Yaml files."""

    def read(self, filepath: str, **kwargs) -> dict:
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return yaml.load(f, Loader=yamlordereddictloader.Loader)
        else:
            return {}

    def write(self, data: dict, filepath: str, **kwargs) -> None:
        with open(filepath, "r") as f:
            yaml.dump(data, f)
