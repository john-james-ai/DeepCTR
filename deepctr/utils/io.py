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
# Modified : Saturday, April 9th 2022, 6:08:21 am                                                  #
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
import numpy as np
from tqdm import tqdm
import yaml
import yamlordereddictloader
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

findspark.init()


# ------------------------------------------------------------------------------------------------ #


class IO(ABC):
    """Base class for IO classes"""

    @abstractmethod
    def read(self, filepath: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame, filepath: str, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
class CsvIO(IO):
    """Handles IO of pandas DataFrames to /from CSV Files"""

    def read(
        self,
        filepath: str,
        sep: str = ",",
        header: list = None,
        names: list = None,
        usecols: list = None,
        index_col: bool = False,
        dtype: dict = None,
        n_chunks: int = 20,
        progress_bar: bool = True,
    ) -> pd.DataFrame:
        """Reads a CSV file into pandas DataFrame with progress monitor."""

        if progress_bar:
            return self._load_progress_bar(
                filepath=filepath,
                sep=sep,
                header=header,
                names=names,
                usecols=usecols,
                index_col=index_col,
                dtype=dtype,
                n_chunks=n_chunks,
            )
        else:
            return self._load_no_progress_bar(
                filepath=filepath,
                sep=sep,
                header=header,
                names=names,
                usecols=usecols,
                index_col=index_col,
                dtype=dtype,
                n_chunks=n_chunks,
            )

    def _load_progress_bar(
        self,
        filepath: str,
        sep: str = ",",
        header: list = None,
        names: list = None,
        usecols: list = None,
        index_col: bool = False,
        dtype: dict = None,
        n_chunks: int = 20,
    ) -> pd.DataFrame:

        rows = sum(1 for _ in open(filepath, "r"))

        chunksize = int(rows / n_chunks)

        chunks = []

        with tqdm(total=rows, desc="\tRows read: ") as bar:
            for chunk in pd.read_csv(
                filepath,
                sep=sep,
                header=header,
                names=names,
                usecols=usecols,
                index_col=index_col,
                dtype=dtype,
                low_memory=False,
                chunksize=chunksize,
            ):
                chunks.append(chunk)
                bar.update(len(chunk))

        df = pd.concat((f for f in chunks), axis=0)

        return df

    def _load_no_progress_bar(
        self,
        filepath: str,
        sep: str = ",",
        header: list = None,
        names: list = None,
        usecols: list = None,
        index_col: bool = False,
        dtype: dict = None,
        n_chunks: int = 20,
    ) -> pd.DataFrame:

        rows = sum(1 for _ in open(filepath, "r"))

        chunksize = int(rows / n_chunks)

        chunks = []

        for chunk in pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            names=names,
            usecols=usecols,
            index_col=index_col,
            dtype=dtype,
            low_memory=False,
            chunksize=chunksize,
        ):
            chunks.append(chunk)

        df = pd.concat((f for f in chunks), axis=0)

        return df

    def write(
        self,
        data: pd.DataFrame,
        filepath: str,
        index_label: str = None,
        sep: str = ",",
        header: bool = True,
        index: bool = False,
        n_chunks: int = 20,
    ) -> None:
        """Writes a large DataFrame to CSV file with progress monitor."""

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        chunks = np.array_split(data.index, n_chunks)

        for chunk, subset in enumerate(tqdm(chunks)):
            if chunk == 0:  # Write in 'w' mode
                data.loc[subset].to_csv(
                    filepath,
                    sep=sep,
                    header=header,
                    index_label=index_label,
                    mode="w",
                    index=index,
                )
            else:
                data.loc[subset].to_csv(
                    filepath,
                    sep=sep,
                    index_label=index_label,
                    header=None,
                    mode="a",
                    index=index,
                )


# ------------------------------------------------------------------------------------------------ #
class YamlIO(IO):
    """Reads and writes from and to Yaml files."""

    @abstractmethod
    def read(self, filepath: str, **kwargs) -> dict:
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return yaml.load(f, Loader=yamlordereddictloader.Loader)
        else:
            return {}

    @abstractmethod
    def write(self, data: dict, filepath: str, **kwargs) -> None:
        with open(filepath, "r") as f:
            yaml.dump(data, f)


# ------------------------------------------------------------------------------------------------ #
class SparkIO(IO):
    """Reads and writes data from a csv to a Spark DataFrame

    Source: https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/

    """

    def read(self, filepath: str, **kwargs) -> pyspark.sql.DataFrame:
        """Reads a csv file into a Spark DataFrame and returns it."""

        # Extract options

        header = kwargs.get("header", True)
        delimiter = kwargs.get("delimiter", ",")
        inferschema = kwargs.get("inferschema", True)
        schema = kwargs.get("schema", None)

        spark = SparkSession.builder.master("local[12]").appName("SparkIO.com").getOrCreate()

        if schema is None:

            df = spark.read.options(
                header=header, inferSchema=inferschema, delimiter=delimiter
            ).csv(filepath)
        else:
            df = spark.read.format("csv").option("header", header).schema(schema).load(filepath)

        spark.stop()

        return df

    def write(self, data: pyspark.sql.DataFrame, filepath: str, **kwargs) -> None:
        """Writes a Spark DataFrame to a csv file"""

        header = kwargs.get("header", True)

        data.write.option("header", header).csv(filepath)


# ------------------------------------------------------------------------------------------------ #


class SparkS3(IO):
    """Read/Write utility between Spark and AWS S3

    Source: https://towardsai.net/p/programming/pyspark-aws-s3-read-write-operations

    """

    def read(self, filepath: str, **kwargs) -> pyspark.sql.DataFrame:

        # Extract bucket from kwargs
        bucket = kwargs.get("bucket", None)

        # Set up Spark session on Spark Standalone Cluster
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "-- packages com.amazonaws:aws-java-sdk:1.7.4,org."
            "apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
        )

        # Spark Configuration
        conf = (
            SparkConf()
            .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            .setAppName("pyspark_aws")
            .setMaster("local[*]")
        )

        sc = SparkContext(conf=conf)
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

        # Set Spark Hadoop properties for all worker nodes
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        hadoopConf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        hadoopConf.set("fs.s3a.endpoint", "s3-us-east-1.amazonaws.com")
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        spark = SparkSession(sc)

        df = spark.read.csv(f"s3a://{bucket}/{filepath}", header=True, inferSchema=True)

        sc.stop()

        return df

    def write(self, data: pyspark.sql.DataFrame, filepath: str, **kwargs) -> None:

        # Extract bucket from kwargs
        bucket = kwargs.get("bucket", None)

        # Set up Spark session on Spark Standalone Cluster
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "-- packages com.amazonaws:aws-java-sdk:1.7.4,org."
            "apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
        )

        # Spark Configuration
        conf = (
            SparkConf()
            .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
            .setAppName("pyspark_aws")
            .setMaster("local[*]")
        )

        sc = SparkContext(conf=conf)
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

        # Set Spark Hadoop properties for all worker nodes
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        hadoopConf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        hadoopConf.set("fs.s3a.endpoint", "s3-us-east-1.amazonaws.com")
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        data.write.format("csv").option("header", "true").save(
            f"s3a://{bucket}/{filepath}", mode="overwrite"
        )

        sc.stop()
