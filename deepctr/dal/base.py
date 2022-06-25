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
# Created    : Thursday June 23rd 2022 09:28:39 pm                                                 #
# Modified   : Friday June 24th 2022 09:05:01 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /fao.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 02:51:48 pm                                                    #
# Modified   : Thursday June 23rd 2022 09:24:42 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Defines File and Dataset objects used throughout the DAL package."""
import os
from dataclasses import dataclass, field
from datetime import datetime
import inspect
import logging
import logging.config
from typing import Any

from deepctr import Entity
from deepctr.dal import STAGES, FORMATS, STORAGE_TYPES, SOURCES
from deepctr.data.local import SparkCSV, SparkParquet
from deepctr.data.remote import S3
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
#                                            FILE                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class File(Entity):
    """Defines the interface for File objects. Note: name is inherited from Entity class."""

    # name: str Inherited from Entity class
    source: str  # The name as the dataset as externally recognized. i.e. alibaba
    dataset: str  # The collection to which the file belongs
    storage_type: str  # Either 'local' or 's3'
    format: str  # The uncompressed format of the data, i.e. csv, parquet
    stage_id: int  # The stage identifier. See deepctr/dal/__init__.py
    stage_name: str = None  # Associated with stage_id. See STAGES in deepctr/dal/__init__.py
    home: str = "data"  # The home directory for all data. Can be overidden for testing.
    bucket: str = None  # The bucket containing the file. Only relevant to s3 storage_type.
    filepath: str = None  # Path to the file. Synonymous with object_key for s3.
    compressed: bool = False  # Indicates if the file is compressed
    size: int = 0  # The size of the file in bytes
    rows: int = 0  # The number of rows in the file.
    cols: int = 0  # The number of columns in the file.
    id: int = 0  # The id assigned by the database
    created: datetime = None
    modified: datetime = None
    accessed: datetime = None

    def __post_init__(self) -> None:
        self._validate()
        self._set_stage_name()
        self._set_filepath()
        self._set_metadata()

    def _validate(self) -> None:
        validate = Validator()
        self.source = validate.source(self.source)
        self.format = validate.format(self.format)
        self.stage_id = validate.stage(self.stage_id)
        self.storage_type = validate.storage_type(self.storage_type)

    def _set_filepath(self) -> None:
        if not self.filepath:
            stage_name = str(self.stage_id) + "_" + self.stage_name
            self.filename = (self.name + "." + self.format).replace(" ", "").lower()
            if self.storage_type == "local":
                self.folder = os.path.join(self.home, self.source, self.dataset, stage_name)
            else:
                self.folder = os.path.join(self.source, self.dataset, stage_name)
            self.filepath = os.path.join(self.folder, self.filename)

    def _set_metadata(self) -> None:
        """Sets the metadata variables including size, dates, and stage_name"""
        self.stage_name = STAGES.get(self.stage_id)
        if self.storage_type == "local":
            io = SparkCSV() if self.format == "csv" else SparkParquet()
            metadata = io.metadata(self.filepath)
        else:
            io = S3()
            metadata = io.metadata(bucket=self.bucket, object_key=self.filepath)

        self.rows = metadata.rows
        self.cols = metadata.cols
        self.size = metadata.size
        self.created = metadata.created
        self.modified = metadata.modified
        self.accessed = metadata.accessed


# ------------------------------------------------------------------------------------------------ #
#                                          DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class Dataset(Entity):
    """Defines the parameters of local datasets to which the Files belong. """

    # name: str Inherited from Entity class
    source: str  # The name as the dataset as externally recognized. i.e. alibaba
    storage_type: str  # Either 'local' or 's3'
    folder: str  # The folder containing the files in the dataset.
    format: str  # Either csv or parquet format.
    stage_id: int  # The stage identifier. See deepctr/dal/__init__.py for stage_ids
    stage_name: str = None  # Associated with stage_id. See STAGES in deepctr/dal/__init__.py
    compressed: bool = False  # Indicates whether the files are compressed.
    size: int = 0  # The total size of the dataset in bytes.
    home: str = "data"  # The home directory for all data. Can be overidden for testing.
    bucket: str = None  # The bucket containing the dataset. Only relevant to s3 storage_type.
    files: dict = field(default_factory=dict)  # Dictionary of File objects indexed by name.
    id: int = 0  # The id assigned by the database
    created: datetime = None
    modified: datetime = None
    accessed: datetime = None

    files: list = field(default_factory=list)  # List of file objects

    def __post_init__(self) -> None:
        self._validate()
        self._add_files()

    def _validate(self) -> None:
        validate = Validator()
        self.source = validate.source(self.source)
        self.format = validate.format(self.format)
        self.storage_type = validate.storage_type(self.storage_type)
        self.stage_id = validate.stage(self.stage_id)
        self.stage_name = STAGES.get(self.stage_id)

    def _add_files(self) -> None:
        if self.storage_type == "s3":
            self._add_files_remote()
        else:
            self._add_files_local()

    def _add_files_remote(self) -> None:
        pass

    def _add_files_local(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                       VALIDATOR                                                  #
# ------------------------------------------------------------------------------------------------ #
class Validator:
    """Provides validation for Dataset and File objects"""

    def source(self, value: str) -> bool:
        if value not in SOURCES:
            self._fail(value, SOURCES)
        else:
            return value

    def format(self, value: str) -> bool:
        value = value.replace(".", "")
        if value not in FORMATS:
            self._fail(value, FORMATS)
        else:
            return value

    def storage_type(self, value: str) -> bool:
        if value not in STORAGE_TYPES:
            self._fail(value, STORAGE_TYPES)
        else:
            return value

    def stage(self, value: int) -> bool:
        if value not in STAGES.keys():
            self._fail(value, STAGES)
        else:
            return value

    def _fail(self, value: Any, valid_values: list):
        variable = inspect.stack()[1][3]
        caller_method = inspect.stack()[0][3]
        caller_classname = caller_method.__class__.__name__
        msg = "Error in {}: {}. Invalid {}: {}. Valid values are: {}".format(
            caller_classname, caller_method, variable, value, valid_values
        )
        logger.error(msg)
        raise ValueError(msg)
