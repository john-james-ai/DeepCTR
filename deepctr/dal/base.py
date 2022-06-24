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
# Modified   : Thursday June 23rd 2022 10:51:33 pm                                                 #
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
from deepctr.utils.aws import get_size_aws
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
#                                        FILE                                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class File(Entity):
    """Defines the interface for File objects. Note: name is inherited from Entity class."""

    # name: str Inherited from Entity class
    source: str  # The name as the dataset is externally recognized. i.e. alibaba
    dataset: str  # The collection to which the file belongs
    storage_type: str  # Either 'local' or 's3'
    format: str  # The uncompressed format of the data, i.e. csv, parquet
    stage_id: int  # The stage identifier. See lab_book.md for stage_ids
    stage_name: str = None  # Associated with stage_id. See STAGES in deepctr/dal/base
    home: str = "data"  # The home directory for all data. Can be overidden for testing.
    bucket: str = None  # The bucket containing the file. Only relevant to s3 storage_type.
    filepath: str = None  # Path to the file. Synonymous with object_key for s3.
    compressed: bool = False  # Indicates if the file is compressed
    size: int = 0  # The size of the file in bytes
    id: int = 0  # The id assigned by the database
    created: datetime = datetime.now()  # Should be overwritten if file exists.

    def __post_init__(self) -> None:
        self._validate()
        self._set_stage_name()
        self._set_filepath()
        self._set_size()

    def _validate(self) -> None:
        validate = Validator()
        self.source = validate.source(self.source)
        self.format = validate.format(self.format)
        self.stage_id = validate.stage(self.stage_id)
        self.storage_type = validate.storage_type(self.storage_type)
        if self.storage_type == "s3":
            self.filepath = validate.filepath(self.filepath)

    def _set_size(self) -> None:
        if self.storage_type == "local":
            if self.filepath:
                if os.path.exists(self.filepath):
                    self.size = os.path.getsize(self.filepath)
        else:
            self.size = get_size_aws(bucket=self.bucket, object_key=self.filepath)

    def _set_stage_name(self) -> None:
        self.stage_name = STAGES.get(self.stage_id)

    def _set_filepath(self) -> None:
        if self.storage_type == "local":
            stage_name = str(self.stage_id) + "_" + self.stage_name
            if not self.filepath:
                self.filename = (self.name + "." + self.format).replace(" ", "").lower()
                self.folder = os.path.join(self.home, self.source, self.dataset, stage_name)
                self.filepath = os.path.join(self.folder, self.filename)

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "source": self.source,
            "dataset": self.dataset,
            "stage_id": self.stage_id,
            "stage_name": self.stage_name,
            "storage_type": self.storage_type,
            "home": self.home,
            "bucket": self.bucket,
            "filepath": self.filepath,
            "format": self.format,
            "compressed": self.compressed,
            "size": self.size,
            "created": self.created,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                          DATASET                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class Dataset(Entity):
    """Defines the parameters of local datasets to which the Files belong. """

    # name: str Inherited from Entity class
    source: str  # The external data source
    stage_id: int  # The data processing stage number
    storage_type: str  # Where the data are stored. Either 's3', or 'local'
    folder: str  # Folder on hard drive or within an s3 bucket.
    bucket: str = None  # Required for s3 datasets

    files: list = field(default_factory=list)  # List of file objects

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        validate = Validator()
        self.source = validate.source(self.source)
        self.storage_type = validate.storage_type(self.storage_type)
        self.stage_id = validate.stage(self.stage_id)

    def add_file(self, file: File) -> None:
        self.files.append(file)


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

    def filepath(self, value: int) -> bool:
        # This is called for S3 files. Filepath is the object key and must not be None.
        # Could have done this in the File object, but chose to keep all validation
        # and exception handling for validation errors in the validation object.
        if value is None:
            self._fail(value)
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
