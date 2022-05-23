#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_web.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 13th 2022 07:05:02 pm                                                    #
# Modified   : Friday May 13th 2022 07:05:02 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import pytest
import shutil
import logging
import logging.config

from deepctr.dal.params import DatasetParams, EntityParams, S3Params
from deepctr.data.web import S3
from deepctr.utils.log_config import LOG_CONFIG

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logging.getLogger("py4j").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
# ------------------------------------------------------------------------------------------------ #

HOME = "tests/data/s3/data/web"
DATASOURCE = "alibaba"
DATASET = "trieste"
STAGE = "staged"
FORMAT = "csv"
NAME = "XFORM"

BUCKET = "deepctr"
FOLDER = "test/dal/web/"
OBJECTNAME = "PIVOT"
OBJECT = os.path.join(FOLDER, OBJECTNAME) + ".csv"
OBJECT_C = os.path.join(FOLDER, OBJECTNAME) + ".csv.tar.gz"

# ------------------------------------------------------------------------------------------------ #


class Params:
    def get_source_file(self, compressed, no):

        source = EntityParams(
            datasource=DATASOURCE,
            dataset=DATASET,
            stage=STAGE,
            home=HOME,
            name=NAME + "_" + str(no),
            format=FORMAT,
            compressed=compressed,
        )
        return source

    def upload_entity(compress, force, delete=False):
        source = EntityParams(
            datasource=DATASOURCE,
            dataset=DATASET,
            stage=STAGE,
            home=HOME,
            name=NAME,
            format=FORMAT,
            compressed=compress,
        )

        object = OBJECT if not compress else OBJECT_C
        destination = S3Params(bucket=BUCKET, folder=FOLDER, OBJECT=object)
        rao = RemoteAccessObject()
        rao.upload_entity(source, destination, compress, force)
        assert rao.exists(bucket=BUCKET, object=object), logger.error(
            "Failure in {}. Object {} does not exist.".format(inspect.stack()[1][3], object)
        )

        if delete:
            rao.delete_entity(destination)
            assert not rao.exists(destination), logger.error(
                "Failure in delete_entity. Object {} not deleted.".format(object)
            )
            shutil.rmtree(source)


# ------------------------------------------------------------------------------------------------ #


@pytest.mark.skip()
@pytest.mark.dal_web_upload
class TestDALWebUploadFile:
    def test_setup(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = os.path.join(HOME, FILENAME)
        file_creator(filepath)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_engine(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        param_sets = []
        options = [True, False]
        for option1 in options:
            for option2 in options:
                for option3 in options:
                    d = {"compress": option1, "force": option2, "delete": option3}
                    param_sets.append(d)

        for param_set in param_sets:
            upload_entity(
                compress=param_set["compress"], force=param_set["force"], delete=param_set["delete"]
            )

