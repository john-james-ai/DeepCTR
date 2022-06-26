#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : DeepCTR: Deep Learning for CTR Prediction                                           #
# Version    : 0.1.0                                                                               #
# Filename   : /test_dao.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/DeepCTR                                            #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 26th 2022 07:03:22 pm                                                  #
# Modified   : Sunday June 26th 2022 02:33:31 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
# Remember to propagate any changes made to the database to test_dao_setup.sql
# import os
# import inspect
# import pytest
# import logging
# import logging.config
# from datetime import datetime

# from deepctr import Entity
# from deepctr.dal import STAGES
# from deepctr.dal.base import File
# from deepctr.utils.log_config import LOG_CONFIG
# from deepctr.dal.dao import DAO

# # ------------------------------------------------------------------------------------------------ #
# logging.config.dictConfig(LOG_CONFIG)
# logger = logging.getLogger(__name__)
# # ------------------------------------------------------------------------------------------------ #

# # ================================================================================================ #
# #                                   TEST FILE DAO                                                  #
# # ================================================================================================ #
# FILE_NAME = "test_dao_input"
# FILE_SOURCE = "alibaba"
# FILE_DATASET = "test_dataset"
# FILE_STORAGE_TYPE = "local"
# FILE_FORMAT = "csv"
# FILE_STAGE_ID = 2
# FILE_STAGE_NAME = STAGES.get(FILE_STAGE_ID)
# FILE_HOME = "tests/data/test_dal/test_fao"
# FILE_BUCKET = "deepctr"
# FILE_FILEPATH = os.path.join(FILE_HOME, FILE_SOURCE, FILE_DATASET, "2_loaded", FILE_NAME + ".csv")
# FILE_COMPRESSED = False
# FILE_SIZE = 0
# FILE_ID = 3
# FILE_CREATED = datetime.now()
# FILE_OBJECT_KEY = "alibaba/vesuvio/ad_feature.csv.tar.gz"


# @pytest.mark.dao
# @pytest.mark.filedao
# @pytest.mark.skip
# class TestFileDAO:
#     def check_filepath(self, entity: Entity) -> None:
#         stage_name = str(entity.stage_id) + "_" + entity.stage_name
#         filename = (entity.name + "." + entity.format).replace(" ", "").lower()
#         folder = os.path.join(entity.home, entity.source, entity.dataset, stage_name)
#         filepath = os.path.join(folder, filename)
#         assert filepath == entity.filepath

#     def check_entity(self, entity: Entity, i: int) -> None:
#         assert entity.id == i
#         assert entity.name == FILE_NAME + "_" + str(i)
#         assert entity.source == FILE_SOURCE
#         assert entity.dataset == FILE_DATASET
#         assert entity.format == FILE_FORMAT
#         assert entity.stage_id == FILE_STAGE_ID
#         assert entity.stage_name == STAGES.get(FILE_STAGE_ID)
#         assert entity.home == FILE_HOME
#         assert entity.bucket is None
#         self.check_filepath(entity)
#         assert entity.compressed == FILE_COMPRESSED
#         assert entity.size == 0
#         assert isinstance(entity.created, datetime)

#     def test_insert(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = FileDAO(connection)

#         for i in range(1, 6):
#             entity = File(
#                 name=FILE_NAME + "_" + str(i),
#                 source=FILE_SOURCE,
#                 dataset=FILE_DATASET,
#                 storage_type=FILE_STORAGE_TYPE,
#                 format=FILE_FORMAT,
#                 stage_id=FILE_STAGE_ID,
#                 home=FILE_HOME,
#                 created=datetime.now(),
#             )

#             entity = dao.add(entity)
#             assert isinstance(entity, File)
#             self.check_entity(entity, i)

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_find(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = FileDAO(connection)

#         for i in range(1, 6):
#             entity = dao.find(i)
#             self.check_entity(entity, i)
#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_findall(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = FileDAO(connection)
#         entities = dao.findall()
#         assert len(entities) == 5, logger.error(
#             "Failure {} {}: Didn't find all entities.".format(
#                 self.__class__.__name__, inspect.stack()[0][3]
#             )
#         )

#         for entity in entities:
#             self.check_entity(entity, entity.id)

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_update(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = FileDAO(connection)

#         for i in range(1, 6):
#             entity_in = dao.find(i)

#             # Filepath
#             entity_in.filepath = "/some/filepath"
#             dao.update(entity_in)

#             # Compressed
#             entity_in.compressed = True
#             dao.update(entity_in)

#             # Size
#             entity_in.size = 12345
#             dao.update(entity_in)

#             # Get the updated entity
#             entity = dao.find(i)

#             assert entity.filepath == "/some/filepath"
#             assert entity.compressed is True
#             assert entity.size == 12345

#         dao.rollback()

#         # Confirm rollback
#         entities = dao.findall()
#         for entity in entities:
#             assert entity.filepath != "/some/filepath/"
#             assert entity.compressed is False
#             assert entity.size == 0
#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
