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
# Modified   : Friday June 24th 2022 01:28:57 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
# Remember to propagate any changes made to the database to test_dao_setup.sql
import os
import inspect
import pytest
import logging
import logging.config
from datetime import datetime

from deepctr import Entity
from deepctr.dal import STAGES
from deepctr.dal.base import File
from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.dao import FileDAO  # DagDAO, TaskDAO, , S3FileDAO

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# # ================================================================================================ #
# #                                      TEST DAG DAO                                                #
# # ================================================================================================ #
# @pytest.mark.dao
# @pytest.mark.dagdao
# @pytest.mark.skip
# class TestDagDAO:

#     entity = DagEntity(name="test_dag", desc="test dag entity", n_tasks=10, created=datetime.now())

#     def test_insert(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = DagDAO(connection)

#         for i in range(1, 6):
#             TestDagDAO.entity.name = TestDagDAO.entity.name + "_" + str(i)

#             TestDagDAO.entity = dao.add(TestDagDAO.entity)
#             assert isinstance(TestDagDAO.entity, DagEntity), logger.error(
#                 "Failure {} {}: Did not return a DagEntity object".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert TestDagDAO.entity.id == 1, logger.error(
#                 "Failure {} {}: Entity id not changed".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_find(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = DagDAO(connection)

#         for i in range(1, 6):
#             entity = dao.find(i)
#             assert entity.id == i, logger.error(
#                 "Failure {} {}: Didn't find matching Dag".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_findall(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = DagDAO(connection)
#         entities = dao.findall()
#         assert len(entities) == 5, logger.error(
#             "Failure {} {}: Didn't find all entities.".format(
#                 self.__class__.__name__, inspect.stack()[0][3]
#             )
#         )

#         for entity in entities:
#             assert entity.id in range(1, 6), logger.error(
#                 "Failure {} {}: Id out of range.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert isinstance(entity.n_tasks, int), logger.error(
#                 "Failure {} {}: n_tasks not int.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.n_tasks == 10, logger.error(
#                 "Failure {} {}: n_tasks != 10.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert isinstance(entity.n_tasks_done, int), logger.error(
#                 "Failure {} {}: n_tasks_done not int.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert isinstance(entity.created, datetime), logger.error(
#                 "Failure {} {}: created not datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert not isinstance(entity.modified, datetime), logger.error(
#                 "Failure {} {}: modified is datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert not isinstance(entity.started, datetime), logger.error(
#                 "Failure {} {}: started is datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert not isinstance(entity.stopped, datetime), logger.error(
#                 "Failure {} {}: stopped is datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert isinstance(entity.duration, int), logger.error(
#                 "Failure {} {}: duration not int.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert isinstance(entity.return_code, int), logger.error(
#                 "Failure {} {}: return_code not int.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_update(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = DagDAO(connection)
#         duration = (
#             datetime.fromisoformat("2022-06-20T00:11:53")
#             - datetime.fromisoformat("2022-06-20T00:07:53")
#         ).total_seconds()

#         for i in range(1, 6):
#             entity_in = dao.find(i)

#             # Started
#             entity_in.started = datetime.fromisoformat("2022-06-20T00:07:53")
#             dao.update(entity_in)

#             # Updated
#             entity_in.modified = datetime.fromisoformat("2022-06-20T00:09:53")
#             entity_in.n_tasks_done = i
#             dao.update(entity_in)

#             # Stopped
#             entity_in.stopped = datetime.fromisoformat("2022-06-20T00:11:53")
#             entity_in.duration = duration
#             entity_in.return_code = 0
#             dao.update(entity_in)

#             # Get the updated entity
#             entity = dao.find(i)

#             assert entity.started == datetime.fromisoformat("2022-06-20T00:07:53")
#             assert entity.modified == datetime.fromisoformat("2022-06-20T00:09:53")
#             assert entity.stopped == datetime.fromisoformat("2022-06-20T00:11:53")
#             assert entity.duration == duration

#         dao.rollback()

#         # Confirm rollback
#         entities = dao.findall()
#         for entity in entities:
#             assert entity.started is None, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.modified is None, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.stopped is None, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.duration == 0, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


# # ================================================================================================ #
# #                                      TEST TASK DAO                                               #
# # ================================================================================================ #
# @pytest.mark.dao
# @pytest.mark.taskdao
# @pytest.mark.skip
# class TestTaskDAO:

#     entity = TaskEntity(
#         name="test_task", desc="test task entity", seq=1, dag_id=3, created=datetime.now()
#     )

#     def test_insert(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = TaskDAO(connection)

#         for i in range(1, 6):
#             TestTaskDAO.entity.name = TestTaskDAO.entity.name + "_" + str(i)

#             TestTaskDAO.entity = dao.add(TestTaskDAO.entity)
#             assert isinstance(TestTaskDAO.entity, TaskEntity), logger.error(
#                 "Failure {} {}: Did not return a TaskEntity object".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert TestTaskDAO.entity.id == 1, logger.error(
#                 "Failure {} {}: Entity id not changed".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_find(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = TaskDAO(connection)

#         for i in range(1, 6):
#             entity = dao.find(i)
#             assert entity.id == i, logger.error(
#                 "Failure {} {}: Didn't find matching Task".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_findall(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = TaskDAO(connection)
#         entities = dao.findall()
#         assert len(entities) == 5, logger.error(
#             "Failure {} {}: Didn't find all entities.".format(
#                 self.__class__.__name__, inspect.stack()[0][3]
#             )
#         )

#         for entity in entities:
#             assert entity.id in range(1, 6), logger.error(
#                 "Failure {} {}: Id out of range.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert isinstance(entity.seq, int), logger.error(
#                 "Failure {} {}: n_tasks not int.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.seq == 1, logger.error(
#                 "Failure {} {}: n_tasks != 10.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert isinstance(entity.created, datetime), logger.error(
#                 "Failure {} {}: created not datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert not isinstance(entity.modified, datetime), logger.error(
#                 "Failure {} {}: modified is datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert not isinstance(entity.started, datetime), logger.error(
#                 "Failure {} {}: started is datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert not isinstance(entity.stopped, datetime), logger.error(
#                 "Failure {} {}: stopped is datetime.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert isinstance(entity.duration, int), logger.error(
#                 "Failure {} {}: duration not int.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#             assert isinstance(entity.return_code, int), logger.error(
#                 "Failure {} {}: return_code not int.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#     def test_update(self, caplog, connection):
#         logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

#         dao = TaskDAO(connection)
#         duration = (
#             datetime.fromisoformat("2022-06-20T00:11:53")
#             - datetime.fromisoformat("2022-06-20T00:07:53")
#         ).total_seconds()

#         for i in range(1, 6):
#             entity_in = dao.find(i)

#             # Started
#             entity_in.started = datetime.fromisoformat("2022-06-20T00:07:53")
#             dao.update(entity_in)

#             # Updated
#             entity_in.modified = datetime.fromisoformat("2022-06-20T00:09:53")
#             entity_in.n_tasks_done = i
#             dao.update(entity_in)

#             # Stopped
#             entity_in.stopped = datetime.fromisoformat("2022-06-20T00:11:53")
#             entity_in.duration = duration
#             entity_in.return_code = 0
#             dao.update(entity_in)

#             # Get the updated entity
#             entity = dao.find(i)

#             assert entity.started == datetime.fromisoformat("2022-06-20T00:07:53")
#             assert entity.modified == datetime.fromisoformat("2022-06-20T00:09:53")
#             assert entity.stopped == datetime.fromisoformat("2022-06-20T00:11:53")
#             assert entity.duration == duration

#         dao.rollback()

#         # Confirm rollback
#         entities = dao.findall()
#         for entity in entities:
#             assert entity.started is None, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.modified is None, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.stopped is None, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )
#             assert entity.duration == 0, logger.error(
#                 "Failure {} {}: Entity didn't rollback.".format(
#                     self.__class__.__name__, inspect.stack()[0][3]
#                 )
#             )

#         logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


# ================================================================================================ #
#                                   TEST LOCALFILE DAO                                             #
# ================================================================================================ #
FILE_NAME = "test_dao_input"
FILE_SOURCE = "alibaba"
FILE_DATASET = "test_dataset"
FILE_STORAGE_TYPE = "local"
FILE_FORMAT = "csv"
FILE_STAGE_ID = 2
FILE_STAGE_NAME = STAGES.get(FILE_STAGE_ID)
FILE_HOME = "tests/data/test_dal/test_fao"
FILE_BUCKET = "deepctr"
FILE_FILEPATH = os.path.join(FILE_HOME, FILE_SOURCE, FILE_DATASET, "2_loaded", FILE_NAME + ".csv")
FILE_COMPRESSED = False
FILE_SIZE = 0
FILE_ID = 3
FILE_CREATED = datetime.now()
FILE_OBJECT_KEY = "alibaba/vesuvio/ad_feature.csv.tar.gz"


@pytest.mark.dao
@pytest.mark.filedao
class TestFileDAO:
    def check_filepath(self, entity: Entity) -> None:
        stage_name = str(entity.stage_id) + "_" + entity.stage_name
        filename = (entity.name + "." + entity.format).replace(" ", "").lower()
        folder = os.path.join(entity.home, entity.source, entity.dataset, stage_name)
        filepath = os.path.join(folder, filename)
        assert filepath == entity.filepath

    def check_entity(self, entity: Entity, i: int) -> None:
        assert entity.id == i
        assert entity.name == FILE_NAME + "_" + str(i)
        assert entity.source == FILE_SOURCE
        assert entity.dataset == FILE_DATASET
        assert entity.format == FILE_FORMAT
        assert entity.stage_id == FILE_STAGE_ID
        assert entity.stage_name == STAGES.get(FILE_STAGE_ID)
        assert entity.home == FILE_HOME
        assert entity.bucket is None
        self.check_filepath(entity)
        assert entity.compressed == FILE_COMPRESSED
        assert entity.size == 0
        assert isinstance(entity.created, datetime)

    def test_insert(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = FileDAO(connection)

        for i in range(1, 6):
            entity = File(
                name=FILE_NAME + "_" + str(i),
                source=FILE_SOURCE,
                dataset=FILE_DATASET,
                storage_type=FILE_STORAGE_TYPE,
                format=FILE_FORMAT,
                stage_id=FILE_STAGE_ID,
                home=FILE_HOME,
                created=datetime.now(),
            )

            entity = dao.add(entity)
            assert isinstance(entity, File)
            self.check_entity(entity, i)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = FileDAO(connection)

        for i in range(1, 6):
            entity = dao.find(i)
            self.check_entity(entity, i)
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = FileDAO(connection)
        entities = dao.findall()
        assert len(entities) == 5, logger.error(
            "Failure {} {}: Didn't find all entities.".format(
                self.__class__.__name__, inspect.stack()[0][3]
            )
        )

        for entity in entities:
            self.check_entity(entity, entity.id)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = FileDAO(connection)

        for i in range(1, 6):
            entity_in = dao.find(i)

            # Filepath
            entity_in.filepath = "/some/filepath"
            dao.update(entity_in)

            # Compressed
            entity_in.compressed = True
            dao.update(entity_in)

            # Size
            entity_in.size = 12345
            dao.update(entity_in)

            # Get the updated entity
            entity = dao.find(i)

            assert entity.filepath == "/some/filepath"
            assert entity.compressed is True
            assert entity.size == 12345

        dao.rollback()

        # Confirm rollback
        entities = dao.findall()
        for entity in entities:
            assert entity.filepath != "/some/filepath/"
            assert entity.compressed is False
            assert entity.size == 0
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
