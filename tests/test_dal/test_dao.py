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
# Modified   : Tuesday June 21st 2022 02:36:36 am                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : BSD 3-clause "New" or "Revised" License                                             #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pytest
import logging
import logging.config
from datetime import datetime

from deepctr.utils.log_config import LOG_CONFIG
from deepctr.dal.entity import DagEntity, TaskEntity, LocalFileEntity, S3FileEntity
from deepctr.dal.dao import DagDAO, TaskDAO, LocalFileDAO, S3FileDAO

# ------------------------------------------------------------------------------------------------ #
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                      TEST DAG DAO                                                #
# ================================================================================================ #
@pytest.mark.dao
@pytest.mark.dagdao
class TestDagDAO:

    entity = DagEntity(name="test_dag", desc="test dag entity", n_tasks=10, created=datetime.now())

    def test_insert(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DagDAO(connection)

        for i in range(1, 6):
            TestDagDAO.entity.name = TestDagDAO.entity.name + "_" + str(i)

            TestDagDAO.entity = dao.add(TestDagDAO.entity)
            assert isinstance(TestDagDAO.entity, DagEntity), logger.error(
                "Failure {} {}: Did not return a DagEntity object".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert TestDagDAO.entity.id == 1, logger.error(
                "Failure {} {}: Entity id not changed".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DagDAO(connection)

        for i in range(1, 6):
            entity = dao.find(i)
            assert entity.id == i, logger.error(
                "Failure {} {}: Didn't find matching Dag".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DagDAO(connection)
        entities = dao.findall()
        assert len(entities) == 5, logger.error(
            "Failure {} {}: Didn't find all entities.".format(
                self.__class__.__name__, inspect.stack()[0][3]
            )
        )

        for entity in entities:
            assert entity.id in range(1, 6), logger.error(
                "Failure {} {}: Id out of range.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.n_tasks, int), logger.error(
                "Failure {} {}: n_tasks not int.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.n_tasks == 10, logger.error(
                "Failure {} {}: n_tasks != 10.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.n_tasks_done, int), logger.error(
                "Failure {} {}: n_tasks_done not int.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert isinstance(entity.created, datetime), logger.error(
                "Failure {} {}: created not datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert not isinstance(entity.modified, datetime), logger.error(
                "Failure {} {}: modified is datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert not isinstance(entity.started, datetime), logger.error(
                "Failure {} {}: started is datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert not isinstance(entity.stopped, datetime), logger.error(
                "Failure {} {}: stopped is datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert isinstance(entity.duration, int), logger.error(
                "Failure {} {}: duration not int.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert isinstance(entity.return_code, int), logger.error(
                "Failure {} {}: return_code not int.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = DagDAO(connection)
        duration = (
            datetime.fromisoformat("2022-06-20T00:11:53")
            - datetime.fromisoformat("2022-06-20T00:07:53")
        ).total_seconds()

        for i in range(1, 6):
            entity_in = dao.find(i)

            # Started
            entity_in.started = datetime.fromisoformat("2022-06-20T00:07:53")
            dao.update(entity_in)

            # Updated
            entity_in.modified = datetime.fromisoformat("2022-06-20T00:09:53")
            entity_in.n_tasks_done = i
            dao.update(entity_in)

            # Stopped
            entity_in.stopped = datetime.fromisoformat("2022-06-20T00:11:53")
            entity_in.duration = duration
            entity_in.return_code = 0
            dao.update(entity_in)

            # Get the updated entity
            entity = dao.find(i)

            assert entity.started == datetime.fromisoformat("2022-06-20T00:07:53")
            assert entity.modified == datetime.fromisoformat("2022-06-20T00:09:53")
            assert entity.stopped == datetime.fromisoformat("2022-06-20T00:11:53")
            assert entity.duration == duration

        dao.rollback()

        # Confirm rollback
        entities = dao.findall()
        for entity in entities:
            assert entity.started is None, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.modified is None, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.stopped is None, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.duration == 0, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


# ================================================================================================ #
#                                      TEST TASK DAO                                               #
# ================================================================================================ #
@pytest.mark.dao
@pytest.mark.taskdao
class TestTaskDAO:

    entity = TaskEntity(
        name="test_task", desc="test task entity", seq=1, dag_id=3, created=datetime.now()
    )

    def test_insert(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = TaskDAO(connection)

        for i in range(1, 6):
            TestTaskDAO.entity.name = TestTaskDAO.entity.name + "_" + str(i)

            TestTaskDAO.entity = dao.add(TestTaskDAO.entity)
            assert isinstance(TestTaskDAO.entity, TaskEntity), logger.error(
                "Failure {} {}: Did not return a TaskEntity object".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert TestTaskDAO.entity.id == 1, logger.error(
                "Failure {} {}: Entity id not changed".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = TaskDAO(connection)

        for i in range(1, 6):
            entity = dao.find(i)
            assert entity.id == i, logger.error(
                "Failure {} {}: Didn't find matching Task".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = TaskDAO(connection)
        entities = dao.findall()
        assert len(entities) == 5, logger.error(
            "Failure {} {}: Didn't find all entities.".format(
                self.__class__.__name__, inspect.stack()[0][3]
            )
        )

        for entity in entities:
            assert entity.id in range(1, 6), logger.error(
                "Failure {} {}: Id out of range.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.seq, int), logger.error(
                "Failure {} {}: n_tasks not int.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.seq == 1, logger.error(
                "Failure {} {}: n_tasks != 10.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.created, datetime), logger.error(
                "Failure {} {}: created not datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert not isinstance(entity.modified, datetime), logger.error(
                "Failure {} {}: modified is datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert not isinstance(entity.started, datetime), logger.error(
                "Failure {} {}: started is datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert not isinstance(entity.stopped, datetime), logger.error(
                "Failure {} {}: stopped is datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert isinstance(entity.duration, int), logger.error(
                "Failure {} {}: duration not int.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

            assert isinstance(entity.return_code, int), logger.error(
                "Failure {} {}: return_code not int.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = TaskDAO(connection)
        duration = (
            datetime.fromisoformat("2022-06-20T00:11:53")
            - datetime.fromisoformat("2022-06-20T00:07:53")
        ).total_seconds()

        for i in range(1, 6):
            entity_in = dao.find(i)

            # Started
            entity_in.started = datetime.fromisoformat("2022-06-20T00:07:53")
            dao.update(entity_in)

            # Updated
            entity_in.modified = datetime.fromisoformat("2022-06-20T00:09:53")
            entity_in.n_tasks_done = i
            dao.update(entity_in)

            # Stopped
            entity_in.stopped = datetime.fromisoformat("2022-06-20T00:11:53")
            entity_in.duration = duration
            entity_in.return_code = 0
            dao.update(entity_in)

            # Get the updated entity
            entity = dao.find(i)

            assert entity.started == datetime.fromisoformat("2022-06-20T00:07:53")
            assert entity.modified == datetime.fromisoformat("2022-06-20T00:09:53")
            assert entity.stopped == datetime.fromisoformat("2022-06-20T00:11:53")
            assert entity.duration == duration

        dao.rollback()

        # Confirm rollback
        entities = dao.findall()
        for entity in entities:
            assert entity.started is None, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.modified is None, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.stopped is None, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.duration == 0, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


# ================================================================================================ #
#                                   TEST LOCALFILE DAO                                             #
# ================================================================================================ #
@pytest.mark.dao
@pytest.mark.localfiledao
class TestLocalFileDAO:

    entity = LocalFileEntity(
        name="test_localfile",
        dataset="test localfile entity",
        source="alibaba",
        stage_id=3,
        stage_name="staged",
        format="csv",
        created=datetime.now(),
    )

    def test_insert(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = LocalFileDAO(connection)

        for i in range(1, 6):
            TestLocalFileDAO.entity.name = TestLocalFileDAO.entity.name + "_" + str(i)

            TestLocalFileDAO.entity = dao.add(TestLocalFileDAO.entity)
            assert isinstance(TestLocalFileDAO.entity, LocalFileEntity), logger.error(
                "Failure {} {}: Did not return a LocalFileEntity object".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert TestLocalFileDAO.entity.id == 1, logger.error(
                "Failure {} {}: Entity id not changed".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = LocalFileDAO(connection)

        for i in range(1, 6):
            entity = dao.find(i)
            assert entity.id == i, logger.error(
                "Failure {} {}: Didn't find matching LocalFile".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = LocalFileDAO(connection)
        entities = dao.findall()
        assert len(entities) == 5, logger.error(
            "Failure {} {}: Didn't find all entities.".format(
                self.__class__.__name__, inspect.stack()[0][3]
            )
        )

        for entity in entities:
            assert entity.id in range(1, 6), logger.error(
                "Failure {} {}: Id out of range.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.source, str), logger.error(
                "Failure {} {}: source not str.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.stage_id == 3, logger.error(
                "Failure {} {}: stage_id != 3.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.stage_name == "staged", logger.error(
                "Failure {} {}: stage_name not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.format == "csv", logger.error(
                "Failure {} {}: format not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.filepath is None, logger.error(
                "Failure {} {}: filepath is not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert not entity.compressed, logger.error(
                "Failure {} {}: comopressed is not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.size == 0, logger.error(
                "Failure {} {}: size is not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.created, datetime), logger.error(
                "Failure {} {}: created not datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = LocalFileDAO(connection)

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
            assert entity.filepath is None, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.compressed is False, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.size == 0, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


# ================================================================================================ #
#                                     TEST S3FILE DAO                                              #
# ================================================================================================ #
@pytest.mark.dao
@pytest.mark.s3filedao
class TestS3FileDAO:

    entity = S3FileEntity(
        name="test_s3file",
        dataset="test s3file entity",
        source="alibaba",
        stage_id=3,
        stage_name="staged",
        bucket="smoke",
        object_key="mirrors",
        format="csv",
        created=datetime.now(),
    )

    def test_insert(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = S3FileDAO(connection)

        for i in range(1, 6):
            TestS3FileDAO.entity.name = TestS3FileDAO.entity.name + "_" + str(i)

            TestS3FileDAO.entity = dao.add(TestS3FileDAO.entity)
            assert isinstance(TestS3FileDAO.entity, S3FileEntity), logger.error(
                "Failure {} {}: Did not return a S3FileEntity object".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert TestS3FileDAO.entity.id == 1, logger.error(
                "Failure {} {}: Entity id not changed".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_find(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = S3FileDAO(connection)

        for i in range(1, 6):
            entity = dao.find(i)
            assert entity.id == i, logger.error(
                "Failure {} {}: Didn't find matching S3File".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_findall(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = S3FileDAO(connection)
        entities = dao.findall()
        assert len(entities) == 5, logger.error(
            "Failure {} {}: Didn't find all entities.".format(
                self.__class__.__name__, inspect.stack()[0][3]
            )
        )

        for entity in entities:
            assert entity.id in range(1, 6), logger.error(
                "Failure {} {}: Id out of range.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.source, str), logger.error(
                "Failure {} {}: source not str.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.stage_id == 3, logger.error(
                "Failure {} {}: stage_id != 3.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.stage_name == "staged", logger.error(
                "Failure {} {}: stage_name not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.format == "csv", logger.error(
                "Failure {} {}: format not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.bucket == "smoke", logger.error(
                "Failure {} {}: bucket is not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.object_key == "mirrors", logger.error(
                "Failure {} {}: object_key is not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.compressed, logger.error(
                "Failure {} {}: comopressed is not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.size == 0, logger.error(
                "Failure {} {}: size is not correct".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert isinstance(entity.created, datetime), logger.error(
                "Failure {} {}: created not datetime.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_update(self, caplog, connection):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        dao = S3FileDAO(connection)

        for i in range(1, 6):
            entity_in = dao.find(i)

            # Bucket
            entity_in.bucket = "espresso"
            dao.update(entity_in)

            # object_key
            entity_in.object_key = "prague/one"
            dao.update(entity_in)

            # Compressed
            entity_in.compressed = False
            dao.update(entity_in)

            # Size
            entity_in.size = 12345
            dao.update(entity_in)

            # Get the updated entity
            entity = dao.find(i)

            assert entity.bucket == "espresso"
            assert entity.object_key == "prague/one"
            assert entity.compressed is False
            assert entity.size == 12345

        dao.rollback()

        # Confirm rollback
        entities = dao.findall()
        for entity in entities:
            assert entity.bucket == "smoke", logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.object_key == "mirrors", logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.compressed is True, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
            assert entity.size == 0, logger.error(
                "Failure {} {}: Entity didn't rollback.".format(
                    self.__class__.__name__, inspect.stack()[0][3]
                )
            )
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
