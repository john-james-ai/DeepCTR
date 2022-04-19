#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCTR: Deep Learning and Neural Architecture Selection for CTR Prediction           #
# Version  : 0.1.0                                                                                 #
# File     : /config.py                                                                            #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/DeepCTR                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, April 19th 2022, 3:46:54 pm                                                  #
# Modified : Tuesday, April 19th 2022, 4:24:35 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Classes and functions for credential configurations"""
import os
import logging
from dotenv import load_dotenv
from deepctr.utils.io import YamlIO

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Credentials:
    """Provides access to credentials."""

    __FILEPATH = None

    def __init__(self) -> None:
        load_dotenv()
        Credentials.__FILEPATH = os.getenv("credentials_filepath")

    def get_database_credentials(self, database_name: str) -> dict:
        """Obtains the credentials for the specified database

        Args:
            database_name (str): Name of database for which credentials are requested
        Return:
            Dictionary containing credentials for the requested database
        """
        return self.get_credentials(resource_type="database", resource=database_name)

    def get_cloud_credentials(self, provider: str) -> dict:
        """Obtains the credentials for the cloud provider

        Args:
            provider (str): Name of cloud services provider
        Return:
            Dictionary containing credentials for the requested cloud provider
        """
        return self.get_credentials(resource_type="cloud", resource=provider)

    def get_credentials(self, resource_type: str, resource: str) -> dict:
        """Obtains the credentials for the given resource and resource type

        Args:
            resource_type: Either 'database' or 'cloud'
            resource: Name for resource in credentials file
        Returns: dictionary
        """
        io = YamlIO()
        credentials_data = io.read(Credentials.__FILEPATH)
        try:
            return credentials_data[resource_type].get(resource)
        except KeyError as e:
            logger.error("No credentials for {} {}\n{}".format(resource_type, resource, e))
