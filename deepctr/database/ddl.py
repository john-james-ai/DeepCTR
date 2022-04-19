#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /load.py                                                                              #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/ctr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 12th 2022, 5:34:59 am                                                 #
# Modified : Sunday, April 17th 2022, 5:10:19 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Data definition language (ALIBABA) used to create and manage MySQL Databases"""

# ================================================================================================ #
#                                     ALIBABA DATABASE                                             #
# ================================================================================================ #
"""Defines the ALIBABA for the Alibaba Dataset"""
ALIBABA_DDL = {}
# ------------------------------------------------------------------------------------------------ #
#                             FOREIGN KEY CHECKS OFF                                               #
# ------------------------------------------------------------------------------------------------ #
ALIBABA_DDL["foreign_key_checks_off"] = """SET FOREIGN_KEY_CHECKS = 0;"""
ALIBABA_DDL["foreign_key_checks_on"] = """SET FOREIGN_KEY_CHECKS = 1;"""
# ------------------------------------------------------------------------------------------------ #
#                                    DROP TABLES                                                   #
# ------------------------------------------------------------------------------------------------ #
ALIBABA_DDL["drop_tables"] = """DROP TABLE IF EXISTS user, impression, behavior, ad;"""
# ------------------------------------------------------------------------------------------------ #
#                                  DROP DATABASES                                                  #
# ------------------------------------------------------------------------------------------------ #
ALIBABA_DDL["drop_deepctr"] = """DROP DATABASE IF EXISTS deepctr;"""


# ------------------------------------------------------------------------------------------------ #
#                                 CREATE DATABASES                                                 #
# ------------------------------------------------------------------------------------------------ #
ALIBABA_DDL["create_deepctr"] = """CREATE DATABASE deepctr;"""


# ------------------------------------------------------------------------------------------------ #
#                                  CREATE TABLES                                                   #
# ------------------------------------------------------------------------------------------------ #
# UserSchema table
ALIBABA_DDL[
    "create_user_table"
] = """
CREATE TABLE user (
    user_id INT NOT NULL PRIMARY KEY,
    cms_segment_id INT,
    cms_group_id INT,
    gender_code INT,
    age_level INT,
    consumption_level DECIMAL(8,2),
    shopping_level INT,
    student INT,
    city_level DECIMAL(8,2)
) ENGINE=InnoDB;
"""

# Ad Table
ALIBABA_DDL[
    "create_ad_table"
] = """
CREATE TABLE ad (
    adgroup_id INT NOT NULL PRIMARY KEY,
    campaign_id INT NOT NULL,
    customer_id INT NOT NULL,
    category_id INT NOT NULL,
    brand DECIMAL(8,2),
    price DECIMAL(8,2)
) ENGINE=InnoDB;
"""
# behavior table
ALIBABA_DDL[
    "create_behavior_table"
] = """
CREATE TABLE behavior (
    user_id INT NOT NULL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    btag VARCHAR(8) NOT NULL,
    category_id INT NOT NULL,
    brand DECIMAL(8,2) NOT NULL,

    INDEX idx (user_id, timestamp)
) ENGINE=InnoDB;
"""

# interaction table
ALIBABA_DDL[
    "create_impression_table"
] = """
CREATE TABLE impression (
    user_id INT NOT NULL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    adgroup_id INT NOT NULL,
    scenario VARCHAR(16) NOT NULL,
    click INT NOT NULL,

    INDEX idx (user_id, timestamp),
    INDEX agidx (adgroup_id),

    CONSTRAINT agidx FOREIGN KEY (adgroup_id)
        REFERENCES ad(adgroup_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE

) ENGINE=InnoDB;
"""
