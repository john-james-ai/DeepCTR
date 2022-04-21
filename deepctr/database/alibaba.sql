/*SQL for building the alibaba MySQL database comprising:
    - Impressions Table
    - User Table
    - Ad Table
    - Behavior Table
*/

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS user, impression, behavior, ad;
DROP DATABASE IF EXISTS alibaba;

CREATE DATABASE alibaba;

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

CREATE TABLE ad (
    adgroup_id INT NOT NULL PRIMARY KEY,
    campaign_id INT NOT NULL,
    customer_id INT NOT NULL,
    category_id INT NOT NULL,
    brand DECIMAL(8,2),
    price DECIMAL(8,2)
) ENGINE=InnoDB;


CREATE TABLE behavior (
    user_id INT NOT NULL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    btag VARCHAR(8) NOT NULL,
    category_id INT NOT NULL,
    brand DECIMAL(8,2) NOT NULL,

    INDEX idx (user_id, timestamp)
) ENGINE=InnoDB;

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

SET FOREIGN_KEY_CHECKS = 1;

COMMIT;