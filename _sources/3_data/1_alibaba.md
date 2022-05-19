---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.8
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Alibaba Ad Display Click Dataset
The Alibaba Ad Display Click Dataset originates from the real-world traffic logs of the Taobao Marketplace recommender system. Headquartered in Hangzhou, Zhejiang, People's Republic of China, Taobao Marketplace facilitates consumer-to-consumer (C2C) retail for small businesses and entrepreneurs.

{numref}`ali_display_summary`: Alibaba Ad Display / Click Summary

```{table} Dataset Summary
:name: ali_display_summary

|        Users        |    User Profiles    |     Interactions     |            Advertising Campaigns           |       Behaviors       |
|:-------------------:|:-------------------:|:--------------------:|:------------------------------------------:|:---------------------:|
|          1,140,000  |          1,061,768  |          26,557,961  |                                   846,811  |          723,268,134  |
```
The advertising / click interactions dataset summarized in {numref}`ali_display_summary` represent the raw ads and impressions served to approximately 1.1 million randomly selected users over the eight days beginning May 6, 2017, and ending on May 13, 2017. In addition, user profiles were obtained for 1.06 million of the users to whom ads were served during the eight day period. In total, some 26 million interactions were captured.

The dataset also includes 22 additional days of user behaviors - page views, favorite tagging, shopping cart activity and product purchases. Data collected from rendered a user behavior log exceeding 700 million actions.

## Dataset Organization
Four files comprise the Alibaba Ad Display / Click Dataset:
- Raw samples of advertising impressions,
- User profiles,
- Ad campaigns, and
- User behaviors

The entities and attributes described below have been  are described below.

### Raw Sample
Over 26 million user/advertisement interactions are collectively defined by:

| Feature    | Column     | Type   | Description                                                             |
|------------|------------|--------|-------------------------------------------------------------------------|
| user       | user_id    | int    |  A de-identified identifier for a   user. (Composite Primary Key)       |
| time_stamp | timestamp  | bigint |  The timestamp when the interaction   occurred. (Composite Primary Key) |
| adgroup_id | adgroup_id | int    |  A desensitized advertising unit   identifier.                          |
| pid        | scenario   | int    |  Definition unspecified.                                                |
| noclk      | non_click  | int    |  Binary indicator of no click. 0 if   yes. 1 if no.                     |
| clk        | click      | int    |  Binary indicator of the occurence   of a click. 1 if yes. 0 if no.     |

### Advertising Campaign
About 846,000 Advertising Campaigns connect customer and item. The six features are:

| Feature     | Column      | Type  | Description                                                  |
|-------------|-------------|-------|--------------------------------------------------------------|
| adgroup_id  | adgroup_id  | int   |  A desensitized advertising unit   identifier. (Primary Key) |
| cate_id     | category_id | int   |  A product's decensitized commodity   category id.           |
| campaign_id | campaign_id | int   |  A desensitized advertising plan   identifier.               |
| customer    | customer_id | int   |  A desensitized customer segment   identifier.               |
| brand       | brand_id    | float |  A desensitized brand to which the   product belongs.        |
| price       | price       | float |  The price for the product.   Currency not specified.        |

### User Profile
Some 1.06 million user profiles are defined by the following nine attributes:

| Feature              | Column            | Type  | Description                                             |
|----------------------|-------------------|-------|---------------------------------------------------------|
| userid               | user_id           | int   |  A de-identified identifier for a   user. (Primary Key) |
| cms_segid            | cms_segment_id    | int   |  A micro-group identifier.                              |
| cms_group_id         | cms_group_id      | int   |  Unspecified                                            |
| final_gender_code    | gender_code       | int   |  1 for male, 2 for female.                              |
| age_level            | age_level         | int   |  Unspecified                                            |
| pvalue_level         | consumption_level | float | 1:  low- grade, 2.0:  mid-grade, 3.0:  high-grade.      |
| shopping_level       | shopping_level    | int   | 1:  shallow user , 2:  moderate user , 3:  deep user    |
| occupation           | student           | int   |  1 if user is a college student, 0   if no.             |
| new_user_class_level | city_level        | float |  Unspecified.                                           |

### User Behavior
The behavior file contains over 700 million actions, and has the following five attributes:

| Feature              | Column            | Type  | Description                                             |
|----------------------|-------------------|-------|---------------------------------------------------------|
| userid               | user_id           | int   |  A de-identified identifier for a   user. (Primary Key) |
| cms_segid            | cms_segment_id    | int   |  A micro-group identifier.                              |
| cms_group_id         | cms_group_id      | int   |  Unspecified                                            |
| final_gender_code    | gender_code       | int   |  1 for male, 2 for female.                              |
| age_level            | age_level         | int   |  Unspecified                                            |
| pvalue_level         | consumption_level | float | 1:  low- grade, 2.0:  mid-grade, 3.0:  high-grade.      |
| shopping_level       | shopping_level    | int   | 1:  shallow user , 2:  moderate user , 3:  deep user    |
| occupation           | student           | int   |  1 if user is a college student, 0   if no.             |
| new_user_class_level | city_level        | float |  Unspecified.                                           |


The original dataset may be obtained from the [Alibaba Cloud Tianchi website](https://tianchi.aliyun.com/dataset/dataDetail?dataId=56&userId=1)


```
