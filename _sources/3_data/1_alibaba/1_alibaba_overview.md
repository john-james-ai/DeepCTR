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



|      File      |                Observations               |                  Size (KB)                 |
|:--------------:|:-----------------------------------------:|:------------------------------------------:|
|   raw_sample   |                              26,557,961   |                                 1,062,560  |
|   ad_feature   |                                  846,811  |                                    30,554  |
|  user_profile  |                               1,061,768   |                                    23,493  |
|  behavior_log  |                            723,268,134    |                               23,172,631   |

The advertising / click interactions dataset summarized above represent the raw ads and impressions served to approximately 1.1 million randomly selected users over the eight days beginning May 6, 2017, and ending on May 13, 2017. In addition, user profiles were obtained for 1.06 million of the users to whom ads were served during the eight day period. In total, some 26 million interactions were captured.

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

- **user** / user_id (int): A de-identified identifier for a user. (Composite Primary Key)
- **time_stamp** / timestamp (bigint): The timestamp when the interaction occurred. (Composite Primary Key)
- **adgroup_id** (int): A desensitized advertising unit identifier.
- **pid** / scenario (varchar): Definition unspecified.
- **noclk** / non_click (int): Binary indicator of no click. 0 if yes. 1 if no.
- **clk** / click (int): Binary indicator of the occurence of a click. 1 if yes. 0 if no.

### Advertising Campaign
About 846,000 Advertising Campaigns connect customer and item. The six features are:

- **adgroup_id** (int): A desensitized advertising unit identifier. (Primary Key)
- **cate_id** / category_id (int): A product's decensitized commodity category id.
- **campaign_id** (int): A desensitized advertising plan identifier.
- **customer** / customer_id (int): A desensitized customer segment identifier.
- **brand** / brand_id (float): A desensitized brand to which the product belongs.
- **price** (float): The price for the product. Currency not specified.

### User Profile
Some 1.06 million user profiles are defined by the following nine attributes:

- **userid** / user_id (int): A de-identified identifier for a user. (Primary Key)
- **cms_segid** / cms_segment_id (int): A micro-group identifier.
- **cms_group_id** (int): Unspecified
- **final_gender_code** / gender_code (int): 1 for male, 2 for female.
- **age_level** (int): Unspecified
- **pvalue_level** / consumption_level (float): 1.0: low- grade, 2.0: mid-grade, 3.0: high-grade.
- **shopping_level** (int): 1: shallow user , 2: moderate user , 3: deep user
- **occupation** / student (int): 1 if user is a college student, 0 if no.
- **new_user_class_level** / city_level (float): Unspecified.

### User Behavior
The behavior file contains over 700 million actions, and has the following five attributes:

- **user** / user_id (int): A de-identified identifier for a user. (Composite Primary Key)
- **time_stamp** / timestamp (bigint): The timestamp when the interaction occurred. (Composite Primary Key)
- **btag** (varchar): Tag describing one of the following four behaviors:
  - pv: Page view
  - fav: Like
  - cart: Add to shopping cart
  - buy: Purchase conversion
- **cate** / category_id (int): A product's decensitized commodity category id.
- **brand** / brand_id (float): A desensitized brand to which the product belongs.

The original dataset may be obtained from the [Alibaba Cloud Tianchi website](https://tianchi.aliyun.com/dataset/dataDetail?dataId=56&userId=1)

## Alibaba Downsampling: Less is More
Developing the click-through-rate machine learning and deep learning pipelines involves algorithm evaluation and selection, hyperparameter tuning for each algorithm, model validation and evaluation, neural network architecture development, search, validation, and selection. Further increasing the complexity of the task at hand, large datasets can significantly increase computational expense and time complexity. For instance, the time complexity for support vector machines is O(n3m) {cite}`SupportVectorMachines` in the number of observations (n) and the number of features (m). Even more, our neural architecture search will train and evaluate 100s to 1,000s of models which can become computationally intractable with millions of observations.

Downsampling the Alibaba Ad Display Click dataset can significantly reduce the computational, network, and IO costs; yet, the samples must be representative of the full dataset. To ensure that the downsamples reflect the characteristics of the original dataset, we implement a class-proportional, distribution-preserving, downsampling strategy inspired by {cite}`lotschOptimalDistributionpreservingDownsampling2021` that repeatedly samples and evaluates the similarity of the sample distribution density with that of the original dataset. The downsample that is most similar to the distribution density of the original dataset is selected.

## Distribution Density Measure
Several statistical methods are available for measuring and comparing density distributions. For this assignment, we will measure the degree to which the data follow a normal distribution using the Anderson-Darling test. The test statistic is defined as:

$$A^2=-n-S$$

where:
$$
S=\displaystyle\sum_{i=1}^n\frac{(2i-1)}{n}[\text{ln}\, F(Y_i) + \text{ln}(1-F(Y_{n+1-i}))]
$$

$F$ is the [cumulative distribution function](https://en.wikipedia.org/wiki/Cumulative_distribution_function) of the normal distribution, $Y_i$ is *ordered* data and $n$ is the sample size.

## Selection Criteria
Using the Anderson Darling test statistic, the best subsample will be that which had the smallest distance in each variable from the distribution of the respective original variable. Concretely, we will select the best sample as:

$$
\text{sample}_{best} = \min{(\max{(|A^2\text{orig}_i-A^2\text{sample}_i}|})) \space \forall \space i\in {0,1,...,p}
$$
where p is the number of features.


## Data
Class-proportional, distribution-preserving, representative downsamples of the data were produced at 1% and 10% downsample rates. Thus, three versions Alibaba Ad Display Click dataset are available for download:


|  Dataset  |    Description   |                   Total Size (MB)                  |       raw_sample      |       ad_feature      |      user_profile      |      behavior_log     |
|:---------:|:----------------:|:--------------------------------------------------:|:---------------------:|:---------------------:|:----------------------:|:---------------------:|
|  vesuvio  |   Full Dataset   |                                          23,720    |          26,557,961   |             846,811   |            1,061,768   |         723,268,134   |
|  trieste  |  10% Downsample  |                                          2,372     |           2,655,796   |              84,681   |               106,177  |           72,326,813  |
|  kerouac  |   1% Downsample  |                                               237  |              265,580  |                8,468  |                10,618  |            7,232,681  |

:::{note}
This table should be updated once datasets are created
:::