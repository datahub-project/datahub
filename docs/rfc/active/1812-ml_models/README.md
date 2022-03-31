- Start Date: 08/18/2020
- RFC PR: https://github.com/datahub-project/datahub/pull/1812
- Implementation PR(s): https://github.com/datahub-project/datahub/pull/1721

# Machine Learning Models

## Summary

Adding support for trained machine learning models and features metadata cataloging and enabling search and discovery over them. This is a step towards organizing the essential facts of machine learning models in a structured way leading to responsible democratization of machine
learning and related artificial intelligence technology. The work is inspired by Google's model card [paper](https://arxiv.org/pdf/1810.03993.pdf).

## Motivation

We need to model ML model metadata for transparent model reporting. Below are some of the reasons why storing machine learning model metadata is important:
- Search and discovery of ML models trained, across an organization.
- Drawing boundaries around a model's capabilities and limitations: There is a need to store the conditions under which a model performs best and most consistently and if it has some blind spots. It helps potential users of the models be better informed on which models are best for their specific purposes. Also, it helps minimize usage of machine learning models in contexts for which they are not well suited.
- Metrics and Limitations: A model’s performance can be measured in countless ways, but we need to catalog the metrics that are most relevant and useful. Similarly there is a need to store a model's potential limitations that are most useful to quantify.
- Ensure comparability across models in a well-informed way: Modeling metadata of ML models allows us to compare candidate models' results across not only traditional evaluation metrics but also along the axes of ethical, inclusive, and fairness
considerations.
- Promote reproducibility: Often a model is trained on transformed data, there are some preprocessing steps involved in transforming the data e.g. centering, scaling, dealing with missing values, etc. These transforms should be stored as part of model metadata to ensure reproducibility.
- Ensure Data Governance: Increasing public concern over consumer privacy is resulting in new data laws, such as GDPR and CCPA, causing enterprises to strengthen their data governance & compliance efforts. Therefore, there is a need to store compliance information of ML models containing PII or condidential data (through manual tags or automated process) to eliminate the risk of sensitive data exposure.

## Detailed design
![high level design](high_level_design.png)


As shown in the above diagram, machine learning models are using machine learning features as inputs. These machine learning features
could be shared across different machine learning models. In the example sketched above, `ML_Feature_1` and `ML_Feature_2` are used as inputs for `ML_Model_A` while `ML_Feature_2`, `ML_Feature_3` and `ML_Feature_4` are inputs for `ML_Model_B`.

### URN Representation
We'll define two [URNs](../../../what/urn.md): `MLModelUrn` and `MLFeatureUrn`.
These URNs should allow for unique identification of machine learning models and features, respectively. Machine learning models, like datasets, will be identified by combination of standardized platform urn, name of the model and the fabric type where the model belongs to or where it was generated. Here platform urn corresponds to the data platform for ML Models (like TensorFlow) - representing the platform as an urn enables us to attach platform-specific metadata to it.

A machine learning model URN will look like below:
```
urn:li:mlModel:(<<platform>>,<<modelName>>,<<fabric>>)
```
A machine learning feature will be uniquely identified by it's name and the namespace this feature belongs to.
A machine learning feature URN will look like below:
```
urn:li:mlFeature:(<<namespace>>,<<featureName>>)
```

### Entities
There will be 2 top level GMA [entities](../../../what/entity.md) in the design: ML models and ML features.
It's important to make ML features as a top level entity because ML features could be shared between different ML models.

### ML Model metadata
- Model properties: Basic information about the ML model
  - Model date
  - Model desription
  - Model version
  - Model type: Basic model architecture details e.g. if it is Naive Bayes classifier, Convolutional Neural Network, etc
  - ML features used for training
  - Hyperparameters of the model, used to control the learning process
  - Tags: Primarily to enhance search and discovery of ML models
- Ownership: Users who own the ML model, to help with directing questions or comments about the model.
- Intended Use
  - Primary intended use cases
  - Primary intended user types
  - Out-of-scope use cases
- Model Factors: Factors affecting model performance including groups, instrumentation and environments
  - Relevant Factors: Foreseeable factors for which model performance may vary
  - Evaluation Factors: Factors that are being reported
- Metrics: Measures of model performance being reported, as well as decision thresholds (if any) used.
- Training Data: Details on datasets used for training ML Models
  - Datasets used to train the ML model
  - Motivation behind choosing these datasets
  - Preprocessing steps involved: crucial for reproducibility
  - Link to the process/job that captures training execution
- Evaluation Data: Mirrors Training Data.
- Quantitative Analyses: Provides the results of evaluating the model according to the chosen metrics by linking to relevant dashboard.
- Ethical Considerations: Demonstrate the ethical considerations that went into model development, surfacing ethical challenges and solutions to stakeholders.
- Caveats and Recommendations: Captures additional concerns regarding the model
  - Did the results suggest any further testing?
  - Relevant groups that were not represented in the evaluation dataset
  - Recommendations for model use
  - Ideal characteristics of an evaluation dataset
- Source Code: Contains training and evaluation pipeline source code, along with the source code where the ML Model is defined.
- Institutional Memory: Institutional knowledge for easy search and discovery.
- Status: Captures if the model has been soft deleted or not.
- Cost: Cost associated with the model based on the project/component this model belongs to.
- Deprecation: Captures if the model has been deprecated or not.

### ML Feature metadata
- Feature Properties: Basic information about the ML Feature
  - Description of the feature
  - Data type of the feature i.e. boolean, text, etc. These also include [data types](https://towardsdatascience.com/7-data-types-a-better-way-to-think-about-data-types-for-machine-learning-939fae99a689#:~:text=In%20the%20machine%20learning%20world,groups%20are%20often%20called%20out.) particularly for Machine Learning practitioners. 
- Ownership: Owners of the ML Feature.
- Institutional Memory: Institutional knowledge for easy search and discovery.
- Status: Captures if the feature has been soft deleted or not.
- Deprecation: Captures if the feature has been deprecated or not.

### Metadata graph
![ml_model_graph](ml_model_graph.png)

An example metadata graph with complete data lineage picture is shown above. Below are the main edges of the graph
1. Evaluation dataset contains data used for quantitative analyses and is used for evaluating ML Model hence ML Model is connected to the evaluation dataset(s) through `EvaluatedOn` edge
2. Training dataset(s) contain the training data and is used for training ML Model hence ML Model is connected to the training dataset(s) through `TrainedOn` edge.
3. ML Model is connected to `DataProcess` entity which captures the training execution through a (newly proposed) `TrainedBy` edge.
4. `DataProcess` entity itself uses the training dataset(s) (mentioned in 2) as it's input and hence is connected to the training datasets through `Consumes` edge.
5. ML Model is connected to ML Feature(s) through `Contains` edge.
6. Results of the performance of ML Model can be viewed in a dashboard and is therefore connected to `Dashboard` entity through `Produces` edge.

## How we teach this

We should create/update user guides to educate users for:
 - Search & discovery experience (how to find a machine learning model in DataHub)
 - Lineage experience (how to find different entities connected to the machine learning model)

## Alternatives
A machine learning model could as well store a model ID which uniquely identifies a machine learning model in the machine learning model lifecycle management system. This can then be the only component of `MLModelUrn` however we would then need a system to retrieve model name given the model ID. Hence we chose the approach of modeling `MLModelUrn` similar to `DatasetUrn`.

## Rollout / Adoption Strategy

The design is supposed to be generic enough that any user of DataHub should easily be able
to onboard their ML model and ML feature metadata to DataHub irrespective of their machine learning platform.

Only thing users will need to do is to write an ETL script customized for their machine learning platform (if it's not already provided in DataHub repo). This ETL script will construct and emit ML model and ML feature metadata in the form of [MCEs](../../../what/mxe.md).

## Future Work

- This RFC does not cover model evolution/versions, linking related models together and how we will handle it - that will require it's own RFC.
- This RFC does not cover the UI design of ML Model and ML Feature.
- This RFC does not cover social features like subscribe and follow on ML Model and/or ML Feature.
