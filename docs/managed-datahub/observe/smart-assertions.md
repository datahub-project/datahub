---
description: This page provides an overview of Smart Assertions (AI Anomaly Detection)
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Smart Assertions (AI Anomaly Detection) ⚡

<FeatureAvailability saasOnly />

## What are Smart Assertions?

Smart Assertions are Anomaly Detection monitors that will train on the historical patterns of the data, and make predictions of what 'normal' looks like. They are powered by an incredibly sophisticated ML pipeline, enabling them to account for a large variety of trends in the data, including seasonality.

<p align="left">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/smart-assertion-example.png"/>
</p>

## How do I create Smart Assertions?

Today, you can create Smart Assertions for 3 types of assertions. To learn more about each one, click the respective links below:

1. [Volume](./volume-assertions.md#anomaly-detection-with-smart-assertions-)
2. [Freshness](./freshness-assertions.md#anomaly-detection-with-smart-assertions-)
3. [Column Metrics](./column-assertions.md#anomaly-detection-with-smart-assertions-)

## Improving predictions

You can improve predictions through two key levers:

1. Tuning
2. Anomaly feedback

### Tuning

To tune smart assertion predictions, you can 3 key knobs available to you. Each of these can be accessed in the **Settings** tab of the Assertion.

<p align="left">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/smart-assertion-tuning.png"/>
</p>

**Sensitivity**
A higher sensitivity will have a tighter fit on the data. A lower sensitivity will allow for more data variation before an anomaly is flagged.

**Exclusion Windows**
Set time windows to exclude from training data. This can be useful to exclude known maintenance windows or other periods of downtime, seasonal spikes e.g. holidays or any other windows that are not representative of normal data trends.

**Training data lookback window**
This is the amount of days our ML models will look back to gather training data to generate predictions. If this is too large, we may pick up on old data that is no longer part of the current trend. If it is too short, we may miss key seasonal patterns. You can leverage this alongside exclusion windows to improve qualtiy of predictions.

### Anomaly Feedback

#### False alarms

When an anomaly is flagged by the smart assertion, you may hover over the result dot and select `Mark as Normal`. This will include this data point in the training set, and the model will adjust to ensure it does not flag such data points as anomalies again.

**However**, there are some cases where anomalies are actually expected. For instance, if this is a sudden jump in data, **but it will be the new normal moving forward**, we recommend selecting the `Train as new Normal` option. This will add an exclusion window to all data prior to this run event, and the smart assertion will begin training on data points from this point forward.

#### Missed alarms

If an anomaly is not caught by our Smart Assertions, we recommend doing a few things:

1. You can click `Mark as Anomaly` to flag this specific data point as an anomaly.
2. You should look back at the historical stats on the `Stats` tab of the table, and see if there were any periods of anomalous data that may be polluting the training set. If so, add an `Exclusion Window` in the **Settings tab** of the assertion, to remove this polluted period of data.
3. Finally, consider increasing the sensitivity of the assertion in the **Settings tab**

<p align="left">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/smart-assertion-feedback.png"/>
</p>
