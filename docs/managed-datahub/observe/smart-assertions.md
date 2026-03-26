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

Today, you can create Smart Assertions for 4 types of assertions. To learn more about each one, click the respective links below:

1. [Volume](./volume-assertions.md#anomaly-detection-with-smart-assertions-)
2. [Freshness](./freshness-assertions.md#anomaly-detection-with-smart-assertions-)
3. [Column Metrics](./column-assertions.md#anomaly-detection-with-smart-assertions-)
4. [Custom SQL](./custom-sql-assertions.md#anomaly-detection-with-smart-assertions-)

You can also create Smart Assertions at scale using [Monitoring Rules](/docs/managed-datahub/observe/data-health-dashboard.md#monitoring-rules) on the Data Health page. Monitoring Rules let you define a search predicate (e.g. a domain, platform, or schema) and automatically apply Freshness, Volume, and Schema anomaly monitors to all matching datasets — including new datasets as they appear.

## Time-Series Bucketing

Smart Assertions can be configured with **time-series bucketing** to evaluate data quality at a day or week granularity. Instead of checking the entire table each time, the assertion partitions rows into time buckets using a timestamp column and evaluates each bucket independently.

This is especially powerful for Smart Assertions because it enables the AI model to learn patterns like "Mondays always have higher volume" or "weekend null counts are typically lower", leading to more accurate anomaly detection.

Time-series bucketing is supported for:

- [Smart Volume Assertions](./volume-assertions.md#time-series-bucketing)
- [Smart Column Metric Assertions](./column-assertions.md#time-series-bucketing-for-column-metric-assertions)

## Backfill Assertion History

When you create a Smart Assertion with time-series bucketing, you can optionally **backfill historical data** so the AI model has enough context to make accurate predictions from day one. Without backfill, the model needs to accumulate data over days or weeks of scheduled evaluations before it can reliably detect anomalies.

With backfill enabled, the system queries your warehouse for historical data and populates the assertion's metrics history immediately. This means you get meaningful anomaly detection thresholds right away, with full awareness of seasonality patterns.

For full details on how backfill works, how to configure it, and how to retry failed backfills, see the dedicated [Backfill Assertion History](./assertion-backfill.md) page.

<div align="center"><iframe width="640" height="444" src="https://www.loom.com/embed/61a201aea8464f58826c965fdbfbe255" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen></iframe></div>

## Improving Smart assertion quality

You can improve predictions through two key levers:

1. Tuning
2. Anomaly feedback

### Tuning

You can fix most Smart Assertions with 3 key actions - correct training data, adjust sensitivity, and increasing the lookback window. Each of these can be accessed via the **Tune Predictions** button on the Assertion Profile, or the **Settings tab**.

<div align="center"><iframe width="560" height="315" src="https://www.loom.com/embed/880ce4785b944a50a8662557e2ccf733?sid=192afcf0-7930-4734-9628-dba4b6717495" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen></iframe></div>

**Exclusion Windows**
Set time windows to exclude from training data. This can be useful to exclude known maintenance windows or other periods of downtime, seasonal spikes e.g. holidays or any other windows that are not representative of normal data trends.

**Sensitivity**
A higher sensitivity will have a tighter fit on the data. A lower sensitivity will allow for more data variation before an anomaly is flagged.

**Training data lookback window**
This is the number of days our ML models will look back to gather training data to generate predictions. If this is too large, we may pick up on old data that is no longer part of the current trend. If it is too short, we may miss key seasonal patterns. You can leverage this alongside exclusion windows to improve the quality of predictions.

### Anomaly Feedback

#### False alarms

When an anomaly is flagged by the smart assertion, you may hover over the result dot and select `Mark as Normal`. This will include this data point in the training set, and the model will adjust to ensure it does not flag such data points as anomalies again.

**However**, there are some cases where anomalies are actually expected. For instance, if this is a sudden jump in data, **but it will be the new normal moving forward**, we recommend selecting the `Train as new Normal` option. This will add an exclusion window to all data prior to this run event, and the smart assertion will begin training on data points from this point forward.

#### Missed alarms

If an anomaly is not caught by our Smart Assertions, we recommend doing a few things:

1. You can click `Mark as Anomaly` to flag this specific data point as an anomaly. This will exclude that data point from the training data.
2. Click **Tune Predictions** on the assertion, then exclude any “bad” historical periods from the training set (by adding an `Exclusion Window`). This is useful if older incidents or one-off events are polluting the model’s notion of “normal”.
3. Finally, consider increasing the sensitivity of the assertion in the **Settings tab** which will reduce the range of allowable values.

<p align="left">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/observe/shared/smart-assertion-feedback.png"/>
</p>
