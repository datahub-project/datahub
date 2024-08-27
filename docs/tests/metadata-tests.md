import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Metadata Tests

<FeatureAvailability saasOnly />

DataHub includes a highly configurable, no-code framework that allows you to configure broad-spanning monitors & continuous actions
for the data assets - datasets, dashboards, charts, pipelines - that make up your enterprise Metadata Graph.
At the center of this framework is the concept of a Metadata Test.

There are two powerful use cases that are uniquely enabled by the Metadata Tests framework:

1. Automated Asset Classification
2. Automated Metadata Completion Monitoring

### Automated Asset Classification

Metadata Tests allows you to define conditions for selecting a subset of data assets (e.g. datasets, dashboards, etc),
along with a set of actions to take for entities that are selected. After the test is defined, the actions
will be applied continuously over time, as the selection set evolves & changes with your data ecosystem.

When defining selection criteria, you'll be able to choose from a range of useful technical signals (e.g. usage, size) that are automatically
extracted by DataHub (which vary by integration). This makes automatically classifying the "important" assets in your organization quite easy, which
is in turn critical for running effective Data Governance initiatives within your organization.

For example, we can define a Metadata Test which selects all Snowflake Tables which are in the top 10% of "most queried"
for the past 30 days, and then assign those Tables to a special "Tier 1" group using DataHub Tags, Glossary Terms, or Domains.

### Automated Data Governance Monitoring

Metadata Tests allow you to define & monitor a set of rules that apply to assets in your data ecosystem (e.g. datasets, dashboards, etc). This is particularly useful when attempting to govern
your data, as it allows for the (1) definition and (2) measurement of centralized metadata standards, which are key for both bootstrapping
and maintaining a well-governed data ecosystem.

For example, we can define a Metadata Test which requires that all "Tier 1" data assets (e.g. those marked with a special Tag or Glossary Term),
must have the following metadata:

1. At least 1 explicit owner _and_
2. High-level, human-authored documentation _and_
3. At least 1 Glossary Term from the "Classification" Term Group

Then, we can closely monitor which assets are passing and failing these rules as we work to improve things over time.
We can easily identify assets that are _in_ and _out of_ compliance with a set of centrally-defined standards.

By applying automation, Metadata Tests
can enable the full lifecycle of complex Data Governance initiatives - from scoping to execution to monitoring.

## Metadata Tests Setup, Prerequisites, and Permissions

What you need to manage Metadata Tests on DataHub:

- **Manage Tests** Privilege

This Platform Privilege allows users to create, edit, and remove all Metadata Tests on DataHub. Therefore, it should only be
given to those users who will be serving as metadata Admins of the platform. The default `Admin` role has this Privilege.

> Note that the Metadata Tests feature is currently limited in support for the following DataHub Asset Types:
>
> - Dataset
> - Dashboard
> - Chart
> - Data Flow (e.g. Pipeline)
> - Data Job (e.g. Task)
> - Container (Database, Schema, Project)
>
> If you'd like to see Metadata Tests for other asset types, please let your Acryl Customer Success partner know!

## Using Metadata Tests

Metadata Tests can be created by first navigating to **Govern > Tests**.

To begin building a new Metadata, click **Create new Test**.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/manage-tests.png"/>
</p>

### Creating a Metadata Test

Inside the Metadata Test builder, we'll need to construct the 3 parts of a Metadata Test:

1. **Selection Criteria** - Select assets that are in the scope of the test
2. **Rules** - Define rules that selected assets can either pass or fail
3. **Actions (Optional)** - Define automated actions to be taken assets that are passing
   or failing the test

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-create.png"/>
</p>

#### Step 1. Defining Selection Criteria (Scope)

In the first step, we define a set of conditions that are used to select a subset of the assets in our Metadata Graph
that will be "in the scope" of the new test. Assets that **match** the selection conditions will be considered in scope, while those which do not are simply not applicable for the test.
Once the test is created, the test will be evaluated for any assets which fall in scope on a continuous basis (when an asset changes on DataHub
or once every day).

##### Selecting Asset Types

You must select at least one asset _type_ from a set that includes Datasets, Dashboards, Charts, Data Flows (Pipelines), Data Jobs (Tasks),
and Containers.

<p align="center">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-select-type.png"/>
</p>

Entities will the selected types will be considered in scope, while those of other types will be considered out of scope and
thus omitted from evaluation of the test.

##### Building Conditions

**Property** conditions are the basic unit of comparison used for selecting data assets. Each **Property** condition consists of a target _property_,
an _operator_, and an optional _value_.

A _property_ is an attribute of a data asset. It can either be a technical signal (e.g. **metric** such as usage, storage size) or a  
metadata signal (e.g. owners, domain, glossary terms, tags, and more), depending on the asset type and applicability of the signal.
The full set of supported _properties_ can be found in the table below.

An _operator_ is the type of predicate that will be applied to the selected _property_ when evaluating the test for an asset. The types
of operators that are applicable depend on the selected property. Some examples of operators include `Equals`, `Exists`, `Matches Regex`,
and `Contains`.

A _value_ defines the right-hand side of the condition, or a pre-configured value to evaluate the property and operator against. The type of the value
is dependent on the selected _property_ and *operator. For example, if the selected *operator\* is `Matches Regex`, the type of the
value would be a string.

By selecting a property, operator, and value, we can create a single condition (or predicate) used for
selecting a data asset to be tested. For example, we can build property conditions that match:

- All datasets in the top 25% of query usage in the past 30 days
- All assets that have the "Tier 1" Glossary Term attached
- All assets in the "Marketing" Domain
- All assets without owners
- All assets without a description

To create a **Property** condition, simply click **Add Condition** then select **Property** condition.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-create-property-condition.png"/>
</p>

We can combine **Property** conditions using boolean operators including `AND`, `OR`, and `NOT`, by
creating **Logical** conditions. To create a **Logical** condition, simply click **Add Condition** then select an
**And**, **Or**, or **Not** condition.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-create-logical-condition.png"/>
</p>

Logical conditions allow us to accommodate complex real-world selection requirements:

- All Snowflake Tables that are in the Top 25% of most queried AND do not have a Domain
- All Looker Dashboards that do not have a description authored in Looker OR in DataHub

#### Step 2: Defining Rules

In the second step, we can define a set of conditions that selected assets must match in order to be "passing" the test.
To do so, we can construct another set of **Property** conditions (as described above).

> **Pro-Tip**: If no rules are supplied, then all assets that are selected by the criteria defined in Step 1 will be considered "passing".
> If you need to apply an automated Action to the selected assets, you can leave the Rules blank and continue to the next step.

<p align="center">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-create-rules.png"/>
</p>

When combined with the selection criteria, Rules allow us to define complex, highly custom **Data Governance** policies such as:

- All datasets in the top 25% of query usage in the past 30 days **must have an owner**.
- All assets in the "Marketing" Domain **must have a description**
- All Snowflake Tables that are in the Top 25% of most queried AND do not have a Domain **must have
  a Glossary Term from the Classification Term Group**

##### Validating Test Conditions

During Step 2, we can quickly verify that the Selection Criteria & Rules we've authored
match our expectations by testing them against some existing assets indexed by DataHub.

To verify your Test conditions, simply click **Try it out**, find an asset to test against by searching & filtering down your assets,
and finally click **Run Test** to see whether the asset is passes or fails the provided conditions.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-validate-conditions.png"/>
</p>

#### Step 3: Defining Actions (Optional)

> If you don't wish to take any actions for assets that pass or fail the test, simply click 'Skip'.

In the third step, we can define a set of Actions that will be automatically applied to each selected asset which passes or fails the Rules conditions.

For example, we may wish to mark **passing** assets with a special DataHub Tag or Glossary Term (e.g. "Tier 1"), or remove these special marking for those which are failing.
This allows us to automatically control classifications of data assets as they move in and out of compliance with the Rules defined in Step 2.

A few of the supported Action types include:

- Adding or removing specific Tags
- Adding or removing specific Glossary Terms
- Adding or removing specific Owners
- Adding or removing to a specific Domain

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-define-actions.png"/>
</p>

#### Step 4: Name, Category, Description

In the final step, we can add a freeform name, category, and description for our new Metadata Test.

### Viewing Test Results

Metadata Test results can be viewed in 2 places:

1. On an asset profile page (e.g. Dataset profile page), inside the **Validation** tab.
2. On the Metadata Tests management page. To view all assets passing or failing a particular test,
   simply click on the labels which showing the number of passing or failing assets.

<p align="center">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-test-view-results.png"/>
</p>

### Updating an Existing Test

To update an existing Test, simply click **Edit** on the test you wish to change.

Then, make the changes required and click **Save**. When you save a Test, it may take up to 2 minutes for changes
to be reflected across DataHub.

### Removing a Test

To remove a Test, simply click on the trashcan icon located on the Tests list. This will remove the Test and
deactivate it so that it no is evaluated.

When you delete a Test, it may take up to 2 minutes for changes to be reflected.

### GraphQL

- [listTests](../../graphql/queries.md#listtests)
- [createTest](../../graphql/mutations.md#createtest)
- [deleteTest](../../graphql/mutations.md#deletetest)

## FAQ and Troubleshooting

**When are Metadata Tests evaluated?**

Metadata Tests are evaluated in 2 scenarios:

1. When an individual asset is changed in DataHub, all tests that include it in scope are evaluated
2. On a recurring cadence (usually every 24 hours) by a dedicated Metadata Test evaluator, which evaluates all tests against the Metadata Graph

**Can I configure a custom evaluation schedule for my Metadata Test?**

No, you cannot. Currently, the internal evaluator will ensure that tests are run continuously for
each asset, regardless of whether it is being changed on DataHub.

**How is a Metadata Test different from an Assertion?**

An Assertion is a specific test, similar to a unit test, that is defined for a single data asset. Typically,
it will include domain-specific knowledge about the asset and test against physical attributes of it. For example, an Assertion
may verify that the number of rows for a specific table in Snowflake falls into a well-defined range.

A Metadata Test is a broad spanning predicate which applies to a subset of the Metadata Graph (e.g. across multiple
data assets). Typically, it is defined against _metadata_ attributes, as opposed to the physical data itself. For example,
a Metadata Test may verify that ALL tables in Snowflake have at least 1 assigned owner, and a human-authored description.
Metadata Tests allow you to manage broad policies across your entire data ecosystem driven by metadata, for example to
augment a larger scale Data Governance initiative.


