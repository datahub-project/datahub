### dbt meta automated mappings
dbt allows authors to define meta properties for datasets. Checkout this link to know more - [dbt meta](https://docs.getdbt.com/reference/resource-configs/meta). Our dbt source allows users to define
actions such as add a tag, term or owner. For example if a dbt model has a meta config ```"has_pii": True```, we can define an action
that evaluates if the property is set to true and add, lets say, a ```pii``` tag.
To leverage this feature we require users to define mappings as part of the recipe. The following section describes how you can build these mappings. Listed below is a meta_mapping section that among other things, looks for keys like `business_owner` and adds owners that are listed there.

<Tabs>
<TabItem value="yaml" label="YAML" default>

```yaml
meta_mapping:
  business_owner:
    match: ".*"
    operation: "add_owner"
    config:
      owner_type: user
      owner_category: BUSINESS_OWNER
  has_pii:
    match: True
    operation: "add_tag"
    config:
      tag: "has_pii_test"
  int_property:
    match: 1
    operation: "add_tag"
    config:
      tag: "int_meta_property"
  double_property:
    match: 2.5
    operation: "add_term"
    config:
      term: "double_meta_property"
  data_governance.team_owner:
    match: "Finance"
    operation: "add_term"
    config:
      term: "Finance_test"
```
</TabItem>
<TabItem value="json" label="JSON">

```json
            "meta_mapping": {
                    "business_owner": {
                        "match": ".*",
                        "operation": "add_owner",
                        "config": {"owner_type": "user", "owner_category": "BUSINESS_OWNER"},
                    },
                    "has_pii": {
                        "match": True,
                        "operation": "add_tag",
                        "config": {"tag": "has_pii_test"},
                    },
                    "int_property": {
                        "match": 1,
                        "operation": "add_tag",
                        "config": {"tag": "int_meta_property"},
                    },
                    "double_property": {
                        "match": 2.5,
                        "operation": "add_term",
                        "config": {"term": "double_meta_property"},
                    },
                    "data_governance.team_owner": {
                        "match": "Finance",
                        "operation": "add_term",
                        "config": {"term": "Finance_test"},
                    },
                }
```
</TabItem>
</Tabs>

We support the following operations:
1. add_tag - Requires ```tag``` property in config.
2. add_term - Requires ```term``` property in config.
3. add_owner - Requires ```owner_type``` property in config which can be either user or group. Optionally accepts the ```owner_category``` config property which you can set to one of ```['TECHNICAL_OWNER', 'BUSINESS_OWNER', 'DATA_STEWARD', 'DATAOWNER'``` (defaults to `DATAOWNER`).

Note:
1. Currently, dbt meta mapping is only supported for meta elements defined at the model level (not supported for columns).
2. For string meta properties we support regex matching.

With regex matching, you can also use the matched value to customize how you populate the tag, term or owner fields. Here are a few advanced examples:

#### Data Tier - Bronze, Silver, Gold

If your meta section looks like this:
```yaml
    meta:
      data_tier: Bronze # chosen from [Bronze,Gold,Silver]
```
and you wanted to attach a glossary term like `urn:li:glossaryTerm:Bronze` for all the models that have this value in the meta section attached to them, the following meta_mapping section would achieve that outcome:
```yaml
meta_mapping:
  data_tier:
    match: "Bronze|Silver|Gold"
    operation: "add_term"
     config:
       term: "{{ $match }}"
```
to match any data_tier of Bronze, Silver or Gold and maps it to a glossary term with the same name.

#### Case Numbers - create tags
If your meta section looks like this:
```yaml
    meta:
      case: PLT-4678 # internal Case Number
```
and you want to generate tags that look like `case_4678` from this, you can use the following meta_mapping section:
```yaml
meta_mapping:
  case:
    match: "PLT-(.*)"
    operation: "add_tag"
     config:
       tag: "case_{{ $match }}"
```

#### Stripping out leading @ sign

You can also match specific groups within the value to extract subsets of the matched value. e.g. if you have a meta section that looks like this:
```yaml
meta:
  owner: "@finance-team"
  business_owner: "@janet"
```
and you want to mark the finance-team as a group that owns the dataset (skipping the leading @ sign), while marking janet as an individual user (again, skipping the leading @ sign) that owns the dataset, you can use the following meta-mapping section. 
```yaml
meta_mapping:
  owner:
    match: "^@(.*)"
    operation: "add_owner"
    config:
       owner_type: group
  business_owner:
    match: "^@(?P<owner>(.*))"
    operation: "add_owner"
    config:
      owner_type: user
      owner_category: BUSINESS_OWNER
```
In the examples above, we show two ways of writing the matching regexes. In the first one, `^@(.*)` the first matching group (a.k.a. match.group(1)) is automatically inferred. In the second example, `^@(?P<owner>(.*))`, we use a named matching group (called owner, since we are matching an owner) to capture the string we want to provide to the ownership urn.


### dbt query_tag automated mappings
This works similarly as the dbt meta mapping but for the query tags 

We support the below actions -
1. add_tag - Requires ```tag``` property in config.

The below example set as global tag the query tag `tag` key's value. 
```json
"query_tag_mapping":
{
   "tag":
      "match": ".*"
      "operation": "add_tag"
      "config":
        "tag": "{{ $match }}"
}
```

### Integrating with dbt test

To integrate with dbt tests, the `dbt` source needs access to the `run_results.json` file generated after a `dbt test` execution. Typically, this is written to the `target` directory. A common pattern you can follow is:
1. Run `dbt docs generate` and upload `manifest.json` and `catalog.json` to a location accessible to the `dbt` source (e.g. s3 or local file system)
2. Run `dbt test` and upload `run_results.json` to a location accessible to the `dbt` source (e.g. s3 or local file system)
3. Run `datahub ingest -c dbt_recipe.dhub.yaml` with the following config parameters specified
   * test_results_path: pointing to the run_results.json file that you just created

The connector will produce the following things:
- Assertion definitions that are attached to the dataset (or datasets)
- Results from running the tests attached to the timeline of the dataset

#### View of dbt tests for a dataset
![test view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-tests-view.png)
#### Viewing the SQL for a dbt test
![test logic view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-test-logic-view.png)
#### Viewing timeline for a failed dbt test
![test view](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/dbt-tests-failure-view.png)



