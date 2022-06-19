### dbt meta automated mappings
dbt allows authors to define meta properties for datasets. Checkout this link to know more - [dbt meta](https://docs.getdbt.com/reference/resource-configs/meta). Our dbt source allows users to define
actions such as add a tag, term or owner. For example if a dbt model has a meta config ```"has_pii": True```, we can define an action
that evaluates if the property is set to true and add, lets say, a ```pii``` tag.
To leverage this feature we require users to define mappings as part of the recipe. Following is how mappings can be defined -
```json
            "meta_mapping": {
                    "business_owner": {
                        "match": ".*",
                        "operation": "add_owner",
                        "config": {"owner_type": "user"},
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
We support the below actions -
1. add_tag - Requires ```tag``` property in config.
2. add_term - Requires ```term``` property in config.
3. add_owner - Requires ```owner_type``` property in config which can be either user or group.

Note:
1. Currently, dbt meta mapping is only supported for meta configs defined at the top most level or a node in manifest file. If that is not preset we will look for meta in the config section of the node.
2. For string based meta properties we support regex matching.

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



