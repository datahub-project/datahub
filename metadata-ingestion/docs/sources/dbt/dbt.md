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