## lookml_ingestion.py
This script ingests Looker view metadata from lookml into datahub. Looker views are essentially like database views that can be either materialized or ephemeral, so we treat them as you would any other dataset in datahub.

Underneath the hood, this script uses the `lkml` python parsing library to parse lkml files and so it comes with all the limitations of that underlying parser.

Roughly how the script works:
- Point the script at a directory on the filesystem, finds all files named `*.model.lkml` in any level of nesting
- Finds the viewfile includes in the model file, this indicates that the viewfile is a part of that model (and a model has a single SQL connection associated with it). Does not handle a model importing a view file but *not* using the view in the model since that would require parsing explore blocks and adds complexity.
- For each viewfile in the model, parses the view files. For each view in the viewfile, resolve the sql table name for the view:
	- We do not support parsing derived tables using a `sql:` block, this would require parsing SQL to understand dependencies. We only support views using `sql_table_name`. In the future, could support limited SQL parsing for limited SQL dialects.
	- We support views using the `extends` keyword: https://docs.looker.com/reference/view-params/extends-for-view This is surprisingly painful because views can extend other views in other files. We do this inefficiently right now.
	- We do not support views using `refinements`. SpotHero does not use refinements right now, so we had no need to implement it: https://docs.looker.com/data-modeling/learning-lookml/refinements
- After binding views to models and finding the sql table name associated with the views, we generate the MCE events into a separate looker platform in datahub since they are not "real" tables but "virtual" looker tables

## Steps 
- Use a version of python >= 3.7
- Make a virtual environment
- pip install -r requirements.txt
- Set env var: LOOKER_DIRECTORY to the root path of lkml on your filesystem
- Modify EXTRA_KAFKA_CONF section of script to point to datahub