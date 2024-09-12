
import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Snowflake Tag Propagation Automation

<FeatureAvailability saasOnly />

## Introduction

Snowflake Tag Propagation is an automation that allows you to sync DataHub Glossary Terms and Tags on
both columns and tables back to Snowflake. This automation is available in DataHub Cloud (Acryl) only.

## Capabilities

- Automatically Add DataHub Glossary Terms to Snowflake Tables and Columns
- Automatically Add DataHub Tags to Snowflake Tables and Columns
- Automatically Remove DataHub Glossary Terms and Tags from Snowflake Tables and Columns when they are removed in DataHub

## Enabling Snowflake Tag Sync

1. **Navigate to Automations**: Click on 'Govern' > 'Automations' in the navigation bar.

<p align="left">
  <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
</p>

2. **Create An Automation**: Click on 'Create' and select 'Snowflake Tag Propagation'.

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/snowflake-tag-propagation/automation-type.png"/>
</p>

3. **Configure Automation**: Fill in the required fields to connect to Snowflake, along with the name, description, and category. 
Note that you can limit propagation based on specific Tags and Glossary Terms. If none are selected, then ALL Tags or Glossary Terms will be automatically
propagated to Snowflake tables and columns. Finally, click 'Save and Run' to start the automation

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/snowflake-tag-propagation/automation-form.png"/>
</p>

## Propagating for Existing Assets

You can back-fill historical data for existing assets to ensure that all existing column and table Tags and Glossary Terms are propagated to Snowflake.
Note that it may take some time to complete the initial back-filling process, depending on the number of Snowflake assets you have.

To do so, navigate to the Automation you created in Step 3 above, click the 3-dot "More" menu

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

and then click "Initialize".

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>

This one-time step will kick off the back-filling process for existing descriptions. If you only want to begin propagating
descriptions going forward, you can skip this step.

## Rolling Back Propagated Tags

You can rollback all tags and glossary terms that have been propagated historically.

This feature allows you to "clean up" or "undo" any accidental propagation that may have occurred automatically, in the case
that you no longer want propagated descriptions to be visible.

To do this, navigate to the Automation you created in Step 3 above, click the 3-dot "More" menu

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

and then click "Rollback".

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-rollback.png"/>
</p>

This one-time step will remove all propagated tags and glossary terms from Snowflake. To simply stop propagating new tags, you can disable the automation.

## Viewing Propagated Tags

You can view propagated Tags (and corresponding DataHub URNs) inside the Snowflake UI to confirm the automation is working as expected. 

<p align="left">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/snowflake-tag-propagation/view-snowflake-tags.png"/>
</p>
