---
title: Setup
---

# Looker Ingestion Guide: Setup & Prerequisites

In order to configure ingestion from Looker, you'll first have to ensure you have an API key to access the Looker resources.

## Looker Prerequisites
Follow below steps to create API key.

1. **Login:** Login to `https://<instance-name>.cloud.looker.com`. Replace `<instance-name>` by your looker instance name. For example: `https://abc.cloud.looker.com`

2. **Admin Panel:** Navigate to `Admin Panel` on looker home page.

      <p align="center">
   <img width="75%" alt="Looker home page" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-home-page.png"/>
      </p>

3. **Roles Panel:** Search for `Roles` on `Admin Panel` and click `Roles` to open `Roles Panel`.

      <p align="center">
   <img width="75%" alt="Looker roles search" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-roles-search.png"/>
      </p>

4. **Create A New Permission Set:** On `Role Panel` follow below steps to create a `New Permission Set`.

   a. Go to `New Permission Set`

      <p align="center">
      <img width="75%" alt="Looker new permission set" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-permission-set-button.png"/>
      </p>

   b. Set name for  `New Permission Set`, says `DataHub Connector Permission Set` and select following permissions

      - access_data
      - see_lookml_dashboards
      - see_looks
      - see_user_dashboards
      - explore
      - see_sql
      - see_lookml
      - clear_cache_refresh
      - manage_models
      - see_datagroups
      - see_pdts
      - see_queries
      - see_schedules
      - see_system_activity
      - see_users

    Scroll down and select all permissions mentioned above & click `New Permission Set` 

      <p align="center">
      <img width="75%" alt="Looker permission set window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-permission-set-window.png"/>
      </p>

5. **Create A Role:** On `Role Panel` follow below steps to create a new role.

   a. Go to `New Role`

      <p align="center">
      <img width="75%" alt="Looker new role button" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-role-button.png"/>
      </p>

   b. Set name for `New Role`, says `DataHub Extractor` and set following fields on this window. 

      - Set `Permission Set` to permission set created in previous step i.e `DataHub Connector Permission Set` 
      - `Model Set` to `All`

    Scroll down & click `New Role` 

      <p align="center">
      <img width="75%" alt="Looker new role window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-role-window.png"/>
      </p>

6. **Create A New User:** On `Admin Panel` follow below steps to create a new user.

   a. Search for `Users` and click `Users` to open `Users Panel`

      <p align="center">
      <img width="75%" alt="Looker user search" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-search.png"/>
      </p>

   b. Click `Add Users`. 

      <p align="center">
      <img width="75%" alt="Looker add user" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-add-user-button.png"/>
      </p>

   c. On `Adding a new user` set detail in following fields. 

      - Add user's `Email Addresses` 
      - Set `Roles` to the role created in previous step i.e. `DataHub Extractor`

    click `Save` 

      <p align="center">
      <img width="75%" alt="Looker new user window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-add-new-user.png"/>
      </p>

6. **Create An API Key:** On `User Panel` follow below steps to create an API key.

   a. Click on newly created user on `User Panel`

      <p align="center">
      <img width="75%" alt="Looker user panel" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-panel.png"/>
      </p>

   b. Click `Edit Keys` to open `API Key Panel`. 

      <p align="center">
      <img width="75%" alt="Looker user view" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-view.png"/>
      </p>

   c. On `API Key Panel` click `New API Key` to generate a new `Client Id` and `Client Secret`.
      <p align="center">
      <img width="75%" alt="Looker new api key" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-api-key.png"/>
      </p>

# LookML Ingestion Guide: Setup & Prerequisites
Follow below steps to create GitHub Deploy Key

1. **Generate a private-public ssh key pair:** This will typically generate two files, e.g. `looker_datahub_deploy_key` (this is the private key) and `looker_datahub_deploy_key.pub` (this is the public 
key)

   ```bash
      ssh-keygen -t rsa -f looker_datahub_deploy_key
   ```

2. **Login to GitHub:** Login to https://github.com 


3. **Add Deploy Key:** Navigate to `GitHub Repository` -> `Settings` -> `Deploy Keys` and add public key *i.e. looker_datahub_deploy_key.pub* as deploy key with read access 

      <p align="center">
   <img width="75%" alt="Looker home page" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-deploy-key.png"/>
      </p>

4. Make note of the private key file, you will need to paste the contents of the file into the GitHub Deploy Key field later while [configuring](./configuration.md) ingestion on DataHub Portal.
## Next Steps

Once you've done all of the above steps, it's time to [move on](configuration.md) to configuring the actual ingestion source within DataHub.

_Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!_
