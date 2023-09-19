---
title: Setup
---

# Looker & LookML Ingestion Guide: Setup

## Looker Prerequisites

In order to configure ingestion from Looker, you'll first have to ensure you have an API key to access the Looker resources.

### Login To Looker Instance
Login to `https://<your-looker-instance-name>.cloud.looker.com`. 

### Navigate to Admin Panel
Navigate to `Admin Panel` on looker home page.

<p align="center">
   <img width="75%" alt="Looker home page" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-home-page.png"/>
</p>

### Open Roles Panel
Click `Roles` to open `Roles Panel`.

<p align="center">
   <img width="30%" alt="Looker roles search" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-roles-search.png"/>
</p>

### Create A New Permission Set

On `Role Panel`, go to `New Permission Set`.

   <p align="center">
   <img width="75%" alt="Looker new permission set" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-permission-set-button.png"/>
   </p>

Set name for `New Permission Set` (For example, `DataHub Connector Permission Set`) and select the following permissions.

<details>
<summary>Permission List</summary>

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

</details>

After selecting all permissions mentioned above, click `New Permission Set` at the bottom of the page.

   <p align="center">
   <img width="75%" alt="Looker permission set window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-permission-set-window.png"/>
   </p>

### Create A Role

On `Role Panel`, go to `New Role`.

   <p align="center">
   <img width="75%" alt="Looker new role button" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-role-button.png"/>
   </p>

Set name for `New Role` (For example, `DataHub Extractor`) and set following fields on this window. 

- Set `Permission Set` to permission set created in previous step (i.e `DataHub Connector Permission Set`)
- Set `Model Set` to `All`

Finally, click `New Role` at the bottom of the page. 

   <p align="center">
   <img width="75%" alt="Looker new role window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-role-window.png"/>
   </p>

### Create A New User

On `Admin Panel`, click `Users` to open `Users Panel`

   <p align="center">
   <img width="75%" alt="Looker user search" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-search.png"/>
   </p>

Click `Add Users`. 

   <p align="center">
   <img width="75%" alt="Looker add user" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-add-user-button.png"/>
   </p>

On `Adding a new user`, set detail in following fields. 

- Add user's `Email Addresses` 
- Set `Roles` to the role created in previous step i.e. `DataHub Extractor`

Finally, click `Save`.

<p align="center">
<img width="75%" alt="Looker new user window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-add-new-user.png"/>
</p>

### Create An API Key

On `User Panel`, click on newly created user on `User Panel`.

   <p align="center">
   <img width="75%" alt="Looker user panel" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-panel.png"/>
   </p>

Click `Edit Keys` to open `API Key Panel`. 

   <p align="center">
   <img width="75%" alt="Looker user view" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-view.png"/>
   </p>

On `API Key Panel`, click `New API Key` to generate a new `Client Id` and `Client Secret`.
   <p align="center">
   <img width="75%" alt="Looker new api key" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-api-key.png"/>
   </p>

## LookML Prerequisites

Follow below steps to create GitHub Deploy Key.

### Generate a private-public ssh key pair

```bash
   ssh-keygen -t rsa -f looker_datahub_deploy_key
```

This will typically generate two file like below.
* `looker_datahub_deploy_key` (private key)
* `looker_datahub_deploy_key.pub` (public key)


### Add Deploy Key to GitHub Repository

First, login to [GitHub](https://github.com). 

Navigate to `GitHub Repository` -> `Settings` -> `Deploy Keys` and add public key (i.e. looker_datahub_deploy_key.pub) as deploy key with read access 

<p align="center">
<img width="75%" alt="Looker home page" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-deploy-key.png"/>
</p>

Make note of the private key file, you will need to paste the contents of the file into the GitHub Deploy Key field later while [configuring](./configuration.md) ingestion on DataHub Portal.

## Next Steps

Once you've done all the above steps, it's time to [move on](configuration.md) to configuring the actual ingestion source within DataHub.

_Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!_
