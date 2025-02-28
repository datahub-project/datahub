---
title: Setup
---

# Looker & LookML Ingestion Guide: Setup

## Looker Prerequisites

To configure ingestion from Looker, you'll first have to ensure you have an API key to access the Looker resources.

### Login To Looker Instance

Login to your Looker instance(e.g. `https://<your-looker-instance-name>.cloud.looker.com`).

Navigate to **Admin Panel** & click **Roles** to open Roles Panel.

<p align="center">
   <img width="75%" alt="Looker home page" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-home-page.png"/>
</p>

<p align="center">
   <img width="30%" alt="Looker roles search" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-roles-search.png"/>
</p>

### Create A New Permission Set

On **Roles Panel**, click **New Permission Set**.

   <p align="center">
   <img width="75%" alt="Looker new permission set" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-permission-set-button.png"/>
   </p>

Set a name for the new permission set (e.g., *DataHub Connector Permission Set*) and select the following permissions.

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

After selecting all permissions mentioned above, click **New Permission Set** at the bottom of the page.

<p align="center">
<img width="75%" alt="Looker permission set window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-permission-set-window.png"/>
</p>

### Create A Role

On the **Roles** Panel, click **New Role**.

<p align="center">
<img width="75%" alt="Looker new role button" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-role-button.png"/>
</p>

Set the name for the new role (e.g., *DataHub Extractor*) and set the following fields on this window. 

- Set **Permission Set** to permission set created in previous step (i.e *DataHub Connector Permission Set*)
- Set **Model Set** to `All`

Finally, click **New Role** at the bottom of the page. 

   <p align="center">
   <img width="75%" alt="Looker new role window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-new-role-window.png"/>
   </p>

### Create A New User

On the **Admin** Panel, click **Users** to open the users panel.

   <p align="center">
   <img width="75%" alt="Looker user search" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-search.png"/>
   </p>

Click **Add Users**. 

   <p align="center">
   <img width="75%" alt="Looker add user" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-add-user-button.png"/>
   </p>

On **Adding a new user**, set details in the following fields. 

- Add user's **Email Addresses**.
- Set **Roles** to the role created in previous step (e.g. *DataHub Extractor*) 

Finally, click **Save**.

<p align="center">
<img width="75%" alt="Looker new user window" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-add-new-user.png"/>
</p>

### Create An API Key

On the **User** Panel, click on the newly created user. 

<p align="center">
<img width="75%" alt="Looker user panel" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-panel.png"/>
</p>

Click **Edit Keys** to open the **API Key** Panel. 

<p align="center">
<img width="75%" alt="Looker user view" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-user-view.png"/>
</p>

On the **API Key** Panel, click **New API Key** to generate a new **Client ID** and **Client Secret**.
<p align="center">
<img width="75%" alt="Looker new api key" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-api-key.png"/>
</p>

## LookML Prerequisites

Follow the below steps to create the GitHub Deploy Key.

### Generate a private-public SSH key pair

```bash
ssh-keygen -t rsa -f looker_datahub_deploy_key
# If prompted, don't add a passphrase to the key
```

This will typically generate two files like the one below.
* `looker_datahub_deploy_key` (private key)
* `looker_datahub_deploy_key.pub` (public key)


### Add Deploy Key to GitHub Repository

First, log in to [GitHub](https://github.com). 

Navigate to **GitHub Repository** -> **Settings** -> **Deploy Keys** and add a public key (e.g. `looker_datahub_deploy_key.pub`) as deploy key with read access. 

<p align="center">
<img width="75%" alt="Looker home page" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-deploy-key.png"/>
</p>

Make a note of the private key file. You must paste the file's contents into the GitHub Deploy Key field later while [configuring](./configuration.md) ingestion on the DataHub Portal.

## Next Steps

Once you've done all the above steps, it's time to move on to [configuring the actual ingestion source](configuration.md) within DataHub.

