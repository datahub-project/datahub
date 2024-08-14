---
title: Setup
---
# Tableau Ingestion Guide: Setup & Prerequisites

In order to configure ingestion from Tableau, you'll first have to enable Tableau Metadata API and you should have a user with Site Administrator Explorer permissions.

## Tableau Prerequisites

1. Grant `Site Administrator Explorer permissions` to a user

   A. Log in to Tableau Cloud https://sso.online.tableau.com/public/idp/SSO.
   
   B. Navigate to `Users`.

   <p align="center">
      <img width="75%" alt="Navigate to the Users tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-user-navigation-window.png"/>
   </p>

   C. **For New User**: Follow below steps to grant permission for new user.

      -  Click `Add Users` -> `Add Users by Email`

         <p align="center">
         <img width="75%" alt="Navigate to the Users tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-add-user.png"/>
         </p>

      -  Fill `Enter email addresses`, set `Site role` to `Site Administrator Explorer` and Click `Add Users`

         <p align="center">
         <img width="75%" alt="Navigate to the Users tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-new-user-site-role.png"/>
         </p>

   D. **For Existing User:**  Follow below steps to grant permission for existing user.

      -  Select a user and click `Actions` -> `Site Role`

         <p align="center">
         <img width="75%" alt="Actions Site Role" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-user-list.png"/>
         </p>

      -  Change user role to `Site Administrator Explorer`

         <p align="center">
         <img width="75%" alt="tableau site role" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-site-role.png"/>
         </p>


2. **Enable Tableau Metadata API:** This step is required only for Tableau Server. The Metadata API is installed with Tableau Server but disabled by default.

   - Open a command prompt as an admin on the initial node (*where TSM is installed*) in the cluster
   - Run the command: `tsm maintenance metadata-services enable`

3. **Enable Derived Permissions:** This step is required only when the site is using external assets. For more detail, refer to the tableau documentation [Manage Permissions for External Assets](https://help.tableau.com/current/online/en-us/dm_perms_assets.htm).

   Follow the below steps to enable the derived permissions:

   -  Sign in to Tableau Cloud or Tableau Server as an admin.
   -  From the left navigation pane, click Settings.
   -  On the General tab, under Automatic Access to Metadata about Databases and Tables, select the `Automatically grant authorized users access to metadata about databases and tables` check box.


## Next Steps

Once you've done all of the above in Tableau, it's time to [move on](configuration.md) to configuring the actual ingestion source within DataHub.

