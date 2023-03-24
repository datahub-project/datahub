---
title: Setup
---
# Tableau Ingestion Guide: Setup & Prerequisites

In order to configure ingestion from Tableau, you'll first have to enable Tableau Metadata API and you should have a user with Site Administrator Explorer permissions.

## Tableau Prerequisites

1. **Enable Tableau Metadata API:** 

   - Open a command prompt as an admin on the initial node (*where TSM is installed*) in the cluster
   - Run the command: `tsm maintenance metadata-services enable`

2. **Allow Site Administrator Explorer permissions:**

   a. login to tableau cloud https://sso.online.tableau.com/public/idp/SSO

   b. Navigate to `Users`

   <p align="center">
   <img width="75%" alt="Navigate to the Users tab" src="https://raw.githubusercontent.com/datahub-project/static-assets-fork/main/imgs/guides/tableau/tableau-user-navigation-window.png"/>
   </p> 

   c. Select particular user and click on `Actions` -> `Site Role`

   <p align="center">
   <img width="75%" alt="Actions Site Role" src="https://raw.githubusercontent.com/datahub-project/static-assets-fork/main/imgs/guides/tableau/tableau-user-list.png"/>
   </p> 

   d. Change user role to `Site Administrator Explorer`

   <p align="center">
   <img width="75%" alt="tableau site role" src="https://raw.githubusercontent.com/datahub-project/static-assets-fork/main/imgs/guides/tableau/tableau-site-role.png"/>
   </p> 

## Next Steps

Once you've done all of the above in Tableau, it's time to [move on](configuration.md) to configuring the actual ingestion source within DataHub.

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*