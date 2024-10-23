---
title: "SCIM Integration: Okta and DataHub"
hide_title: true
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';

## SCIM Integration: Okta and DataHub
<FeatureAvailability saasOnly />

## Overview
This document covers the steps required to enable SCIM provisioning from Okta to DataHub.

This document assumes you are using OIDC for SSO with DataHub.
Since Okta doesn't currently support SCIM with OIDC, you would need to create an additional SWA-app-integration to enable SCIM provisioning.

On completing the steps in this guide, Okta will start automatically pushing changes to users/groups of this SWA-app-integration to DataHub, thereby simplifying provisioning of users/groups in DataHub.

### Why SCIM provisioning? 
Let us look at an example of the flows enabled through SCIM provisioning.

Consider the following configuration in Okta
- A group `governance-team`
- And it has two members `john` and `sid`
- And the group has role `Reader`

Through SCIM provisioning, the following are enabled:
* If the `governance-team` group is assigned to the DataHub app in Okta with the role `Reader`, Okta will create the users `john` and `sid` in DataHub with the `Reader` role.
* If you remove `john` from group `governance-team` then `john` would automatically get deactivated in DataHub.
* If you remove `sid` from the DataHub app in Okta, then `sid` would automatically get deactivated in DataHub. 

Generally, any user assignment/unassignment to the app in Okta - directly or through groups - are automatically reflected in the DataHub application.

This guide also covers other variations such as how to assign a role to a user directly, and how group-information can be pushed to DataHub.

> Only Admin, Editor and Reader roles are supported in DataHub. These roles are preconfigured/created on DataHub.

## Configuring SCIM provisioning

### 1. Create an SWA app integration
a). Create a new [SWA app integration](https://help.okta.com/en-us/content/topics/apps/apps_app_integration_wizard_swa.htm), called say, `DataHub-SCIM-SWA`.

Note: this app-integration will only be used for SCIM provisioning. You would continue to use the existing OIDC-app-integration for SSO. 

b). In the `General` tab of the `DataHub-SCIM-SWA` application, check the `Enable SCIM provisioning` option

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/okta/appSettingsEnableScim.png"/>
</p>

You may also want to configure the other selections as shown in the above image, so that this application isn't visible to your users.

### 2. Configure SCIM

a). Generate a personal access token from [DataHub](../../docs/authentication/personal-access-tokens.md#creating-personal-access-tokens).

b). In the `Provisioning` tab, configure the DataHub-SCIM endpoint as shown in the below image:

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/okta/scimConfig1.png"/>
</p>

**Note**: Set the value of the `Bearer` field to the personal access token obtained in step (a) above.  

c). Configure the `To App` section as shown below:

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/okta/scimConfig2.png"/>
</p>

**Note**: We are not pushing passwords to DataHub over SCIM, since we are assuming SSO with OIDC as mentioned earlier.

### 3. Add a custom attribute to represent roles
a). Navigate to `Directory` -> `Profile Editor`, and select the user-profile of this new application.

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/okta/profileEditor.png"/>
</p>

b). Click `Add Attribute` and define a new attribute that will be used to specify the role of a DataHub user.

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/okta/defineRoleAttribute.png"/>
</p>

* Set value of `External name` to `roles.^[primary==true].value`
* Set value of `External namespace` to `urn:ietf:params:scim:schemas:core:2.0:User`
* Define an enumerated list of values as shown in the above image
* Mark this attribute as required
* Select `Attribute type` as `Personal`

c). Add a similar attribute for groups i.e. repeat step (b) above, but select `Attribute Type` as `Group`. (Specify the variable name as, say, `dataHubGroupRoles`.)

### 4. Assign users & groups to the app
Assign users and groups to the app from the `Assignments` tab:

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/okta/assignUsersGroups.png"/>
</p>

While assigning a user/group, choose an appropriate value for the dataHubRoles/dataHubGroupRoles attribute.
Note that when a role is selected for a group, the corresponding role is pushed for all users of that group in DataHub.

### The provisioning setup is now complete
Once the above steps are completed, user assignments/unassignments to the DataHub-SCIM-SWA app in Okta will get reflected in DataHub automatically.

> #### A note on user deletion
>Note that when users are unassigned or deactivated in Okta, the corresponding users in DataHub are also deactivated (marked "suspended").
But when a user is *deleted* in Okta, the corresponding user in DataHub does *not* get deleted.
Refer the Okta documentation on [Delete (Deprovision)](https://developer.okta.com/docs/concepts/scim/#delete-deprovision) for more details.

### 5. (Optional): Configure push groups
When groups are assigned to the app, Okta pushes the group-members as users to DataHub, but the group itself isn't pushed.
To push group information to DataHub, configure the `Push Groups` tab accordingly as shown below:

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/okta/pushGroups.png"/>
</p>

Refer to the Okta [Group Push](https://help.okta.com/en-us/content/topics/users-groups-profiles/app-assignments-group-push.htm) documentation for more details.