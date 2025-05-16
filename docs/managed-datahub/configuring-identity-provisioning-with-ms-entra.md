---
title: "SCIM Integration: MS Entra and DataHub"
hide_title: true
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

## SCIM Integration: MS Entra and DataHub

<FeatureAvailability saasOnly />

## Overview

On completion of this setup the MS Entra will automatically manage the groups/users/roles from MS Entra to DataHub.

Consider following configuration in MS Entra

- A group `governance-team` group
- And it has two members `john` and `sid`
- And the group has role `Reader`

If you configure the `governance-team` for auto provisioning, MS Entra will create the `governance-team` group and its members automatically on DataHub and set the `Reader` roles on users.

If you remove `john` from the group `governance-team`, then MS Entra will automatically remove `john` from DataHub's `governance-team` group.

If you permanently delete a user or group from MS Entra, then MS Entra will automatically delete the user or group from DataHub.

> MS Entra doesn't send the user's password on user creation and hence DataHub Admin need to reset their password to be able to login into the DataHub.

> Only Admin, Editor and Reader roles are supported in DataHub. These roles are preconfigured/created on DataHub

## Configuring User/Group/Roles provisioning from MS Entra to DataHub

### 1. Generate Personal Access Token

Generate a personal access token from [DataHub](../../docs/authentication/personal-access-tokens.md#creating-personal-access-tokens).

### 2. Integrate DataHub With MS Entra

Follow the steps in [Integrate your SCIM endpoint with the Microsoft Entra provisioning service](https://learn.microsoft.com/en-gb/entra/identity/app-provisioning/use-scim-to-provision-users-and-groups#integrate-your-scim-endpoint-with-the-microsoft-entra-provisioning-service) to integrate DataHub SCIM endpoint into MS Entra.

a. Set the `Tenant URL` to `https://<hostname>/gms/openapi/scim/v2`. Replace `<hostname>` with your DataHub instance hostname.

b. Set the `Secret Token` to Personal Access Token created in Step 1.

### 3. Add Users/Groups to the App

Go to the application created in step #2 and click on `Add user/group` as shown in below image

   <p>
   <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/add-user-group.png"/>
   </p>

On the screen select

- user/group
- _optional_: if you have created roles as mentioned in the "Create app roles" section below, then you can also specify an appropriate role for the user/group

### 4. (Optional): Assigning Datahub roles to users / groups

If roles are to be assigned to users/groups in Datahub, create a new app-role and specify a mapping for it as detailed in the following steps.

a). Go to `Provisioning` section inside the App and click on `Provision Microsoft Entra ID Users` as shown in below image

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/provisioning.png"/>
</p>

b). On the Attribute Mapping page, scroll down to the bottom and click on `Add New Mapping`

c). Fill details as shown in below image

Fill listed fields

- Set `Mapping type` to `Expression`
- Set `Expression` to `SingleAppRoleAssignment([appRoleAssignments])`
- Set `Target attribute` to `roles[primary eq "True"].value`
- Set `Match objects using this attribute` to `No`
- Set `Apply this mapping` to `Always`

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/edit-mapping-form.png"/>
</p>

The Attribute Mapping page should reflect the new entry.

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/add-new-mapping.png"/>
</p>

d). **Create Role**: Go back to the app created in Step #2, go to the "Users & Groups" section and click on "application registration" to create the role

<p>
<img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/application-registration.png"/>
</p>

Create three roles having `Display Name` and `Value` as mentioned below

- Admin
- Editor
- Reader

Only these three roles are supported in DataHub.

e). While creating the App Role set `Allowed member types` to `Users/Groups`

While adding users/groups to the app (refer step #3 above), you can now select any of these three roles.
