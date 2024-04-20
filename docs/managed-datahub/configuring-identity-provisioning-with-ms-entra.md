---
title: "Configuring MS Entra with DataHub"
hide_title: true
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Entity Events API
<FeatureAvailability saasOnly />

# Configuring User/Group/Roles provisioning from MS Entra to DataHub

1. **Generate Personal Access Token**: 
    Generate a personal access token from [DataHub](../../docs/authentication/personal-access-tokens.md#creating-personal-access-tokens).

2. **Integrate DataHub With MS Entra**: Follow steps [Integrate your SCIM endpoint with the Microsoft Entra provisioning service](https://learn.microsoft.com/en-gb/entra/identity/app-provisioning/use-scim-to-provision-users-and-groups#integrate-your-scim-endpoint-with-the-microsoft-entra-provisioning-service) to integrate DataHub SCIM endpoint into MS Entra.

    a. Set the `Tenant URL` to `https://<hostname>/gms/openapi/scim/v2`. Replace `<hostname>` with your DataHub instance hostname.

    b. Set the `Secret Token` to Personal Access Token created in Step 1.

3. **Update Attribute Mapping For Role**: 

    a. Go to `Provisioning` section inside the App and click on `Provision Microsoft Entra ID Users` as shown in below image 

    <p>
    <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/provisioning.png"/>
    </p>

    b. Click on `Add Mapping`

    <p>
    <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/add-new-mapping.png"/>
    </p>

    c. Fill detail as shown in below image

    <p>
    <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/edit-mapping-form.png"/>
    </p>

    d. **Create Role**: Go to `Provisioning` section and click on `application registration.` to create the role 

    <p>
    <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/application-registration.png"/>
    </p>

    Create three roles having `Display Name` and `Value` as mentioned below 

    - Admin
    - Editor
    - Reader 

    e. While creating the App Role set `Allowed member types` to `Users/Groups`

4. **Add Users/Groups/Roles in the App**: Go to application created in step #1 and click on `Add user/group` as shown in below image 

    <p>
    <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/scim/add-user-group.png"/>
    </p>

    On the screen choose 
    - Group/User 
    - And role for the Group/User. The role should be one of the role created in Step 3 
