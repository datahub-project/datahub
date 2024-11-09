---
description: >-
  This page will help you set up OIDC SSO with your identity provider to log
  into Acryl Data
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';


# Enable OIDC SSO
<FeatureAvailability saasOnly />

This guide will walk you through configuring OIDC Single Sign-On in DataHub Cloud.

### Step 1. Complete OIDC Prerequisites

You will need the following in order to enable OIDC SSO in DataHub Cloud:

- Gather the **Client ID**, **Client Secret**, and **Discovery URI** for your OIDC provider, as detailed in [this guide](../../authentication/guides/sso/initialize-oidc.md).
- Confirm you have the `Manage Platform Settings` privilege in DataHub.


### Step 2. Enable OIDC SSO

1. In DataHub Cloud, navigate to **Settings > Platform > SSO** and choose **OIDC**.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/saas/configure-oidc.png"/>
</p>

2. Enter the **Client ID**, **Client Secret**, and **Discovery URI** from Step 1.
3. Confirm your preferred **User Provisioning Strategy**:

    * **Just-in-Time (JIT) Provisioning** is enabled by default, automatically creating a DataHub User on login if one does not exist.
    * **Pre-Provisioning DataHub Users** will only allow login for pre-provisioned DataHub Users. _Requires configuring SSO Ingestion._

4. Optionally enable **Extract Groups** to extract group memberships in the OIDC profile by default. _Requires JIT Provisioning._
5. Click **Connect**.
6. Log out and log back in through SSO to confirm connection succeeded.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/image-(10).png"/>
</p>


:::note
We do not yet support LDAP or SAML authentication. Please let us know if either of these integrations would be useful for your organization.
:::