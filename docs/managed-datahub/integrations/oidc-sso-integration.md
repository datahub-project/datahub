---
description: >-
  This page will help you set up OIDC SSO with your identity provider to log
  into Acryl Data
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';


# OIDC SSO Integration
<FeatureAvailability saasOnly />


_Note that we do not yet support LDAP or SAML authentication. Please let us know if either of these integrations would be useful for your organization._


## 1. Get Details From Your Identity Provider

If you haven't already, [👉 Complete the prerequisites](../../authentication/guides/sso/initialize-oidc.md) for OIDC authentication.

Once completed, you will have the following pieces of information:
1. _Client ID_ - A unique identifier for your application with the identity provider
2. _Client Secret_ - A shared secret to use for exchange between you and your identity provider.
3. _Discovery URL_ - A URL where the OIDC API of your identity provider can be discovered.


## 2. Enable on DataHub Cloud

:::note
In order to set up the OIDC SSO integration, you must have the `Manage Platform Settings` privilege.
:::

To enable the OIDC integration, start by navigating to **Settings > Platform > SSO.**

1. Click **OIDC**.
2. Enter the **Client ID, Client Secret, and Discovery URI** obtained in the previous steps.
3. If there are any advanced settings you would like to configure, click on the **Advanced** button. These come with defaults, so only input settings here if there is something you need changed from the default configuration.
4. Click **Connect** to enable everything.
5. You can now log out and log back in through SSO to verify it's working.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/image-(10).png"/>
</p>

