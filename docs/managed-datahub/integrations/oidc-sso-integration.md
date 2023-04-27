---
description: >-
  This page will help you set up OIDC SSO with your identity provider to log
  into Acryl Data
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';


# OIDC SSO Integration
<FeatureAvailability saasOnly />


_Note that we do not yet support LDAP or SAML authentication. Please let us know if either of these integrations would be useful for your organization._

If you'd like to do a deeper dive into OIDC configuration outside of the UI, please see our docs [here](/docs/authentication/guides/sso/configure-oidc-react.md)

### Getting Details From Your Identity Provider

To set up the OIDC integration, you will need the following pieces of information.

1. _Client ID_ - A unique identifier for your application with the identity provider
2. _Client Secret_ - A shared secret to use for exchange between you and your identity provider.
3. _Discovery URL_ - A URL where the OIDC API of your identity provider can be discovered. This should suffixed by `.well-known/openid-configuration`. Sometimes, identity providers will not explicitly include this URL in their setup guides, though this endpoint will exist as per the OIDC specification. For more info see [here](http://openid.net/specs/openid-connect-discovery-1_0.html).

The callback URL to register in your Identity Provider will be

```
https://<your-acryl-domain>.acryl.io/callback/oidc 
```

### Configuring OIDC SSO

> In order to set up the OIDC SSO integration, the user must have the `Manage Platform Settings` privilege.

#### Enabling the OIDC Integration

To enable the OIDC integration, start by navigating to **Settings > Platform > SSO.**

1. Click **OIDC**
2. Enable the Integration
3. Enter the **Client ID, Client Secret, and Discovery URI** obtained in the previous steps
4. If there are any advanced settings you would like to configure, click on the **Advanced** button. These come with defaults, so only input settings here if there is something you need changed from the default configuration.
5. Click **Update** to save your settings.

![](../imgs/saas/image-(10).png)
