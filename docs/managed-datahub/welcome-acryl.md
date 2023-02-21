# Getting Started with Acryl DataHub


Welcome to the Acryl DataHub! We at Acryl are on a mission to make data reliable by bringing clarity to the who, what, when, & how of your data ecosystem. We're thrilled to be on this journey with you; and cannot wait to see what we build together!

Close communication is not only welcomed, but highly encouraged. For all questions, concerns, & feedback, please reach out to us directly at support@acryl.io. 

## Prerequisites

Before you go further, you'll need to have a DataHub instance provisioned. The Acryl integrations team will provide you the following once it has been deployed:

1. The URL for your Acryl instance (https://your-domain-name.acryl.io)
2. Admin account credentials for logging into the DataHub UI

Once you have these, you're ready to go.

:::info
If you wish to have a private connection to your DataHub instance, Acryl supports [AWS PrivateLink](https://aws.amazon.com/privatelink/) to complete this connection to your existing AWS account. Please see more details [here](integrations/aws-privatelink.md).
:::

### Logging In

Acryl DataHub currently supports the following means to log into a DataHub instance:

1. **Admin account**: With each deployment of DataHub comes a master admin account. It has a randomly generated password that can be accessed by reaching out to Acryl Integrations team (support@acryl.io). To log in with an admin account, navigate to https://your-domain.acryl.io/login
2. **OIDC**: Acryl DataHub also supports OIDC integration with the Identity Provider of your choice (Okta, Google, etc). To set this up, Acryl integrations team will require the following:
3. _Client ID_ - A unique identifier for your application with the identity provider
4. _Client Secret_ - A shared secret to use for exchange between you and your identity provider. To send this over securely, we recommend using [onetimesecret.com](https://onetimesecret.com/) to create a link.
5. _Discovery URL_ - A URL where the OIDC API of your identity provider can be discovered. This should suffixed by `.well-known/openid-configuration`. Sometimes, identity providers will not explicitly include this URL in their setup guides, though this endpoint will exist as per the OIDC specification. For more info see [here](http://openid.net/specs/openid-connect-discovery-1\_0.html).

The callback URL to register in your Identity Provider will be

```
https://your-acryl-domain.acryl.io/callback/oidc 
```

_Note that we do not yet support LDAP or SAML authentication. Please let us know if either of these integrations would be useful for your organization._

## Getting Started

Acryl DataHub is first and foremost a metadata Search & Discovery product. As such, the two most important parts of the experience are

1. Ingesting metadata
2. Discovering metadata

### Ingesting Metadata

Acryl DataHub employs a push-based metadata ingestion model. In practice, this means running an Acryl-provided agent inside your organization's infrastructure, and pushing that data out to your DataHub instance in the cloud. One benefit of this approach is that metadata can be aggregated across any number of distributed sources, regardless of form or location.

This approach comes with another benefit: security. By managing your own instance of the agent, you can keep the secrets and credentials within your walled garden. Skip uploading secrets & keys into a third-party cloud tool. 

To push metadata into DataHub, Acryl provide's an ingestion framework written in Python. Typically, push jobs are run on a schedule at an interval of your choosing. For our step-by-step guide on ingestion, click [here](docs/managed-datahub/metadata-ingestion-with-acryl/ingestion.md).

### Discovering Metadata

There are 2 primary ways to find metadata: search and browse. Both can be accessed via the DataHub home page.

By default, we provide rich search capabilities across your ingested metadata. This includes the ability to search by tags, descriptions, column names, column descriptions, and more using the global search bar found on the home page.

