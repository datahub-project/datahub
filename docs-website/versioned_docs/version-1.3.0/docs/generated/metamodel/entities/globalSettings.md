---
sidebar_position: 55
title: GlobalSettings
slug: /generated/metamodel/entities/globalsettings
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/globalSettings.md
---
# GlobalSettings
Global settings for an the platform
## Aspects

### globalSettingsInfo
DataHub Global platform settings. Careful - these should not be modified by the outside world!
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalSettingsInfo"
  },
  "name": "GlobalSettingsInfo",
  "namespace": "com.linkedin.settings.global",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "SsoSettings",
          "namespace": "com.linkedin.settings.global",
          "fields": [
            {
              "type": "string",
              "name": "baseUrl",
              "doc": "Auth base URL."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OidcSettings",
                  "namespace": "com.linkedin.settings.global",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "enabled",
                      "doc": "Whether OIDC SSO is enabled."
                    },
                    {
                      "type": "string",
                      "name": "clientId",
                      "doc": "Unique client id issued by the identity provider."
                    },
                    {
                      "type": "string",
                      "name": "clientSecret",
                      "doc": "Unique client secret issued by the identity provider."
                    },
                    {
                      "type": "string",
                      "name": "discoveryUri",
                      "doc": "The IdP OIDC discovery url."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "userNameClaim",
                      "default": null,
                      "doc": "ADVANCED. The attribute / claim used to derive the DataHub username. Defaults to \"preferred_username\"."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "userNameClaimRegex",
                      "default": null,
                      "doc": "ADVANCED. TThe regex used to parse the DataHub username from the user name claim. Defaults to (.*) (all)."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "scope",
                      "default": null,
                      "doc": "ADVANCED. String representing the requested scope from the IdP. Defaults to \"oidc email profile\"."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "clientAuthenticationMethod",
                      "default": null,
                      "doc": "ADVANCED. Which authentication method to use to pass credentials (clientId and clientSecret) to the token endpoint: Defaults to \"client_secret_basic\"."
                    },
                    {
                      "type": [
                        "null",
                        "boolean"
                      ],
                      "name": "jitProvisioningEnabled",
                      "default": null,
                      "doc": "ADVANCED. Whether DataHub users should be provisioned on login if they do not exist. Defaults to true."
                    },
                    {
                      "type": [
                        "null",
                        "boolean"
                      ],
                      "name": "preProvisioningRequired",
                      "default": null,
                      "doc": "ADVANCED. Whether the user should already exist in DataHub on login, failing login if they are not. Defaults to false."
                    },
                    {
                      "type": [
                        "null",
                        "boolean"
                      ],
                      "name": "extractGroupsEnabled",
                      "default": null,
                      "doc": "ADVANCED. Whether groups should be extracted from a claim in the OIDC profile. Only applies if JIT provisioning is enabled. Groups will be created if they do not exist. Defaults to true."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "groupsClaim",
                      "default": null,
                      "doc": "ADVANCED. The OIDC claim to extract groups information from. Defaults to 'groups'."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "responseType",
                      "default": null,
                      "doc": "ADVANCED. Response type."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "responseMode",
                      "default": null,
                      "doc": "ADVANCED. Response mode."
                    },
                    {
                      "type": [
                        "null",
                        "boolean"
                      ],
                      "name": "useNonce",
                      "default": null,
                      "doc": "ADVANCED. Use Nonce."
                    },
                    {
                      "type": [
                        "null",
                        "long"
                      ],
                      "name": "readTimeout",
                      "default": null,
                      "doc": "ADVANCED. Read timeout."
                    },
                    {
                      "type": [
                        "null",
                        "boolean"
                      ],
                      "name": "extractJwtAccessTokenClaims",
                      "default": null,
                      "doc": "ADVANCED. Whether to extract claims from JWT access token.  Defaults to false."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "preferredJwsAlgorithm",
                      "default": null,
                      "doc": " ADVANCED. Which jws algorithm to use. Unused."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "preferredJwsAlgorithm2",
                      "default": null,
                      "doc": " ADVANCED. Which jws algorithm to use."
                    }
                  ],
                  "doc": "Settings for OIDC SSO integration."
                }
              ],
              "name": "oidcSettings",
              "default": null,
              "doc": "Optional OIDC SSO settings."
            }
          ],
          "doc": "SSO Integrations, supported on the UI."
        }
      ],
      "name": "sso",
      "default": null,
      "doc": "SSO integrations between DataHub and identity providers"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "OAuthSettings",
          "namespace": "com.linkedin.settings.global",
          "fields": [
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "OAuthProvider",
                  "namespace": "com.linkedin.settings.global",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "enabled",
                      "doc": "Whether this OAuth provider is enabled."
                    },
                    {
                      "type": "string",
                      "name": "name",
                      "doc": "The name of this OAuth provider. This is used for display purposes only."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "jwksUri",
                      "default": null,
                      "doc": "The URI of the JSON Web Key Set (JWKS) endpoint for this OAuth provider."
                    },
                    {
                      "type": "string",
                      "name": "issuer",
                      "doc": "The expected issuer (iss) claim in the JWTs issued by this OAuth provider."
                    },
                    {
                      "type": "string",
                      "name": "audience",
                      "doc": "The expected audience (aud) claim in the JWTs issued by this OAuth provider."
                    },
                    {
                      "type": "string",
                      "name": "algorithm",
                      "default": "RS256",
                      "doc": "The JWT signing algorithm required for this provider.\nPrevents algorithm confusion attacks. Common values: RS256, RS384, RS512, PS256, ES256"
                    },
                    {
                      "type": "string",
                      "name": "userIdClaim",
                      "default": "sub",
                      "doc": "The JWT claim to use as the user identifier for this provider.\nDifferent providers use different claims (sub, email, preferred_username, etc.)"
                    }
                  ],
                  "doc": "An OAuth Provider. This provides information required to validate inbound\nrequests with OAuth 2.0 bearer tokens."
                }
              },
              "name": "providers",
              "doc": "Trusted OAuth Providers"
            }
          ],
          "doc": "Trust oauth providers to use for authentication."
        }
      ],
      "name": "oauth",
      "default": null,
      "doc": "Settings related to the oauth authentication provider"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "GlobalViewsSettings",
          "namespace": "com.linkedin.settings.global",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "dataHubView"
                ],
                "name": "viewedWith"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "defaultView",
              "default": null,
              "doc": "The default View for the instance, or organization."
            }
          ],
          "doc": "Settings for DataHub Views feature."
        }
      ],
      "name": "views",
      "default": null,
      "doc": "Settings related to the Views Feature"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "DocPropagationFeatureSettings",
          "namespace": "com.linkedin.settings.global",
          "fields": [
            {
              "type": "boolean",
              "name": "enabled"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "config",
              "default": null,
              "doc": "The configuration for the feature, in JSON format."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "configVersion",
              "default": null,
              "doc": "The version of the configuration schema that has been used to serialize\n       the config.\nIf not provided, the version is assumed to be the latest version."
            },
            {
              "type": "boolean",
              "name": "columnPropagationEnabled",
              "default": true
            }
          ]
        },
        "null"
      ],
      "name": "docPropagation",
      "default": {
        "configVersion": null,
        "config": null,
        "enabled": true,
        "columnPropagationEnabled": true
      },
      "doc": "Settings related to the documentation propagation feature"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "GlobalHomePageSettings",
          "namespace": "com.linkedin.settings.global",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "defaultTemplate",
              "doc": "The urn that will be rendered in the UI by default for all users"
            }
          ],
          "doc": "Global settings related to the home page for an instance"
        }
      ],
      "name": "homePage",
      "default": null,
      "doc": "Global settings related to the home page for an instance"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "ApplicationsSettings",
          "namespace": "com.linkedin.settings.global",
          "fields": [
            {
              "type": "boolean",
              "name": "enabled"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "config",
              "default": null,
              "doc": "The configuration for the feature, in JSON format."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "configVersion",
              "default": null,
              "doc": "The version of the configuration schema that has been used to serialize\n       the config.\nIf not provided, the version is assumed to be the latest version."
            }
          ]
        }
      ],
      "name": "applications",
      "default": null,
      "doc": "Settings related to applications. If not enabled, applications won't show up in navigation"
    }
  ],
  "doc": "DataHub Global platform settings. Careful - these should not be modified by the outside world!"
}
```
</details>

## Relationships

### Outgoing
These are the relationships stored in this entity's aspects
- viewedWith

   - DataHubView via `globalSettingsInfo.views.defaultView`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
