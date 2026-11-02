### Overview

The `boomi` module ingests Boomi Integration processes and their folders through the Boomi Platform API. Each process becomes a DataFlow with a single `process` DataJob. The DataJob carries lineage to the tables and objects its connector operations read and write, and to the processes it calls. Recent executions become run history on that DataJob. By default only processes with an active deployment are ingested.

### Prerequisites

#### Create an API token

The Platform API uses basic authentication with an API token. The source plugin sends the user name as `BOOMI_TOKEN.<username>` and the token as the password.

1. Sign in to Boomi as the user the source plugin will run as. The user needs the **API Access** privilege.
2. Open **Settings** > **My User Settings** and select the **Platform API Tokens** tab.
3. Click **Add New Token**, enter a name such as `datahub`, and click **Generate Token**.
4. Copy the token. Boomi does not show the full value again after you close the dialog.
5. Note your account ID, which looks like `mycompany-AB1CD2`. It is also part of the Platform API URL, `https://api.boomi.com/api/rest/v1/<account ID>`.

#### Minimum privileges

Boomi has no built-in read-only API role. Create a custom role with the privileges below and assign it to the user that owns the token, or use a token for a user with the Administrator role.

| Privilege                      | Needed for                                                                                                                           |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| API Access                     | All Platform API calls.                                                                                                              |
| Build Read Access              | Listing processes (`ComponentMetadata`), reading process, connection, operation and profile components, and listing folders.         |
| Runtime Management Read Access | Environment names.                                                                                                                   |
| Packaged Component Deployment  | Reading active deployments (`DeployedPackage`). Without it, the source plugin ingests every process and skips deployment properties. |
| View Results                   | Reading execution history (`ExecutionRecord`). Without it, the source plugin skips run history and logs a warning.                   |

Folder restrictions on the role also hide folders and the processes in them from the source plugin.

#### Choose the API host

| Region         | `api_url`                           |
| -------------- | ----------------------------------- |
| Default (US)   | `https://api.boomi.com`             |
| United Kingdom | `https://api.platform.gb.boomi.com` |

The "View in Boomi" link is only set for these two hosts.
