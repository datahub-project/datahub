### Configuration Notes

1. Handling Liquid Templates

   If a view contains a liquid template, for example:

   ```
   sql_table_name: {{ user_attributes['db'] }}.kafka_streaming.events
   ```

   where `db=ANALYTICS_PROD`, you need to specify the values of those variables in the liquid_variables configuration as shown below:

   ```yml
   liquid_variables:
     user_attributes:
       db: ANALYTICS_PROD
   ```

2. Resolving LookML Constants

   If a view contains a LookML constant, for example:

   ```
   sql_table_name: @{db}.kafka_streaming.events;
   ```

   Ingestion attempts to resolve it's value by looking at project manifest files

     ```yml
     manifest.lkml
       constant: db {
           value: "ANALYTICS_PROD"
       }
     ```

   - If the constant's value is not resolved or incorrectly resolved, you can specify `lookml_constants` configuration in ingestion recipe as shown below. The constant value in recipe takes precedence over constant values resolved from manifest.

     ```yml
     lookml_constants:
       db: ANALYTICS_PROD
     ```


**Additional Notes**

Although liquid variables and LookML constants can be used anywhere in LookML code, their values are currently resolved only for LookML views by DataHub LookML ingestion. This behavior is sufficient since LookML ingestion processes only views and their upstream dependencies.

### Multi-Project LookML (Advanced)

Looker projects support organization as multiple git repos, with [remote includes that can refer to projects that are stored in a different repo](https://cloud.google.com/looker/docs/importing-projects#include_files_from_an_imported_project). If your Looker implementation uses multi-project setup, you can configure the LookML source to pull in metadata from your remote projects as well.

If you are using local or remote dependencies, you will see include directives in your lookml files that look like this:

```
include: "//e_flights/views/users.view.lkml"
include: "//e_commerce/public/orders.view.lkml"
```

Also, you will see projects that are being referred to listed in your `manifest.lkml` file. Something like this:

```
project_name: this_project

local_dependency: {
    project: "my-remote-project"
}

remote_dependency: ga_360_block {
  url: "https://github.com/llooker/google_ga360"
  ref: "0bbbef5d8080e88ade2747230b7ed62418437c21"
}
```

To ingest Looker repositories that are including files defined in other projects, you will need to use the `project_dependencies` directive within the configuration section.
Consider the following scenario:

- Your primary project refers to a remote project called `my_remote_project`
- The remote project is homed in the GitHub repo `my_org/my_remote_project`
- You have provisioned a GitHub deploy key and stored the credential in the environment variable (or UI secret), `${MY_REMOTE_PROJECT_DEPLOY_KEY}`

In this case, you can add this section to your recipe to activate multi-project LookML ingestion.

```
source:
  type: lookml
  config:
    ... other config variables

    project_dependencies:
      my_remote_project:
         repo: my_org/my_remote_project
         deploy_key: ${MY_REMOTE_PROJECT_DEPLOY_KEY}
```

Under the hood, DataHub will check out your remote repository using the provisioned deploy key, and use it to navigate includes that you have in the model files from your primary project.

If you have the remote project checked out locally, and do not need DataHub to clone the project for you, you can provide DataHub directly with the path to the project like the config snippet below:

```
source:
  type: lookml
  config:
    ... other config variables

    project_dependencies:
      my_remote_project: /path/to/local_git_clone_of_remote_project
```

:::note

This is not the same as ingesting the remote project as a primary Looker project because DataHub will not be processing the model files that might live in the remote project. If you want to additionally include the views accessible via the models in the remote project, create a second recipe where your remote project is the primary project.

:::

### Debugging LookML Parsing Errors

If you see messages like `my_file.view.lkml': "failed to load view file: Unable to find a matching expression for '<literal>' on line 5"` in the failure logs, it indicates a parsing error for the LookML file.

The first thing to check is that the Looker IDE can validate the file without issues. You can check this by clicking this "Validate LookML" button in the IDE when in development mode.

If that's not the issue, it might be because DataHub's parser, which is based on the [joshtemple/lkml](https://github.com/joshtemple/lkml) library, is slightly more strict than the official Looker parser.
Note that there's currently only one known discrepancy between the two parsers, and it's related to using [leading colons in blocks](https://github.com/joshtemple/lkml/issues/90).

To check if DataHub can parse your LookML file syntax, you can use the `lkml` CLI tool. If this raises an exception, DataHub will fail to parse the file.

```sh
pip install lkml

lkml path/to/my_file.view.lkml
```
