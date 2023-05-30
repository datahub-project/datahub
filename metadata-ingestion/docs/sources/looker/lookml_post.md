#### Configuration Notes

:::note

The integration can use an SQL parser to try to parse the tables the views depends on. 

:::

This parsing is disabled by default, but can be enabled by setting `parse_table_names_from_sql: True`.  The default parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
As this package doesn't officially support all the SQL dialects that Looker supports, the result might not be correct. You can, however, implement a custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.

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
