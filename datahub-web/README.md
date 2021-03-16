DataHub Ember Client (Legacy)
==============================================================================

>**Notice**: As of March 2021, the React application will be the default frontend experience supported by the community. 
> Going forward, major feature development will occur on the React application. Eventually, support for Ember will be 
> deprecated altogether (tentatively mid-2021).
> 
> Please migrate to the React app, which resides under the `datahub-web-react` directory.

## About
This repository is for the legacy web-client and related packages for DataHub, LinkedIn's premier data search and
discovery tool. It is written as an Ember application in a mono-repository. For more information about our code
architecture choices and logic, please visit the `documentation` folder [here](documentation/MAIN.md).


## Package Organization

Our mono-repository consists of an Ember application and multiple Ember addons and libraries. For more information
about our mono-repo, you can refer to the documentation available [here](documentation/introduction/02-MONOREPO),
found in the `documentation` folder.

`packages/data-portal` - This is the host Ember application into which all of our addons will be imported.

`@datahub/entities` - This Ember addon consists of logic specific to specific entities supported by DataHub.

`@datahub/shared` - This Ember addon consists of features and functionality that is either shared by multiple entities
or may not necessarily be related to any specific entity but rather for the general functionality of the application.

`@datahub/data-models` - This Ember addon consists of our classes for our entities and behavior definitions known as
"render props" (for more information, visit the `documentation` folder [here](documentation/MAIN.md)). It also
contains the API calls that each entity can make to fetch data and information.

`@datahub/metadata-types` - This Ember addon houses the translated PDSC/PDL models that are defined at the GMA level.
These models are converted to typescript in order to be consumed by our application in a way that JavaScript can
understand.

`@datahub/utils` - This Ember addon contains general utility and helper functions that assist with the operation
with the rest of the application and have no specific dependency on any other part of the application or any models

`@dh-tools/pdsc` - This library translates the PDSC/PDL models from our GMA backend to TypeScript.

`@dh-tools/eslint-plugin` - This plugin works with our eslint-tool to promote good and clean code practices

`@dh-tools/dependencies` - This tool was created for dependency management to improve build times in the application.

`@nacho-ui/core` - This Ember addon is a remnant of our shared component library efforts internally that is now a
general purpose library within DataHub to provide some generic components to be used by the application.

`blueprints/datahub-addon` - This provides Ember with a template for creating new addons whenever needed


## Creating a New Addon

Use the following command and follow the prompts to generate a new addon. However, before doing so, please make sure
the content of the addon does not better fit in one of the other packages. Additionally, it may be helpful to read our
documentation notes [here](documentation/MAIN.md) to understand the logic of how the addons came about and why the
application is laid out the way it is.

```
ember g datahub-addon
```


## Starting the Application

### Quick Start

First, follow the instructions on the DataHub Quickstart Guide for Ember (`quickstart-ember.sh`), which will 
run an instance of the backend for DataHub and also serve the static docker image from `http://localhost:9001`. This is the best way to see the current latest
frontend/UI in action as well. However, if you want to make changes and see them live without having to rebuild a
docker image, you can, from this `datahub-web` folder run the following:

```
cd packages/data-portal
ember s --proxy=http://localhost:9001
```

If necessary, it may ask you to run `yarn install` first.

This will rely on ember's proxy functionality, which will redirect all API calls to the proxy target (which in this
case is running our API from localhost), and allow us to run a hot-reloading UI while still connecting to a live API.
Simply open up a browser and go to `localhost:4200` and see the results.

