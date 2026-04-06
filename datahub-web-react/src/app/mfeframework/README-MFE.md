# Micro-Frontends in DataHub

DataHub now supports hosting micro-frontends (MFEs), which can be easily configured via YAML files. Each MFE must expose a `remoteEntry.js` file using [Module Federation](https://webpack.js.org/concepts/module-federation/).

> **Note:** Exporting your `<App/>` component is not sufficient.  
> You must export a `mount` function that accepts a DOM element and renders your app inside it.  
> This approach allows DataHub to support MFEs built with any framework (React, Angular, Vue, Svelte, etc.).

## Getting Started Locally

To get started, refer to the [Module Federation documentation](https://webpack.js.org/concepts/module-federation/), online tutorials (such as [this example](https://medium.com/paloit/a-beginners-guide-to-micro-frontends-with-webpack-module-federation-712f3855f813)), or use your preferred AI tool to write and expose your app's `mount()` function via a remote entry.

A variety of Module Federation examples are available [here](https://github.com/module-federation/module-federation-examples/).  
Most examples include both a "host app" and a "remote app." For DataHub, you only need to implement the "remote app," as DataHub acts as the host.

### Edit the Configuration File

Edit [`mfe.config.local.yaml`](/datahub-frontend/conf/mfe.config.local.yaml) to resemble the following:

```yaml
subNavigationMode: false
microFrontends:
    - id: HelloWorld
      label: HelloWorld DEV
      path: /helloworld-mfe
      remoteEntry: http://localhost:3002/remoteEntry.js
      module: helloWorldMFE/mount
      flags:
          enabled: true
          showInNav: true
      navIcon: HandWaving
```

To ensure compatibility between the DataHub MFE configuration above and your actual MFE, verify the following:

- The HelloWorld app is running on `localhost:3002`.
- The HelloWorld Webpack configuration includes:

```
  plugins: [
    // ...other plugins...
    new ModuleFederationPlugin({
      name: 'helloWorldMFE',
      filename: 'remoteEntry.js',
      exposes: {
        './mount': './src/whatever/sub/path/mount.tsx',
      },
      // ...other options...
    }),
    // ...other plugins...
  ]
```

### Build the `datahub-frontend` Binary

```shell
cd datahub-frontend
../gradlew build
```

### Run the Binary

```shell
cd run
./run-local-frontend
```

By default, the above script ([run-local-frontend](/datahub-frontend/run/run-local-frontend)) uses the [`frontend.env`](/datahub-frontend/run/frontend.env) file, which sets the `MFE_CONFIG_FILE_PATH` environment variable to point to your edited [`mfe.config.local.yaml`](/datahub-frontend/conf/mfe.config.local.yaml).

Additionally, this section in [`frontend.env`](/datahub-frontend/run/frontend.env):

```
PORT=9002
```

ensures the app is available at [http://localhost:9002](http://localhost:9002).

### Start Supporting Services

As described in the [DataHub Quickstart Guide](https://docs.datahub.com/docs/quickstart), you will need to start several supporting services to use the DataHub GUI.

### Test Your MFE

Navigate to [http://localhost:9002](http://localhost:9002).  
You should see a waving hand menu item in the left navigation bar.

## Deploying to Kubernetes

Suppose HelloWorld is deployed at `https://mydomain-dev.com/helloworld/remoteEntry.js`.

Edit [`mfe.config.dev.yaml`](/datahub-frontend/conf/mfe.config.dev.yaml). This file will be similar to your local configuration, but update the `remoteEntry` field:

```
remoteEntry: https://mydomain-dev.com/helloworld/remoteEntry.js
```

**Note:** The above file name and location is just an example. You may create any file and place it in a separate configuration repository, depending on your organization's practices.

In your Kubernetes YAML, ensure the environment variable `MFE_CONFIG_FILE_PATH` points to your configuration via volumes and volume mounts:

```yaml
  env:
    - name: MFE_CONFIG_FILE_PATH
      value: /mfeconfig/mfe.config.dev.yaml
  volumeMounts:
    - name: mfe-config
      mountPath: /mfeconfig
      readOnly: true

volumes:
  - name: mfe-config
    configMap:
      name: datahub-mfe-config
```

Then include the file in the ConfigMap following Kubernetes best practices.
