# Micro-Frontends in DataHub

> User-facing documentation, including the YAML reference and the typed slot contract, lives at
> [docs/micro-frontends.md](/docs/micro-frontends.md). This file covers local development of the framework.

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
topLevelMenuTitle: My Apps
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

## Placements (slots)

By default an MFE is a **full page** reached from the left navigation (`/mfe<path>`). An entry can
instead be placed into a named, host-owned region of an existing page — a _slot_ — with the optional
`placement` field. The host owns the slot and everything around it; the MFE owns only what renders inside.

| Slot                | Where it renders                    | Extra context passed to `mount` |
| ------------------- | ----------------------------------- | ------------------------------- |
| `nav.page`          | Full page at `/mfe<path>` (default) | —                               |
| `entity.detail.tab` | A tab on every entity profile page  | `entity: { urn, type }`         |

```yaml
microFrontends:
    - id: access-tab
      label: Access
      remoteEntry: https://mydomain-dev.com/access/remoteEntry.js
      module: accessMFE/mount
      flags:
          enabled: true
          showInNav: false
      placement:
          slot: entity.detail.tab
          entityTypes: [dataset] # optional coarse filter (GraphQL EntityType names, case-insensitive)
          tabName: 'Access' # optional tab label; defaults to label
```

`path` and `navIcon` are only required for `nav.page` entries. A slot entry never gets a `/mfe` route or a
navigation item. The tab appears only when the entry is `enabled` and (if `entityTypes` is set) the page's
entity type matches; the remote bundle is loaded the first time the tab is opened.

### The typed contract

`mount(el, ctx)` receives a typed context for the slot it was placed in. Import the types from
`datahub-web-react/src/app/mfeframework/slots/slotTypes.ts` so both sides share one definition:

```ts
import type { EntityDetailTabContext } from '.../mfeframework/slots/slotTypes';

export function mount(el: HTMLElement, ctx: EntityDetailTabContext): () => void {
    // ctx.slot === 'entity.detail.tab', ctx.version === '1.0.0'
    // ctx.entity.urn, ctx.entity.type (e.g. 'DATASET'), ctx.principal?.user (the viewer's urn)
    const root = createRoot(el);
    root.render(<AccessPanel urn={ctx.entity.urn} />);
    return () => root.unmount();
}
```

Every context carries `slot`, `version` and an optional `principal: { user }`; per-slot fields live on the
per-slot type (`EntityDetailTabContext`, `NavPageContext`). To pass more to a slot later, extend that slot's
type and bump `SLOT_CONTRACT_VERSION` — never add surface-specific fields to `SlotBaseContext`.

### Slots are MFE placements

A slot is just a named region the MFE loader knows how to attach to. The set of slots is fixed in code
(`slots/slotTypes.ts`), the YAML is the only registry, and `MFEMount` is the only thing that renders into one.
Built-in tabs and pages do not use slots, and nothing but a Module Federation remote can fill one. Do not
describe slots as a general UI extension point.

### Adding a new slot

Three things are defined at three different times. Keep them separate:

| Question                                | Answered by                               | When         |
| --------------------------------------- | ----------------------------------------- | ------------ |
| Does this placement exist and is it on? | YAML `placement` (`slot`, filters)        | config time  |
| What shape does the MFE receive?        | The slot's context type in `slotTypes.ts` | compile time |
| What are the values for this page?      | The host component building `ctx`         | render time  |

Steps, using `entity.detail.tab` as the worked example:

1. **Type.** Add the id to `MFESlotId`, a `<Slot>Context` that extends `SlotBaseContext` with only the fields
   that region needs, and an entry in `SlotContextMap` (`slots/slotTypes.ts`). Never add surface-specific
   fields to the base.
2. **YAML.** If the slot needs placement options (like `entityTypes` or `tabName`), add them to `MFEPlacement`
   and validate them in `validatePlacement` (`mfeConfigLoader.tsx`). Invalid entries must be dropped, not
   partially accepted.
3. **Host.** In the region that owns the slot, call `useResolveSlot('<slot>', filter)` to get the placed
   entries, build the context from data the page already has, and render `<MFEMount config ctx />`. Memoize
   `ctx`: a new object identity remounts the remote. See `slots/MFEEntityTab.tsx` and
   `slots/useMFEEntityTabs.tsx`; the wiring into the page is one line in `EntityProfile.tsx`.
4. **Lazy.** Make sure the remote is only loaded when the region is shown (antd `Tabs` does this for tabs).
5. **Tests.** Add a Playwright flow under `e2e-test/ui/playwright/tests/mfeframework/` modelled on
   `entity-tab-slot.spec.ts`: presence, context delivery, filter, disabled, remote failure, lazy load.
6. **Docs.** Add the slot to the table in `docs/micro-frontends.md`.

Versioning: `SLOT_CONTRACT_VERSION` is shared by all slots. Adding a new slot does not bump it. Changing the
shape of an existing slot's context does; add fields as optional where possible so older MFEs keep working,
and note the change in `docs/how/updating-datahub.md`.

### Iterating on placements locally

The Vite dev server can serve a local YAML at `/mfe/config` instead of proxying to `datahub-frontend`, so
placements can be changed without restarting the Play service:

```shell
DATAHUB_DEV_MFE_CONFIG_FILE=/path/to/mfe.config.yaml yarn start
```

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
