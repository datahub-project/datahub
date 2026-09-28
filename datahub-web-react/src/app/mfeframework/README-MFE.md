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

| Slot                | Where it renders                                        | Extra context passed to `mount` |
| ------------------- | ------------------------------------------------------- | ------------------------------- |
| `nav.page`          | A standalone page at `/mfe<path>` (default)             | —                               |
| `entity.detail.tab` | A tab on the profile pages of the entity types it lists | `entity: { urn, type }`         |

An entry declares one placement, but that placement can cover several pages: `entityTypes` is a list, so a
single tab entry can appear on datasets, charts and dashboards at once. To put the same remote in more than
one slot, list it more than once with different `id`s — the `remoteEntry` and `module` can repeat.

```yaml
microFrontends:
    - id: access-tab
      label: Access # also the tab caption
      remoteEntry: https://mydomain-dev.com/access/remoteEntry.js
      module: accessMFE/mount
      flags:
          enabled: true
      placement:
          contractVersion: '1.0.0' # REQUIRED — which ctx shape this MFE was built against
          slot: entity.detail.tab
          entityTypes: [dataset] # REQUIRED opt-in (GraphQL EntityType names, case-insensitive)
          visibleWhen: [physicalDataset] # optional fine-grained rule (see Visibility)
```

`path` and `navIcon` are only required for `nav.page` entries, and `flags.showInNav` is nav-only. A slot
entry never gets a `/mfe` route or a navigation item. The remote bundle is loaded the first time the tab
is opened.

The left-navigation item is not the slot — `nav.page` means "a standalone page at `/mfe<path>`", and the
sidebar entry is an optional way to reach it, controlled by `flags.showInNav`. Set it to `false` and the
page stays routable at its URL with no navigation item, which is how you deep-link to a page from
elsewhere in the product.

The tab caption is the entry's `label` — the same generic, user-visible text every MFE already supplies.
There is deliberately no slot-specific caption field, so `placement` stays free of presentation concerns
and generalises to future non-tab slots. Addressing is separate from presentation: the tab's URL segment
is derived from the stable `id` (`/dataset/<urn>/mfe-access-tab`), so renaming `label` never breaks an
existing deep link.

### Visibility — two layers, both host-side

A slot tab's visibility is resolved by the host **before the MFE mounts**, so a hidden tab never costs a
module-federation fetch. An MFE cannot remove its own tab, which is why this lives in config, not in the
remote.

| Layer                     | Driven by               | Sees              | Default when omitted                  |
| ------------------------- | ----------------------- | ----------------- | ------------------------------------- |
| 1 — page class            | `placement.entityTypes` | the entity type   | **matches nothing** (explicit opt-in) |
| 2 — per-entity predicates | `placement.visibleWhen` | the loaded entity | no constraint (tab shows)             |

Layer 2 is an allow-list of **host-registered predicate names** (`slots/slotVisibility.ts`). The tab is
visible if **any** listed predicate matches; an unknown name matches nothing and logs an error, so a typo
hides the tab rather than exposing it somewhere it was meant to be excluded from.

| Predicate         | True when                                               |
| ----------------- | ------------------------------------------------------- |
| `physicalDataset` | the entity is a dataset that is **not** a logical model |

`physicalDataset` exists because "logical" is not an entity type: a logical model is an ordinary dataset
whose data platform is flagged `logical: true`, so `entityTypes: [dataset]` cannot tell the two apart.
Features that are meaningless without a real, physical asset behind the dataset opt out of logical models
with `visibleWhen: [physicalDataset]`.

### Tab addressing

A built-in entity tab is addressed in the URL by its caption, and captions are translated — the same tab is
`/Columns` in English and `/Colonnes` in French. `EntityTab.routeKey` carries a stable segment instead:
`getEntityPath` and `useRoutedTab` both resolve `routeKey ?? name`. Built-in tabs set no key and keep exactly
the URLs they have today; a slot tab sets `mfe-<id>`, so its link survives relabelling, reconfiguration and
locale.

### The typed contract

`mount(el, ctx)` receives a typed context for the slot it was placed in. Import the types from
`datahub-web-react/src/app/mfeframework/slots/slotTypes.ts` so both sides share one definition:

```ts
import type { EntityDetailTabContext } from '.../mfeframework/slots/slotTypes';

export function mount(el: HTMLElement, ctx: EntityDetailTabContext): () => void {
    // ctx.slot === 'entity.detail.tab', ctx.contractVersion === '1.0.0'
    // ctx.entity.urn, ctx.entity.type (e.g. 'DATASET'), ctx.principal?.user (the viewer's urn)
    const root = createRoot(el);
    root.render(<AccessPanel urn={ctx.entity.urn} />);
    return () => root.unmount();
}
```

Every context carries `slot`, `contractVersion` and an optional `principal: { user }`; per-slot fields live
on the per-slot type (`EntityDetailTabContext`, `NavPageContext`).

### Contract versions

`placement.contractVersion` is how an entry tells the host which context shape it expects, and it is
**required whenever `placement` is present**. It is checked twice, and both checks fail closed:

1. **At config load** — a `placement` with a missing or blank `contractVersion` is rejected and the entry
   is dropped.
2. **At render** — a `contractVersion` the host has no builder for means the tab is not shown and the
   remote is never fetched. The only signal is a `console.error`.

Entries with no `placement` at all (every pre-slot config) are treated as `nav.page` on the default
version, so existing configs keep working with no edits.

**Unknown fields must be ignored.** An MFE reads the fields it knows and ignores the rest. That rule is
what makes the context extensible: the host can add a new optional field to a shipped version without a
new version and without breaking anyone. Code that enumerates keys, snapshots the serialised context, or
validates it strictly (zod `.strict()`, ajv `additionalProperties: false`) will break on an addition, and
is the client's bug rather than a contract violation.

So versions are not bumped for additions. Bump when a change can break a reader — removing a field,
renaming it, changing its type, or changing what it means — or when an MFE needs to _require_ something
newly added rather than tolerate its absence. At that point the version stops describing a shape and
starts asserting a capability: "a host serving this version guarantees the field is there."

Because an added field may be absent on an older host, mark it optional and feature-detect
(`ctx.thing ?? fallback`), and note in its doc comment which DataHub version began sending it.

To add a version: add the new versioned type in `slotTypes.ts`, add a builder in
`slotContextBuilders.ts`, and register it under its version key. Never change the meaning or shape of a
field in a version that has shipped — an MFE built against it keeps receiving what it expects.

### Adding a new slot

Three things are defined at three different times. Keep them separate:

| Question                                | Answered by                               | When         |
| --------------------------------------- | ----------------------------------------- | ------------ |
| Does this placement exist and is it on? | YAML `placement` (`slot`, filters)        | config time  |
| What shape does the MFE receive?        | The slot's context type in `slotTypes.ts` | compile time |
| What are the values for this page?      | The host component building `ctx`         | render time  |

Steps, using `entity.detail.tab` as the worked example:

1. **Type.** Add the id to `MFESlotId`, a versioned `<Slot>ContextV1` that extends `SlotBaseContext` with
   only the fields that region needs, and an entry in `SlotContextMap` (`slots/slotTypes.ts`). Never add
   surface-specific fields to the base.
2. **Builder.** Register a context builder for the slot under each supported version in
   `slots/slotContextBuilders.ts`.
3. **YAML.** If the slot needs placement options (like `entityTypes`), add them to `MFEPlacement` and
   validate them in `validatePlacement` (`mfeConfigLoader.tsx`). Invalid entries must be dropped, not
   partially accepted.
4. **Host.** In the region that owns the slot, call `useResolveSlot('<slot>', filter)` to get the placed
   entries, build the context with `buildSlotContext`, and render `<MFEMount config ctx />`. Memoize
   `ctx`: a new object identity remounts the remote. See `slots/MFEEntityTab.tsx` and
   `slots/useMFEEntityTabs.tsx`; the wiring into the page is one line in `EntityProfile.tsx`.
5. **Lazy.** Make sure the remote is only loaded when the region is shown (antd `Tabs` does this for tabs).
6. **Tests.** Add a Playwright flow under `e2e-test/ui/playwright/tests/mfeframework/` modelled on
   `entity-tab-slot.spec.ts`: presence, context delivery, filter, disabled, remote failure, lazy load.
7. **Docs.** Add the slot to the table in this file.

Versioning: adding a new slot does not need a new contract version. Changing the shape of an existing
slot's context does — add a new version rather than editing a shipped one, and note the change in
`docs/how/updating-datahub.md`.

### Iterating on placements locally

The Vite dev server can serve a local YAML at `/mfe/config` instead of proxying to `datahub-frontend`, so
placements can be changed without restarting the Play service:

```shell
DATAHUB_DEV_MFE_CONFIG_FILE=/path/to/mfe.config.yaml yarn start
```

## Deploying

`datahub-frontend` reads the YAML named by `MFE_CONFIG_FILE_PATH` **once at startup** and serves it at
`/mfe/config`; browsers cache that response for five minutes. So restart the service after editing the
file, and allow a few minutes for clients to pick it up. An unreadable or empty file makes `/mfe/config`
return 500 and the UI runs with no micro frontends. The Docker image defaults to
[`mfe.config.dev.yaml`](/datahub-frontend/conf/mfe.config.dev.yaml), an empty list;
[`mfe.config.local.yaml`](/datahub-frontend/conf/mfe.config.local.yaml) carries an annotated example of
every field. See also the
[environment variable reference](/docs/deploy/environment-vars.md#micro-frontends).

Remote bundles are subject to the frontend Content-Security-Policy. The defaults are permissive, so
remotes load out of the box; tighten them with `DATAHUB_CSP_SCRIPT_SRC` and `DATAHUB_CSP_CONNECT_SRC`.

### Security and operations

- An MFE runs in the host window and shares its session. It can call any API the viewer can and can
  navigate the page; it is not sandboxed. Only load remotes you control, over HTTPS, and allow their
  origins explicitly in the CSP.
- There is no per-MFE permission model. Every authenticated user who can see the page or entity can open
  the MFE.
- Remote loading times out after 5 seconds. A slow, unreachable or throwing remote degrades to an error
  message inside its own region and leaves the rest of the page working.
- Nothing about an MFE is stored in the metadata graph. Removing an entry from the YAML removes it from
  the UI on the next restart.

### Kubernetes

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
