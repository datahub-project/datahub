---
description: "Embed externally hosted micro frontends in the DataHub UI: as pages in the left navigation or as tabs on entity profile pages, declared with a YAML config and a typed mount contract."
---

# Micro Frontends

DataHub can host **micro frontends (MFEs)**: independently built and deployed web applications that render inside the
DataHub UI as if they were part of it. An MFE is declared in a YAML file, loaded at runtime with
[Module Federation](https://webpack.js.org/concepts/module-federation/), and mounted into a region of the page that
DataHub owns. No DataHub rebuild is needed to add, change, or remove one.

Use MFEs to bring organisation-specific workflows into the catalog without forking the UI: for example an access
request flow that talks to your own entitlement services, an internal data-quality dashboard, or a home-grown asset
inventory.

Two kinds of placement are supported:

| Placement           | Where the MFE renders                                                      | Context the MFE receives                                  |
| ------------------- | -------------------------------------------------------------------------- | --------------------------------------------------------- |
| `nav.page`          | A full page at `/mfe<path>`, linked from the left navigation. The default. | Base context only                                         |
| `entity.detail.tab` | A tab on the profile page of every entity (optionally filtered by type).   | Base context + `{ urn, type }` of the entity being viewed |

## How it works

```
                 GET /mfe/config (YAML)                 mount(el, ctx)
 datahub-frontend ────────────────────► DataHub UI (host) ────────────────► your MFE (remoteEntry.js)
   reads MFE_CONFIG_FILE_PATH            builds nav items, routes            renders into `el`
                                         and entity tabs from the YAML;      using the typed `ctx`
                                         loads the remote lazily
```

1. `datahub-frontend` serves the YAML at `MFE_CONFIG_FILE_PATH` verbatim from `GET /mfe/config`.
2. The UI fetches it once per session and validates every entry. Invalid entries are logged to the browser console and
   skipped; the rest of the UI is unaffected.
3. Navigation items, routes, and entity tabs are derived from the YAML **before** any remote code is loaded.
4. When a user opens the page or tab, the host loads `remoteEntry.js` and calls its `mount(el, ctx)` with a DOM
   element and a typed context for that placement. Failures (unreachable remote, no response within 5 seconds, thrown
   error) are contained: the region shows "_label_ is not available at this time" and the rest of the page keeps
   working.

## Building an MFE

An MFE is a webpack Module Federation remote that exposes a `mount` module. Exporting a React component is not
enough; the host is framework-agnostic and only calls `mount`.

```js
// webpack.config.js
new ModuleFederationPlugin({
  name: "accessMFE", // becomes window.accessMFE — must match the first half of `module` in the YAML
  filename: "remoteEntry.js",
  exposes: { "./mount": "./src/mount.tsx" },
});
```

```tsx
// src/mount.tsx
import { createRoot } from "react-dom/client";
import type { EntityDetailTabContext } from "./slotTypes"; // copied from DataHub, see "The typed contract" below

export function mount(
  el: HTMLElement,
  ctx: EntityDetailTabContext,
): () => void {
  const root = createRoot(el);
  root.render(
    <AccessPanel
      urn={ctx.entity.urn}
      entityType={ctx.entity.type}
      viewer={ctx.principal?.user}
    />,
  );
  return () => root.unmount();
}
```

The host currently loads remotes in webpack's `var` format (`window.<name>` global). Serve `remoteEntry.js` from an
origin your DataHub deployment's Content Security Policy allows, and enable CORS if it is on a different host.

## Declaring MFEs

```yaml
topLevelMenuTitle: Apps # heading of the navigation group (default: "MFE Apps")
subNavigationMode: false # true: one collapsible menu; false: items spread in the sidebar
microFrontends:
  # A full page in the left navigation (placement omitted => nav.page)
  - id: asset-inventory
    label: Asset Inventory
    path: /asset-inventory # route under /mfe
    remoteEntry: https://apps.example.com/inventory/remoteEntry.js
    module: inventoryMFE/mount # <remote name>/<exposed module>
    flags:
      enabled: true
      showInNav: true
    navIcon: Package # any Phosphor icon name

  # A tab on dataset profile pages
  - id: access-tab
    label: Access
    remoteEntry: https://apps.example.com/access/remoteEntry.js
    module: accessMFE/mount
    flags:
      enabled: true
      showInNav: false
    placement:
      slot: entity.detail.tab
      entityTypes: [dataset] # optional coarse filter; omit to appear on every entity type
      tabName: "Access" # optional; defaults to label
```

| Field                   | Required                | Notes                                                                                 |
| ----------------------- | ----------------------- | ------------------------------------------------------------------------------------- |
| `id`                    | yes                     | Unique identifier for the entry.                                                      |
| `label`                 | yes                     | Navigation title; default tab name; used in error messages.                           |
| `remoteEntry`           | yes                     | URL of the Module Federation `remoteEntry.js`.                                        |
| `module`                | yes                     | `<remoteName>/<exposedModule>`, e.g. `accessMFE/mount`.                               |
| `flags.enabled`         | yes                     | `false` hides the MFE everywhere; the remote is never fetched.                        |
| `flags.showInNav`       | yes                     | Only meaningful for `nav.page`.                                                       |
| `path`                  | `nav.page` only         | Route under `/mfe`, must start with `/`.                                              |
| `navIcon`               | `nav.page` only         | Phosphor icon name. Optional icon for tabs.                                           |
| `placement.slot`        | no (default `nav.page`) | `nav.page` or `entity.detail.tab`.                                                    |
| `placement.entityTypes` | no                      | GraphQL entity type names (`dataset`, `chart`, `dashboard`, …), case-insensitive.     |
| `placement.tabName`     | no                      | Tab label for `entity.detail.tab`. Shown as written; not translated.                  |
| `topLevelMenuTitle`     | no (top level)          | Heading of the navigation group. Default: "MFE Apps".                                 |
| `subNavigationMode`     | no (top level)          | `true` collapses navigation entries into one menu; `false` lists them in the sidebar. |

Entries that fail validation are skipped and reported in the browser console; the remaining entries still load.
Keys that are not listed above are ignored. There is no feature flag: micro frontends are active whenever the file
lists at least one enabled entry.

Entity tabs are routed by name (`/dataset/<urn>/<tabName>`), so pick a `tabName` that does not collide with a built-in
tab such as `Columns` or `Lineage`.

### Deploying the config

Point [`MFE_CONFIG_FILE_PATH`](./deploy/environment-vars.md#micro-frontends) on the `datahub-frontend` service at
your YAML. The file is read once at startup and browsers cache the response for five minutes, so restart the service
after changing it and allow a few minutes for users to pick it up. The Docker image ships
[`mfe.config.dev.yaml`](../datahub-frontend/conf/mfe.config.dev.yaml) (an empty list) as the default;
[`mfe.config.local.yaml`](../datahub-frontend/conf/mfe.config.local.yaml) carries an annotated example of every
field.

With the Helm chart, supply the file through the `datahub-frontend` subchart:

```yaml
datahub-frontend:
  extraEnvs:
    - name: MFE_CONFIG_FILE_PATH
      value: /mfeconfig/mfe.config.yaml
  extraVolumes:
    - name: mfe-config
      configMap:
        name: datahub-mfe-config
  extraVolumeMounts:
    - name: mfe-config
      mountPath: /mfeconfig
      readOnly: true
```

Developers iterating on a config locally can have the web client serve it directly; see the
[framework README](../datahub-web-react/src/app/mfeframework/README-MFE.md).

## The typed contract

Every `mount` call receives a context whose shape depends on the slot. The types are defined in
[`slotTypes.ts`](../datahub-web-react/src/app/mfeframework/slots/slotTypes.ts); copy or vendor that file into your MFE
so both sides compile against the same definition.

```ts
type SlotBaseContext = {
  slot: "nav.page" | "entity.detail.tab";
  version: string; // contract version, currently "1.0.0"
  principal?: { user: string }; // the viewer's urn, when known
};

type NavPageContext = SlotBaseContext & { slot: "nav.page" };

type EntityDetailTabContext = SlotBaseContext & {
  slot: "entity.detail.tab";
  entity: { urn: string; type: string }; // e.g. { urn: "urn:li:dataset:(...)", type: "DATASET" }
};
```

The base context is deliberately small and surface-agnostic. Anything else an MFE needs (schema, ownership, the
viewer's groups) it fetches itself from DataHub's [GraphQL API](./api/graphql/overview.md) using the browser
session, which is shared with the host. Per-slot context grows by extending that slot's type and bumping the
contract version, never by adding surface-specific fields to the base.

Two gates decide whether an entity tab appears, and they are meant to be used together:

- **`placement.entityTypes`** in YAML is the coarse, declarative gate: "does this tab belong on this class of page at
  all?" It is evaluated before any remote code loads.
- **Logic inside the MFE** is the fine gate: given the actual entity and viewer, render the workflow, a message, or
  nothing.

## Security and operations

- MFEs run in the same origin and session as the DataHub UI. Only load remotes you control, over HTTPS, and allow
  their origins explicitly with `DATAHUB_CSP_SCRIPT_SRC` and `DATAHUB_CSP_CONNECT_SRC` (the defaults are permissive;
  see [Environment Variables](./deploy/environment-vars.md#micro-frontends)). There is no per-MFE permission model:
  every authenticated user who can see the page or entity can open the MFE. An MFE is not sandboxed: it runs in the
  host window and can read the session, call any API the viewer can, and navigate the page.
- Remote loading times out after 5 seconds; a slow or unreachable remote degrades to an error message inside its own
  region.
- Nothing about an MFE is stored in the metadata graph. Removing an entry from the YAML removes it from the UI on
  the next restart.

## See also

- [Data Access Roles](./features/feature-guides/access-roles.md) and the `SHOW_ACCESS_MANAGEMENT` flag: DataHub's
  built-in way to show which roles grant access to an asset. Consider it before building an access MFE.
- [Environment Variables](./deploy/environment-vars.md#micro-frontends) for `MFE_CONFIG_FILE_PATH` and CSP settings.
- [Plugins Guide](./plugins.md) for backend authentication and authorization extension points.
- [`README-MFE.md`](../datahub-web-react/src/app/mfeframework/README-MFE.md) in the web client for the developer-facing
  walkthrough and local setup.
