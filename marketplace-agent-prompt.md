# Agent task: plan + build a Data Product Marketplace (demo)

You are working in the DataHub monorepo (`datahub-web-react` frontend + `datahub-graphql-core` backend). I need a **Data Product Marketplace**: a discovery surface over Data Product entities, plus a Data Product detail page with an Output Ports tab. This is for a live demo in a few hours, so favor reuse and low risk over completeness.

**Before writing any code, produce an implementation PLAN** (file-by-file: what you'll add, what you'll reuse, the GraphQL queries, and the data mapping). Wait for my approval, then implement. Call out anything ambiguous instead of guessing.

## Branch

Build on the **Data Product hierarchy branch** (the one that adds `parentDataProduct` to `DataProductProperties`). Do **not** merge or depend on Victor's "make data products filterable" PR — it's unrelated to rendering this UI and has open review changes. If you're not on the hierarchy branch, stop and tell me.

## Hard constraints

- **No metadata-model (PDL) changes, no new backend aspects.** Use the existing Data Product entity model as-is.
- Prefer **no new backend resolvers**. If a query genuinely can't be expressed with existing GraphQL, surface that in the plan rather than silently adding Java.
- **No Contract tab or contract panel.** Data contracts are not scoped to Data Products in the model yet — do not render anything implying a product-level contract.
- Reuse existing components, hooks, and GraphQL fragments wherever possible. This is a new *view* composed from existing data, not new data.

## Entity model facts you can rely on

- Data Product properties live in `dataProductProperties`: `name`, `description`, `assets` (array of `DataProductAssociation`), and — on this branch — `parentDataProduct: Urn`.
- `DataProductAssociation` = `{ destinationUrn, outputPort: boolean }`. **Output ports are just assets with `outputPort == true`.**
- A Data Product belongs to exactly one Domain (`domains` aspect) and carries `ownership`, `globalTags`, `glossaryTerms`, `deprecation`, `status`.
- There is **no native lifecycle status** field (proposed/draft/active/deprecated). For the demo, derive a status pill from the `deprecation` aspect (`Deprecated` if deprecated, else `Active`); treat richer ODPS statuses as future enrichment — hardcode in seed data if you want more variety, but don't invent a model field.

## Existing code to reuse (verify paths on the branch)

- Entity class / profile: `datahub-web-react/src/app/entityV2/dataProduct/DataProductEntity.tsx`
- Output ports hook + section: `datahub-web-react/src/app/entityV2/summary/modules/outputPorts/useGetOutputPorts.ts`, `entityV2/dataProduct/OutputPortsSection.tsx`
- Assets tab + list hooks: `entityV2/dataProduct/DataProductEntitiesTab.tsx`, `entityV2/dataProduct/generateUseListDataProductAssets.ts`
- Preview card (for grid tiles): `entityV2/dataProduct/preview/Preview.tsx`
- Create/edit + domain listing: `entityV2/domain/DataProductsTab/` (`DataProductsTab.tsx`, `CreateDataProductModal.tsx`, `DataProductBuilderForm.tsx`)
- Search plumbing + filter constants: `app/searchV2/` (note `DATA_PRODUCT_FILTER_NAME` in `searchV2/utils/constants.ts`)
- Hierarchy (this branch): `parentDataProduct` on the DP; child listing hook `entityV2/summary/modules/dataProducts/useGetChildDataProducts.ts` — **note it currently filters by Domain, not by parent product**; rework or add a sibling that filters on `parentDataProduct`.
- GraphQL query docs: `datahub-web-react/src/graphql/dataProduct.graphql`, `search.graphql`

## Data sources per surface

- **Marketplace grid + facets:** `searchAcrossEntities(types: [DATA_PRODUCT], query, orFilters)` — already returns each `DataProduct` with `properties`, `domain`, `ownership`, `tags`, `glossaryTerms`, and facet aggregations (domain, tags, owners). No backend change needed to list/filter/facet products.
- **Detail page:** existing `getDataProduct(urn)` query → `properties` (name, description), `domain`, `ownership`, `tags`, `glossaryTerms`, `entities` (asset count), and `parentDataProducts` (hierarchy branch).
- **Output ports:** `DataProduct.entities` returns search results tagged with an `isOutputPort` extra property (see `ListDataProductAssetsResolver`), and `useGetOutputPorts` already wraps this. Render output ports from that; per-row detail (platform, type, column count, last updated, tags) comes from the underlying dataset's existing metadata.
- **Sub-products (hierarchy):** children = Data Products whose `parentDataProduct == <urn>` (search filter on this branch) or the `IsPartOf` incoming relationship. Parent breadcrumb from `parentDataProducts`.

## Screens to build

### 1. Marketplace (discovery)
Route: a new top-level page (e.g. `/marketplace`). Layout: left filter rail (Domain, Status, Tags facets — from search aggregations), main area with a heading, a featured **bundle** card (a parent product showing its sub-products, to demo hierarchy), and a responsive grid of product tiles. Each tile: status pill, name + box icon, domain, short description, owner avatars, tag/term chips, and a footer with `N assets · M output ports`. Reuse `Preview.tsx` styling where possible.

### 2. Data Product detail
Same profile shell as other entities (header, tab bar, meta row, About). Header adds a **Request access** CTA (visual stub only — no access workflow). Tabs: **Summary | Output Ports | Assets | Properties | Lineage** (lineage is already merged — reuse it; do **not** add Contract). 

- **Summary:** meta row (Domain, Owners, Terms, Status, Updated), About, a compact Output Ports panel (top 2–3 + "View all" → Output Ports tab), Sub-products panel (hierarchy), Lineage panel.
- **Output Ports tab:** list of `outputPort == true` assets, each with platform/type, last-updated, column count, tags, a schema peek (first few columns + "+N more"), and consume actions (Copy path, Open in <platform> — visual stubs). A toggle "Output ports (N) / All assets (M)" where All assets routes to the Assets tab.

## What to stub (demo, not production)

- "Request access", "Copy path", "Open in Snowflake" are visual only.
- If seed data is thin, add a small mock/fallback layer for product tiles and output-port schema so the demo is presentable — but keep real queries wired as the primary path so it's clear what's live.

## Deliverable

1. **First: the plan.** New files and their locations; existing files/hooks/fragments reused; the exact GraphQL queries per surface; how status is derived; how sub-products and output ports are fetched; the mock/fallback strategy; and any place you'd otherwise need a backend change (flag it, don't build it).
2. After I approve: implement, keeping diffs scoped to `datahub-web-react`.

## Acceptance

- Marketplace lists real Data Products with working Domain/Tags facets.
- Clicking a tile opens the detail page; Output Ports tab shows the product's output-port assets with schema peek.
- Hierarchy is visible (parent bundle on the marketplace, sub-products + breadcrumb on detail).
- No PDL/model changes; no Contract UI; no dependency on the searchable-membership PR.
