# Playwright Page Object Model — Entity Pages

## Hierarchy

```
BasePage
│
└── BaseEntityPage
│     Navigation: navigateTo(type: EntityType, urn)
│     Entity header: entityName, deprecationBadge, threeDotMenu, clickMenuOption()
│     Composed (lazy getters — initialized on first access):
│         get sidebar(): EntitySidebar
│         get documentation(): DocumentationTab    ← page.documentation.open()
│         get properties(): PropertiesTab          ← page.properties.open()
│         get summary(): SummaryTab                ← page.summary.open()
│         get queries(): QueryTab                  ← page.queries.open()
│         get lineage(): LineageTab                ← page.lineage.open()
│
├── DatasetPage (extends BaseEntityPage)
│     navigateToDataset(urn)
│     get schema(): SchemaTab           ← page.schema.open(), dataset-only tab
│
├── DashboardPage (extends BaseEntityPage)
│     navigateToDashboard(urn)
│     getChartCount(): number
│
└── ChartPage (extends BaseEntityPage)
      navigateToChart(urn)
      getDashboardLinks(): Locator
```

## EntitySidebar

Component model for the right-hand sidebar panel (`#entity-profile-sidebar`).
All sections extend `SidebarSection` base class (shared `container`, `scrollIntoView()`, `expectVisible()`).

```
EntitySidebar
│   open(), openSummaryTab(), openPropertiesTab()
│
├── TagsSection extends SidebarSection   (#entity-profile-tags)
│     add(tagName), remove(tagName)
│     expectTagVisible(name), expectTagNotVisible(name)
│
├── GlossaryTermsSection extends SidebarSection   (#entity-profile-glossary-terms)
│     add(termName), remove(termName)
│     expectTermVisible(name)
│
├── OwnersSection extends SidebarSection   (#entity-profile-owners / add-owners-button)
│     add(name, type), remove(name, elementId)
│     expectOwnerVisible(name)
│
└── DomainSection extends SidebarSection   (#entity-profile-domains)
      set(domainName), remove()
      expectDomainVisible(name)
```

## Tab component models

Plain classes that receive `Page` — composed onto `BaseEntityPage`, no inheritance.
All tabs implement the `Tab` interface which enforces `open()`.

```
SchemaTab                                (data-testid="schema-tab")
    open()
    clickField(fieldName)                ← opens FieldDrawer as a side effect
    readonly drawer: FieldDrawer

FieldDrawer                              (schema field side panel)
    editDescription(text)
    clearDescription()
    expectDescription(text)
    addBusinessAttribute(attr)
    removeBusinessAttribute(attr)
    addTag(tagName), addTerm(termName)
    expectBusinessAttributeVisible(attr)
    expectTagVisible(tagName), expectTermVisible(termName)

DocumentationTab                         (Documentation-entity-tab-header)
    open()
    editDescription(text), clearDescription()
    addLink(url, label, showInPreview)
    updateLink(curUrl, curLabel, newUrl, newLabel, showInPreview)
    removeLink(url)
    expectLinkInTab(url), expectLinkInSidebar(url), expectLinkInHeader(url)

PropertiesTab                            (entity-sidebar-tabs-tab-Properties)
    open()
    addStructuredProperty(propName, value)
    removeStructuredProperty(propName)
    expectPropertyVisible(propName, value)

SummaryTab                               (Summary-entity-tab-header)
    open()
    updateDescription(text)
    addLink(url, label), updateLink(...), removeLink(url, label)
    expectLinkExists(url, label)
    readonly properties: SummaryPropertiesSection
    readonly template: TemplateSectionComponent

QueryTab                                 (Queries-entity-tab-header)
    open()
    add(sql, title, description)
    edit(index, sql, title, description)
    delete(index)

LineageTab                               (Lineage-entity-tab-header / entity-sidebar-tabs-tab-Lineage)
    open()
    clickImpactAnalysis()
    clickUpstreamDirection(), clickDownstreamDirection()
    clickColumnLineageToggle()
    // Graph nodes
    getNode(urn): Locator
    checkNodeExists(urn), checkNodeNotExists(urn)
    expandOne(urn), expandAll(urn), contract(urn)
    // Graph edges
    checkEdgeExists(urn1, urn2)
    checkEdgeBetweenColumnsExists(urn1, col1, urn2, col2)
    // Column interactions
    expandContractColumns(urn)
    hoverColumn(urn, col), unhoverColumn(urn, col), selectColumn(urn, col)
    selectColumnFromDropdown(col)
    // Filter nodes
    getFilterNode(urn, direction): Locator
    filterNodes(urn, direction, query), clearFilter(urn, direction)
    showMore(urn, direction), showAll(urn, direction), showLess(urn, direction)
    // Impact analysis filters
    clickDegree2Filter(), clickDegree3PlusFilter()
    clickAdvancedFilter(), clickAddFilter(), clickFilterByDescription()
    typeFilterText(text), confirmFilterText()
    // Edit lineage modal
    clickLineageEditMenuButton()
    clickEditUpstreamLineage(), clickEditDownstreamLineage()
    searchInLineageEditModal(text)
    // Column path modal
    clickResultTextAndOpenModal()
    verifyColumnPathModal(from, to), closeColumnPathModal()
    // CSV download
    downloadCsvAndRead(filename): Promise<string>
```

## Rules

- Prefer adding fixtures when possible (`fixtures/data.json`)
- Page objects should be readable, well-structured, generic, and extensible
- Prefer `data-testid` selectors — add them to `datahub-web-react` if missing
- Avoid hardcoded `wait` / `waitForTimeout` calls — use Playwright's built-in auto-waiting and `expect` assertions
- Tests must be independent: no shared state, no shared data, runnable in parallel
- Run migrated tests with a single worker (`--workers=1`)
- Avoid reusing common fixture data across tests — create test-specific entries in `data.json`
- Name `data.json` entries specifically to the test, not with wide generic names (e.g. `tagAssignUnassign` not `tag`)
- Use `random.ts` for any random prefixes or run IDs

## Design decisions

- `EntitySidebar` and tab objects are plain classes that receive `Page` — no inheritance, composition only
- `DatasetPage.addOwner()` / `removeOwner()` moves to `EntitySidebar.OwnersSection` — ownership is not dataset-specific
- `entity-documentation.page.ts` is deleted — all methods move into `DocumentationTab` which is composed on `BaseEntityPage`; tests that imported `EntityDocumentationPage` use `page.documentation` instead
- `lineage-v2.page.ts` is deleted — all interactions move into `LineageTab`; navigation helpers (`goToLineageGraph`, `goToDataset`) are absorbed by `BaseEntityPage.navigateTo()` and `DatasetPage.navigateToDataset()`
- `SchemaTab` is composed onto `DatasetPage` only — Dashboard and Chart entities do not have a schema tab
- All field-level interactions (`clickField`, `addBusinessAttributeToField`, etc.) live on `SchemaTab`, not `DatasetPage` — `DatasetPage` only owns navigation and exposes `schema: SchemaTab`
- Schema field drawer interactions are extracted into `FieldDrawer` — complex enough to be its own component, and `SchemaTab.clickField()` opens it as a side effect
- All tabs implement a `Tab` interface enforcing `open(): Promise<void>` — TypeScript guarantees consistency across tab objects
- Tab objects on `BaseEntityPage` use lazy getters (`??=`) — avoid constructing unused tab objects on every page instantiation
- `BaseEntityPage.navigateTo(type: EntityType, urn)` centralises URL construction — subclasses delegate to it rather than duplicating the pattern; `EntityType` enum lives in `utils/constants.ts` alongside the existing `ENTITY_TYPES`
- All `data-testid` strings are centralised in `selectors/` files per domain (`entity-page.selectors.ts`, `sidebar.selectors.ts`, `schema-tab.selectors.ts`) using `as const` — renames require a single-file change, and factory functions (`tab(name)`, `tag(name)`) keep dynamic selectors co-located with static ones

## Folder structure

```
e2e-test/ui/playwright/
│
├── pages/
│   ├── ARCHITECTURE.md
│   ├── base.page.ts                           (unchanged)
│   │
│   ├── entity/                                ✨ new
│   │   ├── base-entity.page.ts                ✨ new
│   │   ├── dataset.page.ts                    ✏️  moved + refactored
│   │   ├── dashboard.page.ts                  ✨ new
│   │   ├── chart.page.ts                      ✨ new
│   │   │
│   │   ├── tabs/                              ✨ new
│   │   │   ├── tab.interface.ts               ✨ new
│   │   │   ├── schema.tab.ts                  ✨ new
│   │   │   ├── field-drawer.ts                ✨ new
│   │   │   ├── documentation.tab.ts           ✨ new
│   │   │   ├── lineage.tab.ts                 ✨ new
│   │   │   ├── properties.tab.ts              ✨ new
│   │   │   ├── summary.tab.ts                 ✨ new
│   │   │   └── queries.tab.ts                 ✨ new
│   │   │
│   │   └── sidebar/                           ✨ new
│   │       ├── entity-sidebar.ts              ✨ new
│   │       └── sections/                      ✨ new
│   │           ├── sidebar-section.ts         ✨ new  (base class)
│   │           ├── tags.section.ts            ✨ new
│   │           ├── glossary-terms.section.ts  ✨ new
│   │           ├── owners.section.ts          ✨ new
│   │           └── domain.section.ts          ✨ new
│   │
│   ├── common/                                (unchanged)
│   │   ├── searchbar-component.ts
│   │   └── sidebar-component.ts
│   │
│   ├── dataset-health.page.ts                 ❌ deleted → BaseEntityPage header methods
│   ├── entity-documentation.page.ts           ❌ deleted → tabs/documentation.tab.ts
│   ├── lineage-v2.page.ts                     ❌ deleted → tabs/lineage.tab.ts
│   │
│   ├── incidents.page.ts                      (unchanged)
│   ├── domains.page.ts                        (unchanged)
│   ├── business-attribute.page.ts             (unchanged)
│   ├── ingestion.page.ts                      (unchanged)
│   ├── login.page.ts                          (unchanged)
│   ├── policies.page.ts                       (unchanged)
│   ├── search.page.ts                         (unchanged)
│   └── welcome-modal.page.ts                  (unchanged)
│
├── selectors/                                 ✨ new
│   ├── entity-page.selectors.ts
│   ├── sidebar.selectors.ts
│   └── schema-tab.selectors.ts
│
├── tests/
│   ├── entity-pages/                          ✨ new (13 pending migrations)
│   │   ├── fixtures/data.json
│   │   ├── tags.spec.ts
│   │   ├── ownership.spec.ts
│   │   ├── summary-tab.spec.ts
│   │   ├── schema-blame.spec.ts
│   │   ├── query-tab.spec.ts
│   │   ├── structured-properties.spec.ts
│   │   └── documents.spec.ts
│   │
│   ├── lineage-v2/                            (unchanged)
│   ├── mutations/                             (unchanged)
│   ├── mutations-v2/                          (unchanged)
│   ├── business-attributes/                   (unchanged)
│   ├── incidents-v2/                          (unchanged)
│   ├── search/                                (unchanged)
│   ├── onboarding/                            (unchanged)
│   └── login-v2/                              (unchanged)
│
├── utils/                                     (unchanged)
├── fixtures/                                  (unchanged)
├── helpers/                                   (unchanged)
└── factories/                                 (unchanged)
```

## Files to migrate / update this session

### New Playwright specs (migrate from Cypress)

| Cypress source | Target Playwright spec |
|---|---|
| `ownershipV2/v2_manage_ownership.js` | `tests/entity-pages/ownership.spec.ts` |
| `summaryTab/aboutSection.js` | `tests/entity-pages/summary-tab.spec.ts` |
| `summaryTab/dataProductSummary.js` | `tests/entity-pages/summary-tab.spec.ts` |
| `summaryTab/domainSummary.js` | `tests/entity-pages/summary-tab.spec.ts` |
| `summaryTab/glossaryNodeSummary.js` | `tests/entity-pages/summary-tab.spec.ts` |
| `summaryTab/glossaryTermSummary.js` | `tests/entity-pages/summary-tab.spec.ts` |
| `schema_blame/schema_blame.js` | `tests/entity-pages/schema-blame.spec.ts` |
| `schema_blameV2/v2_schema_blame.js` | `tests/entity-pages/schema-blame.spec.ts` |
| `query/query_tab.js` | `tests/entity-pages/query-tab.spec.ts` |
| `structured_properties/structured_properties.js` | `tests/entity-pages/structured-properties.spec.ts` |
| `documents/document_management.js` | `tests/entity-pages/documents.spec.ts` |

### Existing Playwright specs to update (import path changes)

These tests import page objects that are being deleted or moved. Update imports to the new paths.

| Spec file | Change required |
|---|---|
| `tests/mutations/dataset-health.spec.ts` | `DatasetHealthPage` → `DatasetPage` (health methods on `BaseEntityPage`) |
| `tests/mutations/dataset-ownership.spec.ts` | `DatasetPage` → `entity/dataset.page.ts` |
| `tests/mutations/edit-documentation.spec.ts` | `EntityDocumentationPage` → `page.documentation` on `DatasetPage` |
| `tests/mutations-v2/v2-edit-documentation.spec.ts` | `EntityDocumentationPage` → `page.documentation` on `DatasetPage` |
| `tests/lineage-v2/v2-lineage-graph.spec.ts` | `LineageV2Page` → `entity/dataset.page.ts` + `page.lineage` |
| `tests/lineage-v2/v2-lineage-column-level.spec.ts` | `LineageV2Page` → `entity/dataset.page.ts` + `page.lineage` |
| `tests/lineage-v2/v2-lineage-column-path.spec.ts` | `LineageV2Page` → `entity/dataset.page.ts` + `page.lineage` |
| `tests/lineage-v2/v2-impact-analysis.spec.ts` | `LineageV2Page` → `entity/dataset.page.ts` + `page.lineage` |
| `tests/lineage-v2/v2-download-lineage-results.spec.ts` | `LineageV2Page` → `entity/dataset.page.ts` + `page.lineage` |
| `tests/business-attributes/business-attribute.spec.ts` | `DatasetPage` → `entity/dataset.page.ts` |
