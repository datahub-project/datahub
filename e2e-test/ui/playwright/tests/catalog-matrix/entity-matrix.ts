/**
 * Core entity types for the catalog matrix specs.
 *
 * Every spec in tests/catalog-matrix iterates this table so one edit flow is exercised on each
 * entity type against a real GMS. Each entry is its own seeded entity (fixtures/data.json), so a
 * test can mutate it without racing other tests or other suites.
 */

import type { APIRequestContext } from '@playwright/test';
import { normalizeMcp, type Mcp } from '../../helpers/seeder-utils';
import type { DocumentationSurface } from '../../pages/entity/entity-description.page';
import { DATAHUB_GRAPHQL_PATH, gmsUrl } from '../../utils/constants';

export type CoreEntityKey = 'dataset' | 'chart' | 'dashboard' | 'dataFlow' | 'dataJob' | 'container';

export interface CoreEntity {
  key: CoreEntityKey;
  /** Entity-registry name, as used by MCPs. */
  entityType: string;
  /** URL segment of the profile page (the entity's `getPathName()` in the web app). */
  pathName: string;
  urn: string;
  /** Name rendered in the profile header. */
  name: string;
  /** Aspect the UI writes an edited description to. */
  editableAspect: string;
  seedDescription: string;
  /**
   * The feature flag that gives this profile a Summary tab, or null when it never has one.
   * Mirrors ENTITY_TYPES_WITH_NEW_SUMMARY_TAB and the dataset-specific gate in
   * useEntityHasSummaryTab in datahub-web-react.
   */
  summaryFlag: 'assetSummaryPageV1' | 'datasetSummaryPageV1' | null;
}

const FLOW_URN = 'urn:li:dataFlow:(airflow,pw_matrix_flow,PROD)';

export const CORE_ENTITIES: readonly CoreEntity[] = [
  {
    key: 'dataset',
    entityType: 'dataset',
    pathName: 'dataset',
    urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_matrix_dataset,PROD)',
    name: 'pw_matrix_dataset',
    editableAspect: 'editableDatasetProperties',
    seedDescription: 'Seeded documentation for the catalog matrix dataset',
    summaryFlag: 'datasetSummaryPageV1',
  },
  {
    key: 'chart',
    entityType: 'chart',
    pathName: 'chart',
    urn: 'urn:li:chart:(looker,pw_matrix_chart)',
    name: 'PW Matrix Chart',
    editableAspect: 'editableChartProperties',
    seedDescription: 'Seeded documentation for the catalog matrix chart',
    summaryFlag: 'assetSummaryPageV1',
  },
  {
    key: 'dashboard',
    entityType: 'dashboard',
    pathName: 'dashboard',
    urn: 'urn:li:dashboard:(looker,pw_matrix_dashboard)',
    name: 'PW Matrix Dashboard',
    editableAspect: 'editableDashboardProperties',
    seedDescription: 'Seeded documentation for the catalog matrix dashboard',
    summaryFlag: 'assetSummaryPageV1',
  },
  {
    key: 'dataFlow',
    entityType: 'dataFlow',
    pathName: 'pipelines',
    urn: FLOW_URN,
    name: 'PW Matrix Flow',
    editableAspect: 'editableDataFlowProperties',
    seedDescription: 'Seeded documentation for the catalog matrix pipeline',
    summaryFlag: null,
  },
  {
    key: 'dataJob',
    entityType: 'dataJob',
    pathName: 'tasks',
    urn: `urn:li:dataJob:(${FLOW_URN},pw_matrix_task)`,
    name: 'PW Matrix Task',
    editableAspect: 'editableDataJobProperties',
    seedDescription: 'Seeded documentation for the catalog matrix task',
    summaryFlag: null,
  },
  {
    key: 'container',
    entityType: 'container',
    pathName: 'container',
    urn: 'urn:li:container:pw_matrix_container',
    name: 'PW Matrix Container',
    editableAspect: 'editableContainerProperties',
    seedDescription: 'Seeded documentation for the catalog matrix container',
    summaryFlag: 'assetSummaryPageV1',
  },
];

/** The feature flags that decide where a profile renders and edits its documentation. */
export interface DocumentationFlags {
  assetSummaryPageV1: boolean;
  datasetSummaryPageV1: boolean;
}

/**
 * Read the flags from the running instance through the frontend proxy, so the expectation tracks
 * whatever defaults the environment under test ships with.
 */
export async function fetchDocumentationFlags(request: APIRequestContext): Promise<DocumentationFlags> {
  const response = await request.post(DATAHUB_GRAPHQL_PATH, {
    data: {
      query: `{ appConfig { featureFlags { assetSummaryPageV1 datasetSummaryPageV1 } } }`,
    },
  });
  if (!response.ok()) {
    throw new Error(`appConfig query failed: ${response.status()} ${await response.text()}`);
  }
  const body = (await response.json()) as { data?: { appConfig?: { featureFlags?: DocumentationFlags } } };
  const flags = body.data?.appConfig?.featureFlags;
  if (!flags) {
    throw new Error(`appConfig query returned no featureFlags: ${JSON.stringify(body).slice(0, 300)}`);
  }
  return flags;
}

/**
 * Where the sidebar pencil must open the editor for this entity, by the same rule the app uses
 * (useEntityHasSummaryTab). The spec asserts the pencil lands on a surface the profile actually
 * renders; routing to a tab the profile dropped silently falls back to the default tab with no
 * editor.
 */
export function resolveDocumentationSurface(entity: CoreEntity, flags: DocumentationFlags): DocumentationSurface {
  return entity.summaryFlag && flags[entity.summaryFlag] ? 'summary' : 'documentation';
}

/**
 * Write the UI-editable description directly, so a test starts from known text regardless of
 * what an earlier (possibly failed) run left behind. Uses the same ingest path as the seeder.
 */
export async function writeEditableDescription(
  gmsRequest: APIRequestContext,
  entity: CoreEntity,
  description: string,
): Promise<void> {
  const mcp: Mcp = {
    entityType: entity.entityType,
    entityUrn: entity.urn,
    changeType: 'UPSERT',
    aspectName: entity.editableAspect,
    aspect: { json: { description } },
  };
  // Ask for a synchronous write. Without `async`, GMS falls back to its ASYNC_INGEST_DEFAULT
  // setting, and on an instance that defaults to async the response returns before the aspect is
  // stored, so the test's first read could still see the previous run's state.
  const response = await gmsRequest.post(`${gmsUrl()}/aspects?action=ingestProposal`, {
    data: { proposal: normalizeMcp(mcp), async: 'false' },
  });
  if (!response.ok()) {
    throw new Error(
      `ingestProposal for ${entity.editableAspect} on ${entity.urn} failed: ${response.status()} ${await response.text()}`,
    );
  }
}
