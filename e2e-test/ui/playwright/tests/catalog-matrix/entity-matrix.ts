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
 * The tag the matrix specs assign, seeded in fixtures/data.json. Its key (`pw_matrix_tag_key`)
 * differs from its display name on purpose: a surface that falls back to the URN tail then shows
 * the key, which no assertion on the name accepts, instead of a string that happens to equal it.
 */
export const MATRIX_TAG = { name: 'PW Matrix Tag' } as const;

/** The glossary term the matrix specs assign, seeded with the same key-differs-from-name trick. */
export const MATRIX_TERM = { name: 'PW Matrix Term' } as const;

/** The domain the matrix specs set, seeded with the same key-differs-from-name trick. */
export const MATRIX_DOMAIN = { name: 'PW Matrix Domain' } as const;

/**
 * Write one aspect of a core entity directly, so a test starts from known state regardless of
 * what an earlier (possibly failed) run left behind. Uses the same ingest path as the seeder.
 */
export async function writeAspect(
  gmsRequest: APIRequestContext,
  entity: CoreEntity,
  aspectName: string,
  aspect: Record<string, unknown>,
): Promise<void> {
  const mcp: Mcp = {
    entityType: entity.entityType,
    entityUrn: entity.urn,
    changeType: 'UPSERT',
    aspectName,
    aspect: { json: aspect },
  };
  const response = await gmsRequest.post(`${gmsUrl()}/aspects?action=ingestProposal`, {
    data: { proposal: normalizeMcp(mcp) },
  });
  if (!response.ok()) {
    throw new Error(
      `ingestProposal for ${aspectName} on ${entity.urn} failed: ${response.status()} ${await response.text()}`,
    );
  }
}

/** Reset the UI-editable description to known text. */
export async function writeEditableDescription(
  gmsRequest: APIRequestContext,
  entity: CoreEntity,
  description: string,
): Promise<void> {
  await writeAspect(gmsRequest, entity, entity.editableAspect, { description });
}

/** Clear every tag on the entity. */
export async function clearTags(gmsRequest: APIRequestContext, entity: CoreEntity): Promise<void> {
  await writeAspect(gmsRequest, entity, 'globalTags', { tags: [] });
}

/** Clear every glossary term on the entity. */
export async function clearGlossaryTerms(gmsRequest: APIRequestContext, entity: CoreEntity): Promise<void> {
  await writeAspect(gmsRequest, entity, 'glossaryTerms', {
    terms: [],
    auditStamp: { time: 0, actor: 'urn:li:corpuser:datahub' },
  });
}

/** Clear the entity's domain. */
export async function clearDomains(gmsRequest: APIRequestContext, entity: CoreEntity): Promise<void> {
  await writeAspect(gmsRequest, entity, 'domains', { domains: [] });
}

/** Reset the entity to not deprecated. */
export async function clearDeprecation(gmsRequest: APIRequestContext, entity: CoreEntity): Promise<void> {
  await writeAspect(gmsRequest, entity, 'deprecation', {
    deprecated: false,
    note: '',
    actor: 'urn:li:corpuser:datahub',
  });
}
