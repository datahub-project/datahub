/** URNs seeded by fixtures/data.json, shared across the lineage-controls specs. */

const PREFIX = 'playwright_lineage_controls';

const dataset = (platform: string, name: string) =>
  `urn:li:dataset:(urn:li:dataPlatform:${platform},${PREFIX}.${name},PROD)`;

export const schemaField = (datasetUrn: string, column: string) => `urn:li:schemaField:(${datasetUrn},${column})`;

// raw ──metric──▶ [model (dbt)] ──metric──▶ mart ──▶ chart
export const RAW_URN = dataset('snowflake', 'raw');
export const MODEL_DBT_URN = dataset('dbt', 'model');
export const MART_URN = dataset('snowflake', 'mart');
export const CHART_URN = `urn:li:chart:(looker,${PREFIX}.chart)`;

// ghost_src ──value──▶ ghost_dst, which is soft deleted
export const GHOST_SRC_URN = dataset('snowflake', 'ghost_src');
export const GHOST_DST_URN = dataset('snowflake', 'ghost_dst');

// wide_upstream ──col_01──▶ wide, which has 25 columns
export const WIDE_URN = dataset('snowflake', 'wide');
export const WIDE_UPSTREAM_URN = dataset('snowflake', 'wide_upstream');

// sf_a ──breed──▶ sf_b ──breed──▶ sf_c
export const SF_A_URN = dataset('snowflake', 'sf_a');
export const SF_B_URN = dataset('snowflake', 'sf_b');
export const SF_C_URN = dataset('snowflake', 'sf_c');

export const BASE_FEATURE_FLAGS = {
  themeV2Enabled: true,
  themeV2Default: true,
  showNavBarRedesign: true,
};
