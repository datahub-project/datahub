import { get } from 'lodash';

import {
    FieldType,
    FieldsValues,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

// Most MicroStrategy extraction toggles default to true in the connector config,
// so reflect that in the form when the recipe does not set the field explicitly.
const getBooleanValueWithTrueDefault = (fieldPath: string) => (recipe: any) => {
    const value = get(recipe, fieldPath);
    if (value !== undefined && value !== null) {
        return value;
    }
    return true;
};

export const MICROSTRATEGY_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Library Base URL',
    helper: 'URL where the MicroStrategy Library is hosted',
    tooltip: 'MicroStrategy Library base URL, for example https://your-company.example.com/MicroStrategyLibrary.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    placeholder: 'https://your-company.example.com/MicroStrategyLibrary',
    required: true,
    rules: null,
};

// Form values are keyed by the literal field name, so a dotted name like
// 'auth.type' must be accessed with a bracket lookup rather than a lodash path.
const authTypeFieldName = 'auth.type';
const authTypePassword = 'password';
const authTypeGuest = 'guest';

export const MICROSTRATEGY_AUTH_TYPE: RecipeField = {
    name: authTypeFieldName,
    label: 'Authentication Mode',
    helper: 'Use password for authenticated tenants, guest for public demo-style access',
    tooltip:
        'Authentication mode. Use password with username/password for authenticated tenants, or guest for public demo-style access.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.auth.type',
    required: true,
    rules: null,
    options: [
        { label: 'Password', value: authTypePassword },
        { label: 'Guest', value: authTypeGuest },
    ],
};

export const MICROSTRATEGY_USERNAME: RecipeField = {
    name: 'auth.username',
    label: 'Username',
    helper: 'Required when Authentication Mode is password',
    tooltip: 'MicroStrategy username. Required when Authentication Mode is password.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.auth.username',
    placeholder: 'username',
    dynamicRequired: (values: FieldsValues) => values?.[authTypeFieldName] === authTypePassword,
    dynamicHidden: (values: FieldsValues) => values?.[authTypeFieldName] === authTypeGuest,
    rules: null,
};

export const MICROSTRATEGY_PASSWORD: RecipeField = {
    name: 'auth.password',
    label: 'Password',
    helper: 'Required when Authentication Mode is password',
    tooltip: 'MicroStrategy password. Required when Authentication Mode is password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.auth.password',
    placeholder: 'password',
    dynamicRequired: (values: FieldsValues) => values?.[authTypeFieldName] === authTypePassword,
    dynamicHidden: (values: FieldsValues) => values?.[authTypeFieldName] === authTypeGuest,
    rules: null,
};

export const MICROSTRATEGY_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    helper: 'Optional identifier for this MicroStrategy instance',
    tooltip:
        'Optional instance identifier (e.g. prod_microstrategy). Useful when ingesting from multiple MicroStrategy environments.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.platform_instance',
    placeholder: 'prod_microstrategy',
    rules: null,
};

const projectAllowFieldPath = 'source.config.project_pattern.allow';
export const MICROSTRATEGY_PROJECT_ALLOW: FilterRecipeField = {
    name: 'project_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Projects',
    tooltip:
        'Only include specific MicroStrategy Projects by providing the name of a Project, or a Regular Expression (REGEX). If not provided, all Projects will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectAllowFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    placeholder: 'MicroStrategy Tutorial',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectAllowFieldPath),
};

const projectDenyFieldPath = 'source.config.project_pattern.deny';
export const MICROSTRATEGY_PROJECT_DENY: FilterRecipeField = {
    name: 'project_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Projects',
    tooltip:
        'Exclude specific MicroStrategy Projects by providing the name of a Project, or a Regular Expression (REGEX). If not provided, all Projects will be included. Deny patterns always take precedence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectDenyFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    placeholder: 'MicroStrategy Tutorial',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectDenyFieldPath),
};

const folderAllowFieldPath = 'source.config.folder_pattern.allow';
export const MICROSTRATEGY_FOLDER_ALLOW: FilterRecipeField = {
    name: 'folder_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Folders',
    tooltip:
        'Only include specific folder containers by providing the name of a Folder, or a Regular Expression (REGEX). If not provided, all Folders will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: folderAllowFieldPath,
    rules: null,
    section: 'Folders',
    filteringResource: 'Folder',
    placeholder: 'Public Objects',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, folderAllowFieldPath),
};

const folderDenyFieldPath = 'source.config.folder_pattern.deny';
export const MICROSTRATEGY_FOLDER_DENY: FilterRecipeField = {
    name: 'folder_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Folders',
    tooltip:
        'Exclude specific folder containers by providing the name of a Folder, or a Regular Expression (REGEX). When an intermediate folder is denied, its children re-parent to the nearest allowed ancestor rather than being dropped. Deny patterns always take precedence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: folderDenyFieldPath,
    rules: null,
    section: 'Folders',
    filteringResource: 'Folder',
    placeholder: 'Public Objects',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, folderDenyFieldPath),
};

const dashboardAllowFieldPath = 'source.config.dashboard_pattern.allow';
export const MICROSTRATEGY_DASHBOARD_ALLOW: FilterRecipeField = {
    name: 'dashboard_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Dashboards',
    tooltip:
        'Only include specific dossiers/dashboards by providing the name of a Dashboard, or a Regular Expression (REGEX). If not provided, all Dashboards will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardAllowFieldPath,
    rules: null,
    section: 'Dashboards',
    filteringResource: 'Dashboard',
    placeholder: 'Sales Dashboard',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardAllowFieldPath),
};

const dashboardDenyFieldPath = 'source.config.dashboard_pattern.deny';
export const MICROSTRATEGY_DASHBOARD_DENY: FilterRecipeField = {
    name: 'dashboard_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Dashboards',
    tooltip:
        'Exclude specific dossiers/dashboards by providing the name of a Dashboard, or a Regular Expression (REGEX). If not provided, all Dashboards will be included. Deny patterns always take precedence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardDenyFieldPath,
    rules: null,
    section: 'Dashboards',
    filteringResource: 'Dashboard',
    placeholder: 'Sales Dashboard',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardDenyFieldPath),
};

const reportAllowFieldPath = 'source.config.report_pattern.allow';
export const MICROSTRATEGY_REPORT_ALLOW: FilterRecipeField = {
    name: 'report_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Reports',
    tooltip:
        'Only include specific MicroStrategy Reports by providing the name of a Report, or a Regular Expression (REGEX). If not provided, all Reports will be included. Only used when Extract Reports is enabled.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: reportAllowFieldPath,
    rules: null,
    section: 'Reports',
    filteringResource: 'Report',
    placeholder: 'Quarterly Report',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, reportAllowFieldPath),
};

const reportDenyFieldPath = 'source.config.report_pattern.deny';
export const MICROSTRATEGY_REPORT_DENY: FilterRecipeField = {
    name: 'report_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Reports',
    tooltip:
        'Exclude specific MicroStrategy Reports by providing the name of a Report, or a Regular Expression (REGEX). If not provided, all Reports will be included. Only used when Extract Reports is enabled. Deny patterns always take precedence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: reportDenyFieldPath,
    rules: null,
    section: 'Reports',
    filteringResource: 'Report',
    placeholder: 'Quarterly Report',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, reportDenyFieldPath),
};

const extractDashboardsFieldPath = 'source.config.extract_dashboards';
export const MICROSTRATEGY_EXTRACT_DASHBOARDS: RecipeField = {
    name: 'extract_dashboards',
    label: 'Extract Dashboards',
    helper: 'Extract dossiers/documents as Dashboards',
    tooltip: 'Whether to extract dossiers/documents as DataHub dashboards.',
    type: FieldType.BOOLEAN,
    fieldPath: extractDashboardsFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractDashboardsFieldPath),
};

const extractChartsFieldPath = 'source.config.extract_charts';
export const MICROSTRATEGY_EXTRACT_CHARTS: RecipeField = {
    name: 'extract_charts',
    label: 'Extract Charts',
    helper: 'Extract visualizations as Charts',
    tooltip: 'Whether to extract visualizations as DataHub charts.',
    type: FieldType.BOOLEAN,
    fieldPath: extractChartsFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractChartsFieldPath),
};

const extractCubesFieldPath = 'source.config.extract_cubes';
export const MICROSTRATEGY_EXTRACT_CUBES: RecipeField = {
    name: 'extract_cubes',
    label: 'Extract Cubes',
    helper: 'Extract dashboard datasets as Datasets',
    tooltip: 'Whether to extract embedded dashboard datasets as DataHub datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: extractCubesFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractCubesFieldPath),
};

export const MICROSTRATEGY_EXTRACT_REPORTS: RecipeField = {
    name: 'extract_reports',
    label: 'Extract Reports',
    helper: 'Extract MicroStrategy Reports as Charts',
    tooltip:
        'Whether to extract MicroStrategy reports as DataHub charts. Disabled by default because reports can be numerous and are independent from dossier visualization extraction.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_reports',
    rules: null,
};

const extractLineageFieldPath = 'source.config.extract_lineage';
export const MICROSTRATEGY_EXTRACT_LINEAGE: RecipeField = {
    name: 'extract_lineage',
    label: 'Extract Lineage',
    helper: 'Extract dataset-to-chart lineage',
    tooltip: 'Whether to emit dataset-to-chart lineage when resolved from definitions.',
    type: FieldType.BOOLEAN,
    fieldPath: extractLineageFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractLineageFieldPath),
};

const extractVisualizationDetailsFieldPath = 'source.config.extract_visualization_details';
export const MICROSTRATEGY_EXTRACT_VISUALIZATION_DETAILS: RecipeField = {
    name: 'extract_visualization_details',
    label: 'Extract Visualization Details',
    helper: 'Resolve per-visualization lineage at runtime',
    tooltip:
        'Whether to execute dashboards and fetch per-visualization runtime definitions to resolve dataset-to-visualization lineage when the static dashboard definition does not include dataset IDs.',
    type: FieldType.BOOLEAN,
    fieldPath: extractVisualizationDetailsFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractVisualizationDetailsFieldPath),
};

const extractSourceWarehousesFieldPath = 'source.config.extract_source_warehouses';
export const MICROSTRATEGY_EXTRACT_SOURCE_WAREHOUSES: RecipeField = {
    name: 'extract_source_warehouses',
    label: 'Extract Source Warehouses',
    helper: 'Discover source warehouse metadata',
    tooltip:
        'Whether to call the MicroStrategy datasource management APIs to discover project source warehouse names, source types, database versions, DBMS names, and connection metadata.',
    type: FieldType.BOOLEAN,
    fieldPath: extractSourceWarehousesFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractSourceWarehousesFieldPath),
};

const extractDashboardDependenciesFieldPath = 'source.config.extract_dashboard_dependencies';
export const MICROSTRATEGY_EXTRACT_DASHBOARD_DEPENDENCIES: RecipeField = {
    name: 'extract_dashboard_dependencies',
    label: 'Extract Dashboard Dependencies',
    helper: 'Extract dashboard component lineage',
    tooltip:
        'Whether to call metadata search lineage APIs for direct dashboard components such as metrics, attributes, filters, and functions.',
    type: FieldType.BOOLEAN,
    fieldPath: extractDashboardDependenciesFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractDashboardDependenciesFieldPath),
};

const extractMetricExpressionsFieldPath = 'source.config.extract_metric_expressions';
export const MICROSTRATEGY_EXTRACT_METRIC_EXPRESSIONS: RecipeField = {
    name: 'extract_metric_expressions',
    label: 'Extract Metric Expressions',
    helper: 'Attach metric expression metadata',
    tooltip:
        'Whether to fetch metric model definitions with expression tokens and attach expression metadata to metric schema fields when the MicroStrategy principal has access.',
    type: FieldType.BOOLEAN,
    fieldPath: extractMetricExpressionsFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractMetricExpressionsFieldPath),
};

const extractModelLineageFieldPath = 'source.config.extract_model_lineage';
export const MICROSTRATEGY_EXTRACT_MODEL_LINEAGE: RecipeField = {
    name: 'extract_model_lineage',
    label: 'Extract Model Lineage',
    helper: 'Extract logical table and warehouse lineage',
    tooltip:
        'Whether to attempt modeling/table API access needed for logical table and source warehouse lineage. If privileges are missing, the connector reports the failure and continues.',
    type: FieldType.BOOLEAN,
    fieldPath: extractModelLineageFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractModelLineageFieldPath),
};

export const MICROSTRATEGY_EXTRACT_WAREHOUSE_LINEAGE: RecipeField = {
    name: 'extract_warehouse_lineage',
    label: 'Extract Warehouse Lineage',
    helper: 'Extract table-level lineage from dashboard SQL',
    tooltip:
        'Whether to execute dashboard/dossier SQL-view APIs and emit upstream coarse table-level lineage from MicroStrategy datasets to source warehouse datasets parsed from SQL. Disabled by default because this is not field-level metric, attribute, or fact lineage.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_warehouse_lineage',
    rules: null,
};

export const MICROSTRATEGY_EXTRACT_REPORT_SQL_LINEAGE: RecipeField = {
    name: 'extract_report_sql_lineage',
    label: 'Extract Report SQL Lineage',
    helper: 'Extract table-level lineage from report SQL',
    tooltip:
        'Whether to execute report SQL-view APIs and emit coarse table-level lineage from report source datasets to source warehouse datasets. Field-level model lineage for report source datasets also requires this flag.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_report_sql_lineage',
    rules: null,
};

export const MICROSTRATEGY_EMIT_DASHBOARD_DATASET_EDGES: RecipeField = {
    name: 'emit_dashboard_dataset_edges',
    label: 'Emit Dashboard Dataset Edges',
    helper: 'Emit dashboard dataset edges as a fallback',
    tooltip:
        'Emit DashboardInfo.datasetEdges as a fallback. Disabled by default because BI dashboards with many datasets make lineage views noisy.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.emit_dashboard_dataset_edges',
    rules: null,
};

const tagMeasuresAndDimensionsFieldPath = 'source.config.tag_measures_and_dimensions';
export const MICROSTRATEGY_TAG_MEASURES_AND_DIMENSIONS: RecipeField = {
    name: 'tag_measures_and_dimensions',
    label: 'Tag Measures and Dimensions',
    helper: 'Tag fields as Measure, Dimension, or Temporal',
    tooltip: 'Tag metric fields as Measure, attribute fields as Dimension, and date/time attribute forms as Temporal.',
    type: FieldType.BOOLEAN,
    fieldPath: tagMeasuresAndDimensionsFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(tagMeasuresAndDimensionsFieldPath),
};

const ingestOwnerFieldPath = 'source.config.ingest_owner';
export const MICROSTRATEGY_INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    label: 'Extract Owners',
    helper: 'Extract owner metadata',
    tooltip: 'Whether to map API owner fields to DataHub ownership aspects.',
    type: FieldType.BOOLEAN,
    fieldPath: ingestOwnerFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(ingestOwnerFieldPath),
};

export const MICROSTRATEGY_INCLUDE_HIDDEN: RecipeField = {
    name: 'include_hidden',
    label: 'Include Hidden Objects',
    helper: 'Include hidden MicroStrategy objects',
    tooltip: 'Whether to include hidden MicroStrategy objects when APIs support it.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_hidden',
    rules: null,
};

const verifySslFieldPath = 'source.config.verify_ssl';
export const MICROSTRATEGY_VERIFY_SSL: RecipeField = {
    name: 'verify_ssl',
    label: 'Verify SSL',
    helper: 'Verify SSL certificates for API calls',
    tooltip: 'Whether to verify SSL certificates for MicroStrategy API calls.',
    type: FieldType.BOOLEAN,
    fieldPath: verifySslFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(verifySslFieldPath),
};
