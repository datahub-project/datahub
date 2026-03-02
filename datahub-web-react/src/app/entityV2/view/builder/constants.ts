import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

import { EntityType } from '@types';

export const URN_FILTER_NAME = 'urn';

/**
 * Single source of truth for entity types available in the View builder.
 * Used by both the "Build Filters" tab (type dropdown) and the "Select Assets" tab.
 * To add/remove a type from Views, update this list only.
 */
export const VIEW_ENTITY_TYPES: { type: EntityType; id: string; displayName: string }[] = [
    { type: EntityType.Dataset, id: 'dataset', displayName: 'Dataset' },
    { type: EntityType.Dashboard, id: 'dashboard', displayName: 'Dashboard' },
    { type: EntityType.Chart, id: 'chart', displayName: 'Chart' },
    { type: EntityType.DataFlow, id: 'dataFlow', displayName: 'Data Flow (Pipeline)' },
    { type: EntityType.DataJob, id: 'dataJob', displayName: 'Data Job (Task)' },
    { type: EntityType.Container, id: 'container', displayName: 'Container' },
    { type: EntityType.Domain, id: 'domain', displayName: 'Domain' },
    { type: EntityType.DataProduct, id: 'dataProduct', displayName: 'Data Product' },
    { type: EntityType.GlossaryTerm, id: 'glossaryTerm', displayName: 'Glossary Term' },
    { type: EntityType.GlossaryNode, id: 'glossaryNode', displayName: 'Term Group' },
    { type: EntityType.Tag, id: 'tag', displayName: 'Tag' },
    { type: EntityType.CorpUser, id: 'corpuser', displayName: 'User' },
    { type: EntityType.CorpGroup, id: 'corpGroup', displayName: 'Group' },
    { type: EntityType.DataPlatform, id: 'dataPlatform', displayName: 'Platform' },
    { type: EntityType.Mlmodel, id: 'mlModel', displayName: 'ML Model' },
    { type: EntityType.MlmodelGroup, id: 'mlModelGroup', displayName: 'ML Model Group' },
    { type: EntityType.Mlfeature, id: 'mlFeature', displayName: 'ML Feature' },
    { type: EntityType.MlfeatureTable, id: 'mlFeatureTable', displayName: 'ML Feature Table' },
    { type: EntityType.MlprimaryKey, id: 'mlPrimaryKey', displayName: 'ML Primary Key' },
    { type: EntityType.Notebook, id: 'notebook', displayName: 'Notebook' },
    { type: EntityType.Document, id: 'document', displayName: 'Document' },
    { type: EntityType.Application, id: 'application', displayName: 'Application' },
];

/** Derived from VIEW_ENTITY_TYPES for the Select Assets tab search query. */
export const SELECTABLE_ASSET_ENTITY_TYPES = VIEW_ENTITY_TYPES.map((e) => e.type);

/**
 * Tab keys for the view definition builder.
 * Build Filters (dynamic) is the default tab.
 */
export const BUILD_FILTERS_TAB_KEY = 'buildFilters';
export const SELECT_ASSETS_TAB_KEY = 'selectAssets';

/**
 * Default filter state for the Build Filters tab.
 * Starts with one blank condition row so users can immediately pick a property.
 */
export const DEFAULT_DYNAMIC_FILTER: LogicalPredicate = {
    type: 'logical',
    operator: LogicalOperatorType.AND,
    operands: [{ type: 'property' }],
};
