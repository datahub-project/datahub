import { EntityType } from '../../types.generated';

export const COMMON_PRIVILEGES = [
    {
        type: 'EDIT_ENTITY_TAGS',
        displayName: 'Edit Tags',
    },
    {
        type: 'EDIT_ENTITY_OWNERS',
        displayName: 'Edit Owners',
    },
    {
        type: 'EDIT_ENTITY_DOCS',
        displayName: 'Edit Documentation',
    },
    {
        type: 'EDIT_ENTITY_DOC_LINKS',
        displayName: 'Edit Links',
    },
    {
        type: 'EDIT_ENTITY',
        displayName: 'Edit All',
    },
];

// Ideally this mapping would be pushed from the server.
// Don't want this to be hardcoded as such.
export const ENTITY_PRIVILEGES = [
    {
        entityType: EntityType.Dataset,
        privileges: [
            ...COMMON_PRIVILEGES,
            {
                type: 'EDIT_DATASET_COL_TAGS',
                displayName: 'Edit Dataset Column Tags',
            },
            {
                type: 'EDIT_DATASET_COL_DESCRIPTION',
                displayName: 'Edit Dataset Column Description',
            },
        ],
    },
    {
        entityType: EntityType.Chart,
        privileges: COMMON_PRIVILEGES,
    },
    {
        entityType: EntityType.Dashboard,
        privileges: COMMON_PRIVILEGES,
    },
    {
        entityType: EntityType.DataFlow,
        privileges: COMMON_PRIVILEGES,
    },
    {
        entityType: EntityType.DataJob,
        privileges: COMMON_PRIVILEGES,
    },
];
