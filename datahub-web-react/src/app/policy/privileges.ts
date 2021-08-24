import { EntityType } from '../../types.generated';

/**
 * TODO: Determine best way to push this from the server.
 * Ideally this mapping would be pushed from the server.
 * It is expected that the server knows how to enforce the following
 * privileges.
 *
 * For now, this must be kept in sync with <paste link here>
 */

/**
 * Default resource types that can be accessed controlled.
 */
export const RESOURCE_TYPES = [
    {
        type: EntityType.Dataset,
        displayName: 'Datasets',
    },
    {
        type: EntityType.Dashboard,
        displayName: 'Dashboards',
    },
    {
        type: EntityType.Chart,
        displayName: 'Charts',
    },
    {
        type: EntityType.DataFlow,
        displayName: 'Data Pipelines',
    },
    {
        type: EntityType.DataJob,
        displayName: 'Data Tasks',
    },
    {
        type: EntityType.Tag,
        displayName: 'Tags',
    },
];

export const COMMON_METADATA_PRIVILEGES = [
    {
        type: 'EDIT_ENTITY_TAGS',
        displayName: 'Edit Tags',
        description: 'The ability to add and remove tags to an asset.',
    },
    {
        type: 'EDIT_ENTITY_OWNERS',
        displayName: 'Edit Owners',
        description: 'The ability to add and remove owners of an asset.',
    },
    {
        type: 'EDIT_ENTITY_DOCS',
        displayName: 'Edit Documentation',
        description: 'The ability to edit documentation about an asset.',
    },
    {
        type: 'EDIT_ENTITY_DOC_LINKS',
        displayName: 'Edit Links',
        description: 'The ability to edit links associated with an asset.',
    },
    {
        type: 'EDIT_ENTITY',
        displayName: 'Edit All',
        description: 'The ability to edit any information about an asset. Super user privileges.',
    },
];

/**
 * Privileges on a per-type basis.
 */
export const RESOURCE_PRIVILEGES = [
    {
        resourceType: EntityType.Dataset,
        privileges: [
            ...COMMON_METADATA_PRIVILEGES,
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
        resourceType: EntityType.Chart,
        privileges: COMMON_METADATA_PRIVILEGES,
    },
    {
        resourceType: EntityType.Dashboard,
        privileges: COMMON_METADATA_PRIVILEGES,
    },
    {
        resourceType: EntityType.DataFlow,
        privileges: COMMON_METADATA_PRIVILEGES,
    },
    {
        resourceType: EntityType.DataJob,
        privileges: COMMON_METADATA_PRIVILEGES,
    },
];

export const PLATFORM_PRIVILEGES = [
    {
        type: 'MANAGE_POLICIES',
        displayName: 'Manage Policies',
        description:
            'Create and remove access control policies. Be careful - Actors with this privilege are effectively super users.',
    },
    {
        type: 'MANAGE_USERS_GROUPS',
        displayName: 'Manage Users & Groups',
        description: 'Create, remove, and update users and groups on DataHub.',
    },
    {
        type: 'VIEW_ANALYTICS',
        displayName: 'View Analytics',
        description: 'View the DataHub analytics dashboard.',
    },
];
