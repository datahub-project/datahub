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
