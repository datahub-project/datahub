import React from 'react';

import { ManageUsersAndGroups } from '@app/identity/ManageUsersAndGroups';

const USERS_AND_GROUPS_VERSION = 'v2';

/**
 * Component wrapper for identities management in Settings
 */
export const IdentitiesContent = () => {
    return <ManageUsersAndGroups version={USERS_AND_GROUPS_VERSION} />;
};
