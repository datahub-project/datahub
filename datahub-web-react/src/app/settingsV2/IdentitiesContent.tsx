import React from 'react';

import { ManageUsersAndGroups } from '@app/identity/ManageUsersAndGroups';

/**
 * Component wrapper for identities management in Settings
 */
export const IdentitiesContent = () => {
    return <ManageUsersAndGroups version="v2" />;
};
