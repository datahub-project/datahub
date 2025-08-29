import React from 'react';

import { ManageIdentities } from '@app/identity/ManageIdentities';
import { ManageUsersAndGroups } from '@app/identity/ManageUsersAndGroups';
import { useIsInviteUsersEnabled } from '@app/useAppConfig';

/**
 * Component wrapper to properly handle the hook usage for identities management
 */
export const IdentitiesContent = () => {
    const inviteUsersEnabled = useIsInviteUsersEnabled();
    return inviteUsersEnabled ? <ManageUsersAndGroups /> : <ManageIdentities version="v2" />;
};
