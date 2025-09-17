import React from 'react';

import { ManageIdentities } from '@app/identity/ManageIdentities';
import { ManageUsersAndGroups } from '@app/identity/ManageUsersAndGroups';
import { useIsAppConfigContextLoaded, useIsInviteUsersEnabled } from '@app/useAppConfig';

/**
 * Component wrapper to properly handle the hook usage for identities management
 */
export const IdentitiesContent = () => {
    const isConfigLoaded = useIsAppConfigContextLoaded();
    const inviteUsersEnabled = useIsInviteUsersEnabled();

    // Wait for config to load to avoid rendering the wrong component initially
    if (!isConfigLoaded) {
        return null;
    }

    return inviteUsersEnabled ? <ManageUsersAndGroups /> : <ManageIdentities version="v2" />;
};
