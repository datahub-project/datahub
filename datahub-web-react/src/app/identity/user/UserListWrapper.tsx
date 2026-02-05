import React from 'react';

import { UserAndGroupList } from '@app/identity/user/UserAndGroupList';
import { UserList as UserListV2 } from '@app/identity/user/UserListV2';
import { useIsInviteUsersEnabled } from '@app/useAppConfig';

/**
 * UserListWrapper - Delegates to the appropriate user list implementation
 * based on the inviteUsersEnabled feature flag.
 *
 * This thin wrapper enables a merge-proof architecture:
 * - OSS updates go to UserListV2.tsx (exported as UserList) - merge target
 * - SaaS features live in UserAndGroupList.tsx (never touched by OSS merges)
 * - This wrapper file is ~30 lines, so merge conflicts are rare and minimal
 *
 * When merging from OSS:
 * 1. OSS updates to their UserListV2.tsx apply cleanly to our UserListV2.tsx
 * 2. No conflicts with UserAndGroupList.tsx (SaaS-only)
 * 3. This wrapper rarely conflicts (just flag logic)
 *
 * @returns UserAndGroupList (SaaS) or UserListV2 (OSS)
 */
export const UserListWrapper = () => {
    const inviteUsersEnabled = useIsInviteUsersEnabled();

    if (inviteUsersEnabled) {
        // SaaS: Enhanced component with invite functionality
        // Features: Recommended tab, email invitations, bulk actions, platform filtering
        return <UserAndGroupList />;
    }

    // OSS: Basic user list from UserListV2.tsx
    // Features: Basic user management, role assignment, user deletion
    return <UserListV2 />;
};
