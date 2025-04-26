import React from 'react';

import { ManageViews } from '@app/entity/view/ManageViews';
import { ManageOwnership } from '@app/entityV2/ownership/ManageOwnership';
import { ManageIdentities } from '@app/identity/ManageIdentities';
import { ManagePermissions } from '@app/permissions/ManagePermissions';
import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { AccessTokens } from '@app/settingsV2/AccessTokens';
import { Preferences } from '@app/settingsV2/Preferences';
import { Features } from '@app/settingsV2/features/Features';
import ManagePosts from '@app/settingsV2/posts/ManagePosts';

/**
 * URL Paths for each settings page.
 */
export const PATHS = [
    { path: 'views', content: <ManageViews /> },
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <ManageIdentities version="v2" /> },
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
    { path: 'features', content: <Features /> },
];

export const DEFAULT_PATH = PATHS[0];
