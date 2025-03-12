import React from 'react';
import { ManageIdentities } from '../identity/ManageIdentities';
import { ManagePermissions } from '../permissions/ManagePermissions';
import { AccessTokens } from './AccessTokens';
import { Preferences } from './Preferences';
import { ManagePolicies } from '../permissions/policy/ManagePolicies';
import { ManageViews } from '../entity/view/ManageViews';
import { ManageOwnership } from '../entityV2/ownership/ManageOwnership';
import ManagePosts from './posts/ManagePosts';
import { Features } from './features/Features';

/**
 * URL Paths for each settings page.
 */
export const PATHS = [
    { path: 'views', content: <ManageViews /> },
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <ManageIdentities /> },
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
    { path: 'features', content: <Features /> },
];

export const DEFAULT_PATH = PATHS[0];
