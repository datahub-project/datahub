import React from 'react';

import { ADDITIONAL_PATHS } from '@app/settingsV2/additionalSettingsPaths';
import { SettingsPath } from '@app/settingsV2/types';
import { lazyDefault, lazyNamed } from '@app/shared/lazyPage';

const ManageViews = lazyNamed(() => import('@app/entityV2/view/ManageViews'), 'ManageViews');
const AccessTokens = lazyNamed(() => import('@app/settingsV2/AccessTokens'), 'AccessTokens');
const IdentitiesContent = lazyNamed(() => import('@app/settingsV2/IdentitiesContent'), 'IdentitiesContent');
const ManagePolicies = lazyNamed(() => import('@app/permissions/policy/ManagePolicies'), 'ManagePolicies');
const Preferences = lazyNamed(() => import('@app/settingsV2/Preferences'), 'Preferences');
const ManagePermissions = lazyNamed(() => import('@app/permissions/ManagePermissions'), 'ManagePermissions');
const ManageOwnership = lazyNamed(() => import('@app/entityV2/ownership/ManageOwnership'), 'ManageOwnership');
const ManagePosts = lazyDefault(() => import('@app/settingsV2/posts/ManagePosts'));
const Features = lazyNamed(() => import('@app/settingsV2/features/Features'), 'Features');

/**
 * URL Paths for each settings page.
 */
export const PATHS: SettingsPath[] = [
    { path: 'views', content: <ManageViews /> },
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <IdentitiesContent /> },
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
    { path: 'features', content: <Features /> },
    ...ADDITIONAL_PATHS,
];

export const DEFAULT_PATH = PATHS[0];
