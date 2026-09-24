import React from 'react';

import { ManageOwnership } from '@app/entityV2/ownership/ManageOwnership';
import { ManageViews } from '@app/entityV2/view/ManageViews';
import { ManagePermissions } from '@app/permissions/ManagePermissions';
import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { AccessTokens } from '@app/settingsV2/AccessTokens';
import { IdentitiesContent } from '@app/settingsV2/IdentitiesContent';
import { Preferences } from '@app/settingsV2/Preferences';
import { ADDITIONAL_PATHS } from '@app/settingsV2/additionalSettingsPaths';
import { Features } from '@app/settingsV2/features/Features';
import ManagePosts from '@app/settingsV2/posts/ManagePosts';
import { SettingsPath } from '@app/settingsV2/types';

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
