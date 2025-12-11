/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ManageOwnership } from '@app/entityV2/ownership/ManageOwnership';
import { ManageViews } from '@app/entityV2/view/ManageViews';
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
