import React from 'react';

import { ManageOwnership } from '@app/entityV2/ownership/ManageOwnership';
import { ManageViews } from '@app/entityV2/view/ManageViews';
import { ManagePermissions } from '@app/permissions/ManagePermissions';
import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { AccessTokens } from '@app/settingsV2/AccessTokens';
import { IdentitiesContent } from '@app/settingsV2/IdentitiesContent';
import { Preferences } from '@app/settingsV2/Preferences';
import ManageHelpLink from '@app/settingsV2/helpLink/ManageHelpLink';
import { ManageActorNotifications } from '@app/settingsV2/personal/notifications/ManageActorNotifications';
import { ManageActorSubscriptions } from '@app/settingsV2/personal/subscriptions/ManageActorSubscriptions';
import { PlatformAiSettings } from '@app/settingsV2/platform/PlatformAiSettings';
import { PlatformIntegrations } from '@app/settingsV2/platform/PlatformIntegrations';
import { PlatformSsoIntegrations } from '@app/settingsV2/platform/PlatformSsoIntegrations';
import { PlatformNotifications } from '@app/settingsV2/platform/notifications/PlatformNotifications';
import ManagePosts from '@app/settingsV2/posts/ManagePosts';

const ACRYL_PATHS = [
    { path: 'integrations', content: <PlatformIntegrations /> },
    { path: 'notifications', content: <PlatformNotifications /> },
    { path: 'sso', content: <PlatformSsoIntegrations /> },
    { path: 'ai', content: <PlatformAiSettings /> },
    { path: 'personal-notifications', content: <ManageActorNotifications isPersonal canManageNotifications /> },
    { path: 'personal-subscriptions', content: <ManageActorSubscriptions isPersonal /> },
];

/**
 * URL Paths for each settings page.
 */
export const PATHS = [
    { path: 'views', content: <ManageViews /> },
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <IdentitiesContent /> },
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
    /* acryl-main only */
    ...ACRYL_PATHS,
    { path: 'helpLink', content: <ManageHelpLink /> },
];

export const DEFAULT_PATH = PATHS[0];
