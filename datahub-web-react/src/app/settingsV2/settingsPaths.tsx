import React from 'react';
import { ManageIdentities } from '../identity/ManageIdentities';
import { ManagePermissions } from '../permissions/ManagePermissions';
import { AccessTokens } from './AccessTokens';
import { PlatformIntegrations } from './platform/PlatformIntegrations';
import { PlatformNotifications } from './platform/notifications/PlatformNotifications';
import { PlatformSsoIntegrations } from './platform/PlatformSsoIntegrations';
import { Preferences } from './Preferences';
import { ManagePolicies } from '../permissions/policy/ManagePolicies';
import { ManageViews } from '../entity/view/ManageViews';
import { ManageOwnership } from '../entityV2/ownership/ManageOwnership';
import { ManageActorNotifications } from './personal/notifications/ManageActorNotifications';
import { ManageActorSubscriptions } from './personal/subscriptions/ManageActorSubscriptions';
import ManagePosts from './posts/ManagePosts';
import ManageHelpLink from './helpLink/ManageHelpLink';

const ACRYL_PATHS = [
    { path: 'integrations', content: <PlatformIntegrations /> },
    { path: 'notifications', content: <PlatformNotifications /> },
    { path: 'sso', content: <PlatformSsoIntegrations /> },
    { path: 'personal-notifications', content: <ManageActorNotifications isPersonal canManageNotifications /> },
    { path: 'personal-subscriptions', content: <ManageActorSubscriptions isPersonal /> },
];

/**
 * URL Paths for each settings page.
 */
export const PATHS = [
    { path: 'views', content: <ManageViews /> },
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <ManageIdentities /> },
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
    /* acryl-main only */
    ...ACRYL_PATHS,
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
    { path: 'helpLink', content: <ManageHelpLink /> },
];

export const DEFAULT_PATH = PATHS[0];
