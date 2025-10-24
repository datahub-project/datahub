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
 * All possible URL Paths for settings pages with their privilege requirements
 */
export const ALL_PATHS = [
    { path: 'views', content: <ManageViews />, requiresPrivilege: null }, // Controlled by feature flag
    { path: 'tokens', content: <AccessTokens />, requiresPrivilege: 'generatePersonalAccessTokens' },
    { path: 'identities', content: <ManageIdentities version="v2" />, requiresPrivilege: 'manageIdentities' },
    { path: 'policies', content: <ManagePolicies />, requiresPrivilege: 'managePolicies' },
    { path: 'preferences', content: <Preferences />, requiresPrivilege: null }, // Always accessible
    { path: 'permissions', content: <ManagePermissions />, requiresPrivilege: 'managePolicies' },
    { path: 'ownership', content: <ManageOwnership />, requiresPrivilege: 'manageOwnershipTypes' },
    { path: 'posts', content: <ManagePosts />, requiresPrivilege: 'manageGlobalAnnouncements' },
    { path: 'features', content: <Features />, requiresPrivilege: 'manageIngestion' },
];

// Helper function to filter paths based on privileges - used by SettingsPage
export const getFilteredPaths = (me: any, config: any) => {
    const isPoliciesEnabled = config?.policiesConfig?.enabled;
    const isIdentityManagementEnabled = config?.identityManagementConfig?.enabled;
    const { readOnlyModeEnabled } = config.featureFlags;

    const showPolicies = (isPoliciesEnabled && me && me?.platformPrivileges?.managePolicies) || false;
    const showUsersGroups = (isIdentityManagementEnabled && me && me?.platformPrivileges?.manageIdentities) || false;
    const showOwnershipTypes = me && me?.platformPrivileges?.manageOwnershipTypes;
    const showHomePagePosts = me && me?.platformPrivileges?.manageGlobalAnnouncements && !readOnlyModeEnabled;
    const showAccessTokens = me && me?.platformPrivileges?.generatePersonalAccessTokens;
    const showFeatures = me?.platformPrivileges?.manageIngestion;

    return ALL_PATHS.filter((pathConfig) => {
        const { path: pathKey, requiresPrivilege } = pathConfig;

        // Apply existing logic for feature flags and specific conditions
        if (pathKey === 'policies' && !showPolicies) return false;
        if (pathKey === 'permissions' && !showPolicies) return false;
        if (pathKey === 'identities' && !showUsersGroups) return false;
        if (pathKey === 'views') return true;
        if (pathKey === 'ownership' && !showOwnershipTypes) return false;
        if (pathKey === 'posts' && !showHomePagePosts) return false;
        if (pathKey === 'tokens' && !showAccessTokens) return false;
        if (pathKey === 'features' && !showFeatures) return false;

        // For other paths, check the privilege requirement
        if (requiresPrivilege && me?.platformPrivileges) {
            return Boolean(me.platformPrivileges[requiresPrivilege]);
        }

        // Always allow paths without privilege requirements
        return true;
    });
};

// Backward compatibility - will be filtered by SettingsPage based on privileges
export const PATHS = ALL_PATHS;
export const DEFAULT_PATH = { path: 'preferences', content: <Preferences /> };
