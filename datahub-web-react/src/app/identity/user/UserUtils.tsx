import { EditOutlined, ReadOutlined, SettingOutlined, UserOutlined } from '@ant-design/icons';
import React from 'react';

import { capitalizeFirstLetter } from '@app/shared/textUtil';

import { CorpUser } from '@types';

export const getRoleNameFromUrn = (roleUrn: string) => {
    return capitalizeFirstLetter(roleUrn.replace('urn:li:dataHubRole:', ''));
};

export const mapRoleIcon = (roleName) => {
    let icon = <UserOutlined />;
    if (roleName === 'Admin') {
        icon = <SettingOutlined />;
    }
    if (roleName === 'Editor') {
        icon = <EditOutlined />;
    }
    if (roleName === 'Reader') {
        icon = <ReadOutlined />;
    }
    return icon;
};

export const shouldShowGlossary = (canManageGlossary: boolean, hideGlossary: boolean) => {
    return canManageGlossary || !hideGlossary;
};

/**
 * Determines if a user should display the "Top User" pill based on their usage percentile.
 * Users with >= 90% usage percentile are considered "Top Users".
 */
export const shouldShowTopUserPill = (user: CorpUser): boolean => {
    return Boolean(
        user?.usageFeatures?.userUsagePercentilePast30Days && user?.usageFeatures?.userUsagePercentilePast30Days >= 90,
    );
};
