import { EditOutlined, ReadOutlined, SafetyCertificateOutlined, SettingOutlined } from '@ant-design/icons';
import React from 'react';

import { capitalizeFirstLetter } from '@app/shared/textUtil';

export const getRoleNameFromUrn = (roleUrn: string) => {
    return capitalizeFirstLetter(roleUrn.replace('urn:li:dataHubRole:', ''));
};

export const mapRoleIcon = (roleName) => {
    if (roleName === 'Admin') {
        return <SettingOutlined />;
    }
    if (roleName === 'Editor') {
        return <EditOutlined />;
    }
    if (roleName === 'Reader') {
        return <ReadOutlined />;
    }
    // Custom roles get a certificate/badge icon to distinguish them from the "No Role" person icon.
    return <SafetyCertificateOutlined />;
};

export const shouldShowGlossary = (canManageGlossary: boolean, hideGlossary: boolean) => {
    return canManageGlossary || !hideGlossary;
};
