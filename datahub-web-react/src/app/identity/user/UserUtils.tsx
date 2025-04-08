import { EditOutlined, ReadOutlined, SettingOutlined, UserOutlined } from '@ant-design/icons';
import React from 'react';

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
