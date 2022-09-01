import React from 'react';
import { EditOutlined, ReadOutlined, SettingOutlined, UserOutlined } from '@ant-design/icons';

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
