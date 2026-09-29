import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React from 'react';
import { useTheme } from 'styled-components';

const SidebarLineageLoadingSection = () => {
    const theme = useTheme();
    return <Spin indicator={<LoadingOutlined style={{ color: theme.colors.textTertiary }} />} />;
};

export default SidebarLineageLoadingSection;
