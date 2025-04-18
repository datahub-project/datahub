import React from 'react';
import { Spin } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';

const SidebarContentsLoadingSection = () => {
    return <Spin indicator={<LoadingOutlined />} />;
};

export default SidebarContentsLoadingSection;
