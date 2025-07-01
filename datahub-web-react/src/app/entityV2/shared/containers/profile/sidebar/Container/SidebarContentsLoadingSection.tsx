import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React from 'react';

const SidebarContentsLoadingSection = () => {
    return <Spin indicator={<LoadingOutlined />} />;
};

export default SidebarContentsLoadingSection;
