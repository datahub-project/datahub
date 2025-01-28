import React from 'react';
import { Spin } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../../constants';

const SidebarLineageLoadingSection = () => {
    return <Spin indicator={<LoadingOutlined style={{ color: ANTD_GRAY[7] }} />} />;
};

export default SidebarLineageLoadingSection;
