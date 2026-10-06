import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import { Spin } from 'antd';
import React from 'react';

const SidebarContentsLoadingSection = () => {
    return <Spin indicator={<CircleNotch />} />;
};

export default SidebarContentsLoadingSection;
