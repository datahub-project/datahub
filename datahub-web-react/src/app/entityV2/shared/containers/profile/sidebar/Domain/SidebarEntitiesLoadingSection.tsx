import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React from 'react';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const SidebarEntitiesLoadingSection = () => {
    return <Spin indicator={<LoadingOutlined style={{ color: ANTD_GRAY[7] }} />} />;
};

export default SidebarEntitiesLoadingSection;
