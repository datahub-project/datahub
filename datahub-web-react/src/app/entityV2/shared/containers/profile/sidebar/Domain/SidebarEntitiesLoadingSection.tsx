/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React from 'react';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const SidebarEntitiesLoadingSection = () => {
    return <Spin indicator={<LoadingOutlined style={{ color: ANTD_GRAY[7] }} />} />;
};

export default SidebarEntitiesLoadingSection;
