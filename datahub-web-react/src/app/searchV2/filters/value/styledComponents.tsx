/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Menu } from 'antd';
import styled from 'styled-components';

export const OptionMenu = styled(Menu)`
    &&& .ant-menu-item {
        height: auto;
        line-height: 22px;
        height: auto;
        padding: 0;
    }

    &&& .ant-dropdown-menu-item {
        padding: 0;
    }
`;
