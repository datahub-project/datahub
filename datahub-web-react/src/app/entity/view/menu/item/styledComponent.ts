/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import MenuItem from 'antd/lib/menu/MenuItem';
import styled from 'styled-components';

export const MenuItemStyle = styled(MenuItem)`
    &&&& {
        background-color: transparent;
        a {
            color: inherit;
        }
    }
`;
