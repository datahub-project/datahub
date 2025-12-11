/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from 'antd';
import styled from 'styled-components';

export const StyledPanelButton = styled(Button)`
    margin: 2px 0;
    padding: 8px 13px;
    display: flex;
    align-items: center;
    width: 100%;

    .anticon {
        margin-bottom: -2px;
    }
`;
