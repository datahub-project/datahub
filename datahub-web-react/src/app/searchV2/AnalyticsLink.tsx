/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import * as React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

const StyledAnalyticsLink = styled(Typography.Text)`
    display: flex;
    margin-right: 20px;
    text-decoration: none;
    && {
        font-size: 14px;
        color: ${(props) => props.theme.styles['layout-header-color']};
    }
    &&:hover {
        color: #000;
    }
`;

export default function AnalyticsLink() {
    return (
        <Link to="/analytics">
            <StyledAnalyticsLink strong>Analytics</StyledAnalyticsLink>
        </Link>
    );
}
