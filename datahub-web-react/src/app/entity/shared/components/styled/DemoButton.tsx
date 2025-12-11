/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    padding: 8px;
    font-size: 14px;
    margin-left: 18px;
`;

export default function DemoButton() {
    return (
        <StyledButton
            type="primary"
            href="https://www.datahub.com/demo?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup"
            target="_blank"
            rel="noopener noreferrer"
        >
            Schedule a Demo
        </StyledButton>
    );
}
