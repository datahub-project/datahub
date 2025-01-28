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
            href="https://www.acryldata.io/datahub-sign-up?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup"
            target="_blank"
            rel="noopener noreferrer"
        >
            Schedule a Demo
        </StyledButton>
    );
}
