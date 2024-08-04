import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import SmartAssertionIcon from '../../../../../../images/sparkle-fill.svg?react';

const Container = styled.span`
    color: ${ANTD_GRAY[8]};
    margin: 4px;
    height: 100%;
    border: 1px solid transparent;
    cursor: pointer;
`;

export const InferredAssertionBadge = () => {
    return (
        <Container>
            <SmartAssertionIcon />
        </Container>
    );
};
