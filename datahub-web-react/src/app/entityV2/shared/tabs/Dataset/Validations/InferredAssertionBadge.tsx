import React from 'react';
import styled from 'styled-components';
import { Sparkle } from 'phosphor-react';
import { REDESIGN_COLORS } from '@src/app/entity/shared/constants';
import { ANTD_GRAY } from '../../../constants';

const Container = styled.span`
    color: ${ANTD_GRAY[8]};
    margin: 4px;
    height: 100%;
    border: 1px solid transparent;
    cursor: pointer;
`;
const Logo = styled(Sparkle)`
    color: ${REDESIGN_COLORS.BLUE};
`;

export const InferredAssertionBadge = () => {
    return (
        <Container>
            <Logo size={16} />
        </Container>
    );
};
