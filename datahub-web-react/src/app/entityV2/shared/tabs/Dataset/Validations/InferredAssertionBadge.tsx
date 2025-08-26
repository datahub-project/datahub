import { Sparkle } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { REDESIGN_COLORS } from '@src/app/entity/shared/constants';

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
