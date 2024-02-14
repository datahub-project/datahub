import React from 'react';
import styled from 'styled-components';
import { ThunderboltFilled } from '@ant-design/icons';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../constants';

const Container = styled.div`
    padding: 10px;
    padding-right: 12px;
    padding-left: 12px;
    color: ${ANTD_GRAY[8]};
    border-radius: 8px;
    margin-left: 8px;
    box-shadow: 0px 0px 4px 0px #0000001a;
    height: 100%;
    &&:hover {
        border: 1px solid ${REDESIGN_COLORS.BLUE};
    }
`;

const Logo = styled(ThunderboltFilled)`
    color: ${REDESIGN_COLORS.BLUE};
`;

export const InferredAssertionBadge = () => {
    return (
        <Container>
            <Logo />
        </Container>
    );
};
