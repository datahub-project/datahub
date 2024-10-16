import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';

const Container = styled.div`
    color: ${ANTD_GRAY[6]};
    background-color: #ffffff;
    opacity: 0.9;
    border-color: black;
    border-radius: 6px;
    border: 1px solid ${ANTD_GRAY[6]};
    padding-right: 6px;
    padding-left: 6px;
    margin-right: 4px;
    margin-left: 4px;
`;

const Letter = styled.span`
    padding: 2px;
`;

export const CommandK = () => {
    return (
        <Container>
            <Letter>⌘</Letter>
            <Letter>K</Letter>
        </Container>
    );
};
