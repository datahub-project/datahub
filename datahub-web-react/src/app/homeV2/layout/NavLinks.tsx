import React from 'react';
import styled from 'styled-components';
import { NavLinksMenu } from './NavLinksMenu';

const Container = styled.div`
    border-radius: 40px;
    background-color: rgba(255, 255, 255, 0.2);
    padding-top: 20px;
    padding-bottom: 20px;
`;

export const NavLinks = () => {
    return (
        <Container>
            <NavLinksMenu />
        </Container>
    );
};
