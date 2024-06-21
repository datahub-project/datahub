import React from 'react';
import styled from 'styled-components';
import { NavLinksMenu } from './NavLinksMenu';

const Container = styled.div`
    border-radius: 47px;
    background-color: #7262d9;
    box-shadow: 0px 8px 8px 4px rgba(0, 0, 0, 0.25);
`;

export const NavLinks = () => {
    return (
        <Container>
            <NavLinksMenu />
        </Container>
    );
};
