import React from 'react';
import styled from 'styled-components';

import { NavLinksMenu } from '@app/homeV2/layout/NavLinksMenu';

const Container = styled.div`
    border-radius: 47px;
    background-color: ${(props) => props.theme.colors.buttonFillBrand};
    box-shadow: ${(props) => props.theme.colors.shadowLg};
`;

export const NavLinks = () => {
    return (
        <Container>
            <NavLinksMenu />
        </Container>
    );
};
