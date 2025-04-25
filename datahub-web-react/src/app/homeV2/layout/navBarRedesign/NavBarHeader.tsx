import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import NavBarToggler from '@app/homeV2/layout/navBarRedesign/NavBarToggler';

import DatahubCoreLogo from '@images/datahub_core.svg?react';

const Container = styled.div`
    display: flex;
    width: 100%;
    height: 40px;
    min-height: 40px;
    align-items: center;
    gap: 8px;
    margin-left: -3px;
    transition: padding 250ms ease-in-out;
`;

const Logotype = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 24px;
    max-height: 24px;
    max-width: 42px;
    border-radius: 4px;
    position: relative;
    object-fit: contain;
    & svg,
    img {
        max-height: 24px;
        max-width: 42px;
        min-width: 42px;
        object-fit: contain;
    }
`;

const StyledLink = styled(Link)`
    display: flex;
    height: 40px;
    align-items: center;
    max-width: calc(100% - 40px);
    width: 100%;
    gap: 8px;
`;

type Props = {
    logotype?: React.ReactElement;
};

export default function NavBarHeader({ logotype }: Props) {
    const { isCollapsed } = useNavBarContext();

    return (
        <Container>
            <StyledLink to="/">
                <Logotype>{logotype}</Logotype>
                {!isCollapsed && <DatahubCoreLogo />}
            </StyledLink>
            {!isCollapsed && <NavBarToggler />}
        </Container>
    );
}
