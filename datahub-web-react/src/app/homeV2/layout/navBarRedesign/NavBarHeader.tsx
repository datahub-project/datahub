import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useGlobalSettingsContext } from '@src/app/context/GlobalSettings/GlobalSettingsContext';
import { colors } from '@src/alchemy-components';
import NavBarToggler from './NavBarToggler';
import { useNavBarContext } from './NavBarContext';

const Container = styled.div`
    display: flex;
    width: 100%;
    height: 40px;
    min-height: 40px;
    align-items: center;
    gap: 8px;
    padding-left: 4px;
    transition: padding 250ms ease-in-out;
`;

const Logotype = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 4px;
    background: ${colors.white};
    padding: 4px;
    position: relative;

    & svg {
        height: 20px;
        width: 20px;
    }
`;

const Title = styled.div`
    color: #374066;
    font-style: normal;
    font: 700 16px/40px Mulish;
    text-wrap: nowrap;
    white-space: nowrap;
    overflow: hidden;
    max-width: calc(100% - 30px);
    text-overflow: ellipsis;
    margin-left: 8px;
`;

const StyledLink = styled(Link)`
    display: flex
    height: 40px;
    align-items: center;
    max-width: calc(100% - 40px);
    width: 100%;
`;

type Props = {
    logotype?: React.ReactElement;
};

export default function NavBarHeader({ logotype }: Props) {
    const { isCollapsed } = useNavBarContext();
    const { globalSettings } = useGlobalSettingsContext();
    const customName = globalSettings?.visualSettings?.customOrgName;

    return (
        <Container>
            <StyledLink to="/">
                <Logotype>{logotype}</Logotype>
                {!isCollapsed ? <Title>{customName || 'DataHub'}</Title> : null}
            </StyledLink>
            {!isCollapsed && <NavBarToggler />}
        </Container>
    );
}
