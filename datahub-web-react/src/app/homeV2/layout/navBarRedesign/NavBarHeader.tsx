import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { colors, Pill } from '@src/alchemy-components';
import NavBarToggler from './NavBarToggler';
import { useNavBarContext } from './NavBarContext';

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

const Title = styled.div`
    color: ${colors.gray[1700]};
    font-style: normal;
    font: 700 16px Mulish;
    text-wrap: nowrap;
    white-space: nowrap;
    overflow: hidden;
    max-width: calc(100% - 30px);
    text-overflow: ellipsis;
    height: 24px;
    display: flex;
    align-items: end;
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
                {!isCollapsed ? <Title>DataHub</Title> : null}
                {!isCollapsed && <Pill label="Core" variant="filled" color="gray" size="sm" />}
            </StyledLink>
            {!isCollapsed && <NavBarToggler />}
        </Container>
    );
}
