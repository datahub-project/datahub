import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import NavBarToggler from './NavBarToggler';
import { useNavBarContext } from './NavBarContext';

const Container = styled.div<{ isCollapsed?: boolean }>`
    display: flex;
    width: 100%;
    height: 40px;
    min-height: 40px;
    align-items: center;
    gap: 8px;
    ${(props) => props.isCollapsed && 'padding-left: 6px;'}
    transition: padding 250ms ease-in-out;
`;

const Logotype = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 4px;
    background: white;

    & svg {
        height: 20px;
    }
`;

const Title = styled.div`
    color: #374066;
    font-style: normal;
    font: 700 16px/40px Mulish;
    text-wrap: nowrap;
`;

const StyledLink = styled(Link)`
    display: flex
    height: 40px;
    align-items: center;
    gap: 8px;
`;

type Props = {
    logotype?: React.ReactElement;
};

export default function NavBarHeader({ logotype }: Props) {
    const { isCollapsed } = useNavBarContext();

    return (
        <Container isCollapsed={isCollapsed}>
            <StyledLink to="/">
                <Logotype>{logotype}</Logotype>
                {!isCollapsed ? <Title>Acryl Data</Title> : null}
            </StyledLink>
            {!isCollapsed && <NavBarToggler iconSize={15} />}
        </Container>
    );
}
