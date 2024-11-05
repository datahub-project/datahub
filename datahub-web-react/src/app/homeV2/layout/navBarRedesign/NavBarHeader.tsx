import { Sidebar } from '@phosphor-icons/react';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    width: 100%;
    height: 40px;
    align-items: center;
    gap: 8px;
`;

const Logotype = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    border-radius: 4px;
    background: white;

    & svg {
        height: 15px;
    }
`;

const Title = styled.div`
    color: #374066;
    font-style: normal;
    font: 700 16px/40px Mulish;
`;

const StyledNavBarToggle = styled(Sidebar)`
    cursor: pointer;
    margin: 0 0 0 auto;
    height: 15px;
    width: 15px;
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
    return (
        <Container>
            <StyledLink to="/">
                <Logotype>{logotype}</Logotype>
                <Title>Acryl Data</Title>
            </StyledLink>
            <StyledNavBarToggle />
        </Container>
    );
}
