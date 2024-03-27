import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { NavLinks } from './NavLinks';
import AcrylIcon from '../../../images/acryl-light-mark.svg?react';

const Container = styled.div`
    height: 100vh;
    padding: 12px;
    background-color: none;
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    background-color: #3B2D94;
    border-radius: 32px;
    height: 100%;
    width: 52px;
`;

const Icon = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    margin-top: 3px;
    width: 44px;
    height: 44px;
    border-radius: 38px;
    border: 1px solid #32267D;
    background: #4C39BE;
    box-shadow: 0px 4px 4px 0px rgba(0, 0, 0, 0.25);
    margin-bottom: 10px;

    & svg {
        height: 22px;
    }
`;

export const NavSidebar = () => {
    return (
        <Container>
            <Content>
                <Link to="/">
                    <Icon>
                        <AcrylIcon />
                    </Icon>
                </Link>
                <NavLinks />
            </Content>
        </Container>
    );
};
