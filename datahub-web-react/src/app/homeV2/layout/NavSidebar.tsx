import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { NavLinks } from './NavLinks';
import AcrylIcon from '../../../images/datahub-logo-light.svg?react';

const Container = styled.div`
    height: 100vh;
    padding: 12px;
    background-color: none;
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: space-between;
    background-color: #5c3fd1;
    border-radius: 24px;
    height: 100%;
    width: 46px;
`;

const Icon = styled.div`
    margin-top: 20px;
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
