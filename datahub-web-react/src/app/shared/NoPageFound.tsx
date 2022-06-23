import React from 'react';
import { Button } from 'antd';
import styled from 'styled-components';
import { useHistory } from 'react-router';

const MainContainer = styled.div`
    height: 100vh;
    display: flex;
    justify-content: center;
    align-items: center;
`;

const PageNotFoundContainer = styled.div`
    max-width: 520px;
    width: 100%;
    line-height: 1.4;
    text-align: center;
`;

const PageNotFoundTextContainer = styled.div`
    position: relative;
    height: 240px;
`;

const NumberContainer = styled.h1`
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    font-size: 252px;
    font-weight: 900;
    margin: 0px;
    color: #262626;
    text-transform: uppercase;
    letter-spacing: -40px;
    margin-left: -20px;
`;

const Number = styled.span`
    text-shadow: -8px 0px 0px #fff;
`;

const SubTitle = styled.h2`
    font-size: 20px;
    font-weight: 400;
    text-transform: uppercase;
    color: #000;
    margin-top: 0px;
    margin-bottom: 25px;
`;

export const NoPageFound = () => {
    const history = useHistory();

    const goToHomepage = () => {
        history.push('/');
    };

    return (
        <MainContainer>
            <PageNotFoundContainer>
                <PageNotFoundTextContainer>
                    <NumberContainer>
                        <Number>4</Number>
                        <Number>0</Number>
                        <Number>4</Number>
                    </NumberContainer>
                </PageNotFoundTextContainer>
                <SubTitle>The page your requested was not found,</SubTitle>
                <Button onClick={goToHomepage}>Back to Home</Button>
            </PageNotFoundContainer>
        </MainContainer>
    );
};
