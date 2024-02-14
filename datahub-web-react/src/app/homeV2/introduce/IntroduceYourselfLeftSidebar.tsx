import React from 'react';
import styled from 'styled-components';
import AcrylIcon from '../../../images/datahub-logo-light.svg?react';

const Container = styled.div`
    margin: 15px;
    max-width: 489px;
    border-radius: 18px;
    background: linear-gradient(159.46deg, #6d64fe 1.95%, #d994e2 99.47%);
    padding-top: 52px;
    padding-left: 52px;
    padding-right: 65px;
    display: flex;
    flex-direction: column;
    algin-self: flex-end;
`;

const Content = styled.div``;

const Title = styled.div`
    // flex: 1;
    max-width: 240px;
    margin-bottom: 30px;
    color: #fff;
    font-family: Mulish;
    font-size: 35px;
    font-style: normal;
    font-weight: 700;
    line-height: 44px; /* 125.714% */
`;

const Subtitle = styled.div`
    color: #fff;
    width: 371px;
    padding-bottom: 130px;

    font-family: Mulish;
    font-size: 16px;
    font-style: normal;
    font-weight: 400;
    line-height: 24px; /* 171.429% */
`;

const Spacer = styled.div`
    flex: 1;
    height: auto;
`;

const AcrylTitle = styled.div`
    color: #fff;

    font-family: Mulish;
    font-size: 14px;
    font-style: normal;
    font-weight: 700;
    letter-spacing: 1px;
    display: flex;
    align-items: center;
    justify-content: start;
`;

export const IntroduceYourselfLeftSidebar = () => {
    return (
        <Container>
            <AcrylTitle>
                <AcrylIcon />
                <span style={{ marginLeft: 8 }}> ACRYL</span>
            </AcrylTitle>
            <Spacer />
            <Content>
                <Title>Start your data journey</Title>
                <Subtitle>
                    Thousands of data professionals use Acryl to discover trusted data, ensure data quality, and
                    collaborate with their colleagues.
                </Subtitle>
            </Content>
        </Container>
    );
};
