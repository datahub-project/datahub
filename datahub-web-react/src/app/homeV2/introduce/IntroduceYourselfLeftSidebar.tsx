import React from 'react';
import styled from 'styled-components';
import { Image } from 'antd';
import dataHubIcon from '../../../images/datahublogo.png';
import introduceBg from '../../../images/introduceBg.svg';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    max-width: 470px;
    margin: 15px;
    padding: 52px 65px 65px 52px;
    background: #191d2e url(${introduceBg}) no-repeat bottom right;
    border-radius: 18px;
`;

const Title = styled.div`
    max-width: 240px;
    margin-bottom: 30px;
    color: #fff;
    font: 700 35px/44px Mulish;
`;

const Subtitle = styled.div`
    width: 371px;
    color: #fff;
    font: 400 16px/24px Mulish;
`;

const AcrylTitle = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    font: 700 14px Mulish;
    color: #fff;
    letter-spacing: 1px;
`;

export const IntroduceYourselfLeftSidebar = () => {
    return (
        <Container>
            <AcrylTitle>
                <Image src={dataHubIcon} preview={false} style={{ width: 24 }} />
            </AcrylTitle>
            <div>
                <Title>Start your data journey</Title>
                <Subtitle>
                    Thousands of data professionals use Acryl to discover trusted data, ensure data quality and
                    collaborate with their colleagues
                </Subtitle>
            </div>
        </Container>
    );
};