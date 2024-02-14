import React, { useEffect } from 'react';
import styled from 'styled-components';
import analytics, { EventType } from '../analytics';
import { IntroduceYourselfLeftSidebar } from './introduce/IntroduceYourselfLeftSidebar';
import { IntroduceYourselfMainContent } from './introduce/IntroduceYourselfMainContent';

const Container = styled.div`
    display: flex;
    height: 100vh;
    background-color: #fff;
`;

export const IntroduceYourself = () => {
    useEffect(() => {
        analytics.event({ type: EventType.IntroduceYourselfViewEvent });
    }, []);

    return (
        <>
            <Container>
                <IntroduceYourselfLeftSidebar />
                <IntroduceYourselfMainContent />
            </Container>
        </>
    );
};
