import React from 'react';
import styled from 'styled-components';
import { RecentActions } from '../content/recent/RecentActions';
import { CenterTabs } from '../content/tabs/CenterTabs';

const Container = styled.div`
    flex: 2;
    overflow: hidden;
    overflow-y: auto;
    padding: 10px;
    margin-top: 8px;
    height: 100%;
    display: flex;
    flex-direction: column;
    /* Hide scrollbar for Chrome, Safari, and Opera */

    &::-webkit-scrollbar {
        display: none;
    }
`;

const Content = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
`;

export const CenterContent = () => {
    return (
        <Container>
            <Content>
                <RecentActions />
                <CenterTabs />
            </Content>
        </Container>
    );
};
