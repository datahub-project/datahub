import React from 'react';
import styled from 'styled-components';
import { Announcements } from '../action/announcement/Announcements';
import { PendingTasks } from '../action/task/PendingTasks';

const Container = styled.div`
    overflow-y: auto;
    padding: 0px 12px 12px 12px;
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-height: 100%;
`;

export const RightSidebar = () => {
    return (
        <Container>
            <Content>
                <Announcements />
                <PendingTasks />
            </Content>
        </Container>
    );
};
