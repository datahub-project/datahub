import React from 'react';
import styled from 'styled-components';
import { RecentlyEditedOrViewed } from './RecentlyEditedOrViewed';
import { useGetRecentActions } from './useGetRecentActions';
import { useUserContext } from '../../../context/useUserContext';

const Container = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    margin-top: 12px;
    padding: 16px 24px 24px 24px;
    overflow: hidden;
    background-color: #fff;
    border-radius: 10px;
    border: 1.5px solid #ecf2f9;
`;

/**
 * Component for displaying recent actions, e.g. things recently viewed or edited for a given user.
 */
export const RecentActions = () => {
    const { user } = useUserContext();
    const recentActions = useGetRecentActions(user);
    const recentlyViewed = recentActions.viewed;

    return (
        <>
            {(recentlyViewed.length && (
                <>
                    <Container>
                        <RecentlyEditedOrViewed entities={recentlyViewed} />
                    </Container>
                </>
            )) ||
                null}
        </>
    );
};
