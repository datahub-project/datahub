import OnboardingContext from '@app/onboarding/OnboardingContext';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { RecentlyEditedOrViewed } from './RecentlyEditedOrViewed';
import { useGetRecentActions } from './useGetRecentActions';
import { useUserContext } from '../../../context/useUserContext';

const Container = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    margin-top: 24px;
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
    const { user, loaded } = useUserContext();
    const { viewed, loading } = useGetRecentActions(user);
    const { isUserInitializing } = useContext(OnboardingContext);

    if (!loaded || loading || isUserInitializing) return null;
    if (!viewed.length) return null;

    return (
        <Container>
            <RecentlyEditedOrViewed entities={viewed} />
        </Container>
    );
};
