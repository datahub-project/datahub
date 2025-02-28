import OnboardingContext from '@app/onboarding/OnboardingContext';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { RecentlyEditedOrViewed } from './RecentlyEditedOrViewed';
import { useGetRecentActions } from './useGetRecentActions';
import { useUserContext } from '../../../context/useUserContext';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    align-items: start;
    justify-content: start;
    padding: ${(props) => (props.$isShowNavBarRedesign ? '16px 20px' : '16px 24px 24px 24px')};
    background-color: #fff;

    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-top: 24px;
        overflow: hidden;
        border-radius: 10px;
        border: 1.5px solid #ecf2f9;
    `}

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        border-radius: ${props.theme.styles['border-radius-navbar-redesign']};
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        margin: 0 5px;
    `}
`;

/**
 * Component for displaying recent actions, e.g. things recently viewed or edited for a given user.
 */
export const RecentActions = () => {
    const { user, loaded } = useUserContext();
    const { viewed, loading } = useGetRecentActions(user);
    const { isUserInitializing } = useContext(OnboardingContext);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    if (!loaded || loading || isUserInitializing) return null;
    if (!viewed.length) return null;

    return (
        <Container $isShowNavBarRedesign={isShowNavBarRedesign}>
            <RecentlyEditedOrViewed entities={viewed} />
        </Container>
    );
};
