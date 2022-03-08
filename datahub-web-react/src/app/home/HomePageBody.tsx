import React from 'react';
import styled from 'styled-components';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { HomePageRecommendations } from './HomePageRecommendations';

const BodyContainer = styled.div`
    padding: 20px 100px;
    margin: 0;
    background-color: ${(props) => props.theme.styles['homepage-background-lower-fade']};
    > div {
        margin-bottom: 20px;
    }
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
`;

export const HomePageBody = () => {
    const authenticatedUserUrn = useGetAuthenticatedUser()?.corpUser?.urn;
    return (
        <BodyContainer>
            {authenticatedUserUrn && <HomePageRecommendations userUrn={authenticatedUserUrn} />}
        </BodyContainer>
    );
};
