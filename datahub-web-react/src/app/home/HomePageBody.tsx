/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { HomePageRecommendations } from '@app/home/HomePageRecommendations';

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
    const user = useUserContext()?.user;
    return <BodyContainer>{user && <HomePageRecommendations user={user} />}</BodyContainer>;
};
