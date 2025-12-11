/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { IntroduceYourselfLeftSidebar } from '@app/homeV2/introduce/IntroduceYourselfLeftSidebar';
import { IntroduceYourselfMainContent } from '@app/homeV2/introduce/IntroduceYourselfMainContent';

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
