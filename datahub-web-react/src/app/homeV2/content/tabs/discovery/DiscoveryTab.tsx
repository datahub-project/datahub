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

import { DataProducts } from '@app/homeV2/content/tabs/discovery/sections/dataProducts/DataProducts';
import { Domains } from '@app/homeV2/content/tabs/discovery/sections/domains/Domains';
import { Insights } from '@app/homeV2/content/tabs/discovery/sections/insight/Insights';
import { Platforms } from '@app/homeV2/content/tabs/discovery/sections/platform/Platforms';
import { OnboardingCards } from '@src/app/homeV2/content/tabs/discovery/sections/onboarding/OnboardingCards';
import { PinnedLinks } from '@src/app/homeV2/reference/sections/pinned/PinnedLinks';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 8px;
    margin-top: 16px;
`;

export const DiscoveryTab = () => {
    return (
        <Container>
            <OnboardingCards />
            <PinnedLinks hideIfEmpty />
            <Domains />
            <DataProducts />
            <Insights />
            <Platforms />
        </Container>
    );
};
