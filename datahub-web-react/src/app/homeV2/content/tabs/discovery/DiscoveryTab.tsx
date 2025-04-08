import React from 'react';
import styled from 'styled-components';

import { DataProducts } from '@app/homeV2/content/tabs/discovery/sections/dataProducts/DataProducts';
import { Domains } from '@app/homeV2/content/tabs/discovery/sections/domains/Domains';
import { Insights } from '@app/homeV2/content/tabs/discovery/sections/insight/Insights';
import { Platforms } from '@app/homeV2/content/tabs/discovery/sections/platform/Platforms';
import { PinnedLinks } from '@src/app/homeV2/reference/sections/pinned/PinnedLinks';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 8px;
`;

export const DiscoveryTab = () => {
    return (
        <Container>
            <PinnedLinks hideIfEmpty />
            <Domains />
            <DataProducts />
            <Insights />
            <Platforms />
        </Container>
    );
};
