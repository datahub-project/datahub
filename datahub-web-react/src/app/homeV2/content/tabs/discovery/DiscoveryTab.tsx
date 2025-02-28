import React from 'react';
import styled from 'styled-components';
import { PinnedLinks } from '@src/app/homeV2/reference/sections/pinned/PinnedLinks';
import { Domains } from './sections/domains/Domains';
import { DataProducts } from './sections/dataProducts/DataProducts';
import { Insights } from './sections/insight/Insights';
import { Platforms } from './sections/platform/Platforms';

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
