import React from 'react';
import { Text } from '@src/alchemy-components';
import styled from 'styled-components';
import useStatsTabContext from '../../../../hooks/useStatsTabContext';
import { DATE_COMMA_TIME_TZ, formatTimestamp } from '../../../../utils';

const Container = styled.div`
    padding-bottom: 16px;
`;

export default function Header() {
    const { properties } = useStatsTabContext();
    const profiles = properties?.profiles;
    const theLatestProfile = profiles?.[0];
    const lastUpdatedAt = theLatestProfile?.timestampMillis;

    const lastUpdatedAtString = formatTimestamp(lastUpdatedAt, DATE_COMMA_TIME_TZ);

    return (
        <Container>
            <Text weight="semiBold">Stats & Insights</Text>
            <Text color="gray">Last Updated: {lastUpdatedAtString}</Text>
        </Container>
    );
}
