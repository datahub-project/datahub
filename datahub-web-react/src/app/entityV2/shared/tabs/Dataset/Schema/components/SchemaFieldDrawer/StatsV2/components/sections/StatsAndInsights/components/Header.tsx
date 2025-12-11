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

import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import {
    DATE_COMMA_TIME_TZ,
    formatTimestamp,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/utils';
import { Text } from '@src/alchemy-components';

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
