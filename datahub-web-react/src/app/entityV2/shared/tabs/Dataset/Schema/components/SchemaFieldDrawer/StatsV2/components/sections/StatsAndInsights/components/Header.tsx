import React from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('entity.profile.schema');
    const { properties } = useStatsTabContext();
    const profiles = properties?.profiles;
    const theLatestProfile = profiles?.[0];
    const lastUpdatedAt = theLatestProfile?.timestampMillis;

    const lastUpdatedAtString = formatTimestamp(lastUpdatedAt, DATE_COMMA_TIME_TZ);

    return (
        <Container>
            <Text weight="semiBold">{t('statsV2Insights.sectionHeading')}</Text>
            <Text>{t('statsV2Insights.lastUpdated', { timestamp: lastUpdatedAtString })}</Text>
        </Container>
    );
}
