import { Card, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import { ViewButton } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/ViewButton';
import {
    CARD_HEIGHT,
    CARD_WIDTH,
    LastMonthStatsContainer,
    StatCards,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/styledComponents';
import { useGetStatsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData';
import { useGetStatsSections } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsSections';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { countFormatter } from '@src/utils/formatter';

const LastMonthStats = () => {
    const { t } = useTranslation('entity.profile.stats');
    const { users, queryCount, totalOperations } = useGetStatsData();
    const { scrollToSection } = useGetStatsSections();
    const { sections } = useStatsSectionsContext();

    return (
        <LastMonthStatsContainer data-testid="last-month-stats">
            <Text size="sm" weight="bold">
                {t('lastMonthStats.label')}
            </Text>
            <StatCards>
                <Card
                    title={users?.length !== undefined ? formatNumberWithoutAbbreviation(users.length) : ''}
                    subTitle={t('lastMonthStats.user', { count: users?.length || 0 })}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={users === undefined}
                    button={sections.users.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.users.hasData ? scrollToSection?.(SectionKeys.ROWS_AND_USERS) : undefined)}
                    dataTestId="users-card"
                />
                <Card
                    title={queryCount !== undefined ? countFormatter(queryCount) : ''}
                    subTitle={t('lastMonthStats.query', { count: queryCount || 0 })}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={queryCount === undefined}
                    button={sections.queries.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.queries.hasData ? scrollToSection?.(SectionKeys.QUERIES) : undefined)}
                    dataTestId="queries-card"
                />
                <Card
                    title={totalOperations !== undefined ? countFormatter(totalOperations) : ''}
                    subTitle={t('lastMonthStats.change', { count: totalOperations || 0 })}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={totalOperations === undefined}
                    button={sections.changes.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.changes.hasData ? scrollToSection?.(SectionKeys.CHANGES) : undefined)}
                    dataTestId="changes-card"
                />
            </StatCards>
        </LastMonthStatsContainer>
    );
};

export default LastMonthStats;
