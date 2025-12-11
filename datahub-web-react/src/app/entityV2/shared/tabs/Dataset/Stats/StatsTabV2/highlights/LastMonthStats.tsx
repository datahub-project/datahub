/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Card, Text } from '@components';
import React from 'react';

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
import { capitalizeFirstLetter, pluralize } from '@src/app/shared/textUtil';
import { countFormatter } from '@src/utils/formatter';

const LastMonthStats = () => {
    const { users, queryCount, totalOperations } = useGetStatsData();
    const { scrollToSection } = useGetStatsSections();
    const { sections } = useStatsSectionsContext();

    return (
        <LastMonthStatsContainer data-testid="last-month-stats">
            <Text size="sm" weight="bold">
                Last 30 days
            </Text>
            <StatCards>
                <Card
                    title={users?.length !== undefined ? formatNumberWithoutAbbreviation(users.length) : ''}
                    subTitle={pluralize(users?.length || 0, 'User')}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={users === undefined}
                    button={sections.users.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.users.hasData ? scrollToSection?.(SectionKeys.ROWS_AND_USERS) : undefined)}
                    dataTestId="users-card"
                />
                <Card
                    title={queryCount !== undefined ? countFormatter(queryCount) : ''}
                    subTitle={capitalizeFirstLetter(pluralize(queryCount || 0, 'Query'))}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={queryCount === undefined}
                    button={sections.queries.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.queries.hasData ? scrollToSection?.(SectionKeys.QUERIES) : undefined)}
                    dataTestId="queries-card"
                />
                <Card
                    title={totalOperations !== undefined ? countFormatter(totalOperations) : ''}
                    subTitle={pluralize(totalOperations || 0, 'Change')}
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
