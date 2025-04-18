import { Card, Text } from '@components';
import { capitalizeFirstLetter, pluralize } from '@src/app/shared/textUtil';
import React from 'react';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { countFormatter } from '@src/utils/formatter';
import { useStatsSectionsContext } from '../StatsSectionsContext';
import { useGetStatsData } from '../useGetStatsData';
import { useGetStatsSections } from '../useGetStatsSections';
import { SectionKeys } from '../utils';
import { CARD_HEIGHT, CARD_WIDTH, LastMonthStatsContainer, StatCards } from './styledComponents';
import { ViewButton } from './ViewButton';

const LastMonthStats = () => {
    const { users, queryCount, totalOperations } = useGetStatsData();
    const { scrollToSection } = useGetStatsSections();
    const { sections } = useStatsSectionsContext();

    return (
        <LastMonthStatsContainer>
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
                />
                <Card
                    title={queryCount !== undefined ? countFormatter(queryCount) : ''}
                    subTitle={capitalizeFirstLetter(pluralize(queryCount || 0, 'Query'))}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={queryCount === undefined}
                    button={sections.queries.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.queries.hasData ? scrollToSection?.(SectionKeys.QUERIES) : undefined)}
                />
                <Card
                    title={totalOperations !== undefined ? countFormatter(totalOperations) : ''}
                    subTitle={pluralize(totalOperations || 0, 'Change')}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={totalOperations === undefined}
                    button={sections.changes.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.changes.hasData ? scrollToSection?.(SectionKeys.CHANGES) : undefined)}
                />
            </StatCards>
        </LastMonthStatsContainer>
    );
};

export default LastMonthStats;
