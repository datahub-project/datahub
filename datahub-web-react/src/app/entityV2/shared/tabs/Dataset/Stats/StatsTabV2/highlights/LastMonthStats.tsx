import { Card, Text } from '@components';
import { capitalizeFirstLetter, pluralize } from '@src/app/shared/textUtil';
import React from 'react';
import { useGetStatsData } from '../useGetStatsData';
import { useGetStatsSections } from '../useGetStatsSections';
import { SectionKeys } from '../utils';
import { CARD_HEIGHT, CARD_WIDTH, LastMonthStatsContainer, StatCards } from './styledComponents';
import { ViewButton } from './ViewButton';

const LastMonthStats = () => {
    const { users, queryCount, totalOperations } = useGetStatsData();
    const { scrollToSection } = useGetStatsSections();

    return (
        <LastMonthStatsContainer>
            <Text size="sm" weight="bold">
                Last 30 days
            </Text>
            <StatCards>
                <Card
                    title={users?.length?.toString() || ''}
                    subTitle={pluralize(users?.length || 0, 'User')}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={users === undefined}
                    button={users && users.length ? <ViewButton /> : undefined}
                    onClick={() => (users && users.length ? scrollToSection?.(SectionKeys.ROWS_AND_USERS) : undefined)}
                />
                <Card
                    title={queryCount?.toString() || ''}
                    subTitle={capitalizeFirstLetter(pluralize(queryCount || 0, 'Query'))}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={queryCount === undefined}
                    button={queryCount ? <ViewButton /> : undefined}
                    onClick={() => (queryCount ? scrollToSection?.(SectionKeys.QUERIES) : undefined)}
                />
                <Card
                    title={totalOperations?.toString() || ''}
                    subTitle={pluralize(totalOperations || 0, 'Change')}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={totalOperations === undefined}
                    button={totalOperations ? <ViewButton /> : undefined}
                    onClick={() => (totalOperations ? scrollToSection?.(SectionKeys.CHANGES) : undefined)}
                />
            </StatCards>
        </LastMonthStatsContainer>
    );
};

export default LastMonthStats;
