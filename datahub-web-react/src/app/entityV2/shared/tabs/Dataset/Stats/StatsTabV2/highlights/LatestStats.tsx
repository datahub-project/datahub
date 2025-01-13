import { Card, Text } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { countFormatter } from '@src/utils/formatter';
import React from 'react';
import { useGetStatsData } from '../useGetStatsData';
import { useGetStatsSections } from '../useGetStatsSections';
import { SectionKeys } from '../utils';
import { CARD_HEIGHT, CARD_WIDTH, LatestStatsContainer, StatCards } from './styledComponents';
import { ViewButton } from './ViewButton';

const LatestStats = () => {
    const { columnStats, rowCount, columnCount } = useGetStatsData();
    const hasColumnStats = columnStats?.length > 0;

    const { scrollToSection } = useGetStatsSections();

    return (
        <LatestStatsContainer>
            <Text size="sm" weight="bold">
                Latest
            </Text>
            <StatCards>
                <Card
                    title={countFormatter(rowCount || 0)}
                    subTitle={pluralize(rowCount || 0, 'Row')}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={rowCount === undefined}
                    button={rowCount ? <ViewButton /> : undefined}
                    onClick={() => (rowCount ? scrollToSection?.(SectionKeys.ROWS_AND_USERS) : undefined)}
                />
                <Card
                    title={columnCount?.toString() || ''}
                    subTitle={pluralize(columnCount || 0, 'Column')}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={columnCount === undefined}
                    button={hasColumnStats ? <ViewButton /> : undefined}
                    onClick={() => (hasColumnStats ? scrollToSection?.(SectionKeys.COLUMN_STATS) : undefined)}
                />
            </StatCards>
        </LatestStatsContainer>
    );
};

export default LatestStats;
