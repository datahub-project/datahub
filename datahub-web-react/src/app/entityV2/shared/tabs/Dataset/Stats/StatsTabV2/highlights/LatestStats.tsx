import { Card, Text } from '@components';
import React from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import { ViewButton } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/ViewButton';
import {
    CARD_HEIGHT,
    CARD_WIDTH,
    LatestStatsContainer,
    StatCards,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/styledComponents';
import { useGetStatsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsData';
import { useGetStatsSections } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/useGetStatsSections';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { pluralize } from '@src/app/shared/textUtil';
import { countFormatter } from '@src/utils/formatter';

const LatestStats = () => {
    const { columnStats, rowCount, columnCount } = useGetStatsData();
    const hasColumnStats = columnStats?.length > 0;

    const { scrollToSection } = useGetStatsSections();
    const { sections } = useStatsSectionsContext();

    return (
        <LatestStatsContainer data-testid="latest-stats">
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
                    button={sections.rows.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.rows.hasData ? scrollToSection?.(SectionKeys.ROWS_AND_USERS) : undefined)}
                    dataTestId="rows-card"
                />
                <Card
                    title={columnCount !== undefined ? formatNumberWithoutAbbreviation(columnCount) : ''}
                    subTitle={pluralize(columnCount || 0, 'Column')}
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={columnCount === undefined}
                    button={hasColumnStats ? <ViewButton /> : undefined}
                    onClick={() => (hasColumnStats ? scrollToSection?.(SectionKeys.COLUMN_STATS) : undefined)}
                    dataTestId="columns-card"
                />
            </StatCards>
        </LatestStatsContainer>
    );
};

export default LatestStats;
