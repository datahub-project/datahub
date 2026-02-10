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
import { formatBytes } from '@src/app/shared/formatNumber';
import { pluralize } from '@src/app/shared/textUtil';
import { countFormatter } from '@src/utils/formatter';

const LatestStats = () => {
    const { rowCount, sizeInBytes } = useGetStatsData();

    const { scrollToSection } = useGetStatsSections();
    const { sections } = useStatsSectionsContext();

    const formattedSize = sizeInBytes !== undefined ? formatBytes(sizeInBytes) : undefined;

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
                    title={formattedSize ? `${formattedSize.number} ${formattedSize.unit}` : ''}
                    subTitle="Storage Size"
                    maxWidth={CARD_WIDTH}
                    height={CARD_HEIGHT}
                    isEmpty={sizeInBytes === undefined}
                    button={sections.storage?.hasData ? <ViewButton /> : undefined}
                    onClick={() => (sections.storage?.hasData ? scrollToSection?.(SectionKeys.STORAGE) : undefined)}
                    dataTestId="storage-size-card"
                />
            </StatCards>
        </LatestStatsContainer>
    );
};

export default LatestStats;
