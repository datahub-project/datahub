import { Tooltip } from '@components';
import { ChartBar } from '@phosphor-icons/react/dist/csr/ChartBar';
import { TrendUp } from '@phosphor-icons/react/dist/csr/TrendUp';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { DefaultTheme, useTheme } from 'styled-components';

import { pathMatchesInsensitiveToV2, pathMatchesNewPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { UsageQueryResult } from '@types';

// Shared SVG gradient id for the "has column stats" icon — a brand-intensity fill signalling activity.
const STATS_BRAND_GRADIENT_ID = 'schema-stats-brand-gradient';

const ICON_SIZE = 18;

// Map the 0–3 popularity level onto the brand-intensity scale so the trend icon reads hotter the
// more a field is queried.
function activityColor(theme: DefaultTheme, status?: number): string {
    switch (status) {
        case 3:
            return theme.colors.chartsBrandHigh;
        case 2:
            return theme.colors.chartsBrandMedium;
        case 1:
            return theme.colors.chartsBrandLow;
        default:
            return theme.colors.chartsBrandBase;
    }
}

const IconsContainer = styled.div`
    display: flex;
    gap: 6px;
    flex-direction: row;
    align-items: center;
`;

const IconWrapper = styled.div<{ hasStats: boolean; isFieldSelected: boolean }>`
    display: flex;
    align-items: center;
    svg {
        opacity: ${(props) => (props.isFieldSelected && !props.hasStats ? '0.5' : '')};
    }
`;

export default function useUsageStatsRenderer(
    usageStats?: UsageQueryResult | null,
    expandedDrawerFieldPath?: string | null,
) {
    const { t } = useTranslation('entity.profile.schema');
    const theme = useTheme();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const maxFieldUsageCount = useMemo(
        () => Math.max(...(usageStats?.aggregations?.fields?.map((field) => field?.count || 0) || [0])),
        [usageStats],
    );

    const usageStatsRenderer = (fieldPath: string) => {
        const isFieldSelected = expandedDrawerFieldPath === fieldPath;

        const fieldProfile = latestProfile?.fieldProfiles?.find((profile) =>
            pathMatchesInsensitiveToV2(profile.fieldPath, fieldPath),
        );

        const relevantUsageStats = usageStats?.aggregations?.fields?.find((fieldStats) =>
            pathMatchesNewPath(fieldStats?.fieldName, fieldPath),
        );
        let usageStatus: number | undefined;
        if (relevantUsageStats && maxFieldUsageCount > 0) {
            usageStatus = Math.ceil(((relevantUsageStats.count || 0) / maxFieldUsageCount) * 3);
        }

        return (
            <IconsContainer>
                <Tooltip
                    placement="top"
                    showArrow={false}
                    title={
                        relevantUsageStats
                            ? t('fieldPopularity.queriesPerMonth', {
                                  count: formatNumberWithoutAbbreviation(relevantUsageStats.count || 0),
                              })
                            : t('fieldPopularity.noUsageData')
                    }
                >
                    <IconWrapper hasStats={!!relevantUsageStats} isFieldSelected={isFieldSelected}>
                        <TrendUp size={ICON_SIZE} weight="bold" color={activityColor(theme, usageStatus)} />
                    </IconWrapper>
                </Tooltip>

                <Tooltip
                    placement="top"
                    title={
                        !fieldProfile ? t('usageStatsRenderer.noColumnStats') : t('usageStatsRenderer.hasColumnStats')
                    }
                >
                    <IconWrapper hasStats={!!fieldProfile} isFieldSelected={isFieldSelected}>
                        <ChartBar
                            size={ICON_SIZE}
                            weight="fill"
                            color={fieldProfile ? `url(#${STATS_BRAND_GRADIENT_ID})` : theme.colors.chartsBrandBase}
                        >
                            <defs>
                                <linearGradient id={STATS_BRAND_GRADIENT_ID} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor={theme.colors.chartsBrandHigh} />
                                    <stop offset="100%" stopColor={theme.colors.chartsBrandMedium} />
                                </linearGradient>
                            </defs>
                        </ChartBar>
                    </IconWrapper>
                </Tooltip>
            </IconsContainer>
        );
    };
    return usageStatsRenderer;
}
