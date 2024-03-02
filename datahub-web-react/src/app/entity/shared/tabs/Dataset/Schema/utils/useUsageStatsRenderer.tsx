import { geekblue } from '@ant-design/colors';
import { Tooltip } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { UsageQueryResult } from '../../../../../../../types.generated';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';

const USAGE_BAR_MAX_WIDTH = 50;

export const UsageBar = styled.div<{ width: number }>`
    width: ${(props) => props.width}px;
    height: 4px;
    background-color: ${geekblue[3]};
    border-radius: 2px;
`;

const UsageBarContainer = styled.div`
    width: 100%;
    height: 100%;
`;

export default function useUsageStatsRenderer(usageStats?: UsageQueryResult | null) {
    const maxFieldUsageCount = useMemo(
        () => Math.max(...(usageStats?.aggregations?.fields?.map((field) => field?.count || 0) || [])),
        [usageStats],
    );

    const usageStatsRenderer = (fieldPath: string) => {
        const relevantUsageStats = usageStats?.aggregations?.fields?.find((fieldStats) =>
            pathMatchesNewPath(fieldStats?.fieldName, fieldPath),
        );

        if (!relevantUsageStats) {
            return null;
        }

        return (
            <Tooltip placement="topLeft" title={`${relevantUsageStats.count} queries / month`}>
                <UsageBarContainer>
                    <UsageBar width={((relevantUsageStats.count || 0) / maxFieldUsageCount) * USAGE_BAR_MAX_WIDTH} />
                </UsageBarContainer>
            </Tooltip>
        );
    };
    return usageStatsRenderer;
}
