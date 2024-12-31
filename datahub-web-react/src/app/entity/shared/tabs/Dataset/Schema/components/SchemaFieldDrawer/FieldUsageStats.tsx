import React, { useMemo } from 'react';
import styled from 'styled-components';
import { GetDatasetQuery } from '../../../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../../../EntityContext';
import { ANTD_GRAY_V2 } from '../../../../../constants';
import { SectionHeader, StyledDivider } from './components';
import { pathMatchesNewPath } from '../../../../../../dataset/profile/schema/utils/utils';
import { UsageBar } from '../../utils/useUsageStatsRenderer';
import { SchemaField } from '../../../../../../../../types.generated';

const USAGE_BAR_MAX_WIDTH = 100;

const UsageBarWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const UsageBarBackground = styled.div`
    background-color: ${ANTD_GRAY_V2[3]};
    border-radius: 2px;
    height: 4px;
    width: ${USAGE_BAR_MAX_WIDTH}px;
`;

const UsageTextWrapper = styled.span`
    margin-left: 8px;
`;

interface Props {
    expandedField: SchemaField;
}

export default function FieldUsageStats({ expandedField }: Props) {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const usageStats = baseEntity?.dataset?.usageStats;
    const hasUsageStats = useMemo(() => (usageStats?.aggregations?.fields?.length || 0) > 0, [usageStats]);
    const maxFieldUsageCount = useMemo(
        () => Math.max(...(usageStats?.aggregations?.fields?.map((field) => field?.count || 0) || [])),
        [usageStats],
    );
    const relevantUsageStats = usageStats?.aggregations?.fields?.find((fieldStats) =>
        pathMatchesNewPath(fieldStats?.fieldName, expandedField.fieldPath),
    );

    if (!hasUsageStats || !relevantUsageStats) return null;

    return (
        <>
            <SectionHeader>Usage</SectionHeader>
            <UsageBarWrapper>
                <UsageBarBackground>
                    <UsageBar
                        width={Math.max(
                            ((relevantUsageStats.count || 0) / maxFieldUsageCount) * USAGE_BAR_MAX_WIDTH,
                            4,
                        )}
                    />
                </UsageBarBackground>
                <UsageTextWrapper>{relevantUsageStats.count || 0} queries / month</UsageTextWrapper>
            </UsageBarWrapper>
            <StyledDivider />
        </>
    );
}
