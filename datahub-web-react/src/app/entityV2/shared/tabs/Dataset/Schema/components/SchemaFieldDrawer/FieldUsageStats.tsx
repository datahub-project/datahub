import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { pathMatchesNewPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { ANTD_GRAY_V2 } from '@app/entityV2/shared/constants';
import { SectionHeader } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import { UsageBar } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useUsageStatsRenderer';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { SchemaField } from '@types';

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

const UsageSection = styled.div`
    margin-bottom: 24px;
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
        <UsageSection>
            <SectionHeader>Usage</SectionHeader>
            <UsageBarWrapper>
                <UsageBarBackground>
                    <UsageBar width={((relevantUsageStats.count || 0) / maxFieldUsageCount) * USAGE_BAR_MAX_WIDTH} />
                </UsageBarBackground>
                <UsageTextWrapper>
                    {formatNumberWithoutAbbreviation(relevantUsageStats.count || 0)} queries / month
                </UsageTextWrapper>
            </UsageBarWrapper>
        </UsageSection>
    );
}
