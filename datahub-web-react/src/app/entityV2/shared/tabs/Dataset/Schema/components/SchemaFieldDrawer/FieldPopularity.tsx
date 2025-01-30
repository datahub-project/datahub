import React, { useMemo } from 'react';
import { Tooltip } from '@components';
import { UsageQueryResult } from '../../../../../../../../types.generated';
import { pathMatchesNewPath } from '../../../../../../dataset/profile/schema/utils/utils';
import { PopularityBars } from './PopularityBars';
import { formatNumberWithoutAbbreviation } from '../../../../../../../shared/formatNumber';

type FieldPopularityProps = {
    isFieldSelected: boolean;
    usageStats: UsageQueryResult | null | undefined;
    fieldPath: string | null;
    displayOnDrawer?: boolean;
};
export const FieldPopularity = ({ isFieldSelected, usageStats, fieldPath, displayOnDrawer }: FieldPopularityProps) => {
    const maxFieldUsageCount = useMemo(
        () => Math.max(...(usageStats?.aggregations?.fields?.map((field) => field?.count || 0) || [])),
        [usageStats],
    );
    const relevantUsageStats = usageStats?.aggregations?.fields?.find((fieldStats) =>
        pathMatchesNewPath(fieldStats?.fieldName, fieldPath),
    );

    let usageStatus;

    if (relevantUsageStats) {
        const percentage = ((relevantUsageStats.count || 0) / maxFieldUsageCount) * 100;
        usageStatus = Math.ceil((percentage / 100) * 3);
    }
    return (
        <Tooltip
            placement="top"
            title={
                relevantUsageStats
                    ? `${formatNumberWithoutAbbreviation(relevantUsageStats.count || 0)} queries / month`
                    : 'No column usage data'
            }
            showArrow={false}
        >
            <div>
                <PopularityBars
                    status={usageStatus}
                    isFieldSelected={isFieldSelected}
                    size={displayOnDrawer ? 'default' : 'small'}
                />
            </div>
        </Tooltip>
    );
};
