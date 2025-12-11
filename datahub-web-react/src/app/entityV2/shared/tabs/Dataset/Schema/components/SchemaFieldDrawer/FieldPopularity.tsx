/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React, { useMemo } from 'react';

import { pathMatchesNewPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { PopularityBars } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

import { UsageQueryResult } from '@types';

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
