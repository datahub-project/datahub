import React from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile } from '../../../../../../../../types.generated';
import { pluralize } from '../../../../../../../shared/textUtil';
import TrendDetail from './TrendDetail';

const StatsSummaryRowContent = styled.div`
    display: flex;
    flex-direction: row;
    align-items: flex-start;
`;

interface Props {
    fieldProfile: DatasetFieldProfile | undefined;
}

export default function StatsSummaryRow({ fieldProfile }: Props) {
    const nullProportion = fieldProfile?.nullProportion;
    const nullCount = fieldProfile?.nullCount;
    const uniqueProportion = fieldProfile?.uniqueProportion;
    const uniqueCount = fieldProfile?.uniqueCount;

    const numericalStatsCount =
        (fieldProfile?.min !== null && fieldProfile?.min !== undefined ? 1 : 0) +
        (fieldProfile?.max !== null && fieldProfile?.max !== undefined ? 1 : 0) +
        (fieldProfile?.mean !== null && fieldProfile?.mean !== undefined ? 1 : 0) +
        (fieldProfile?.median !== null && fieldProfile?.median !== undefined ? 1 : 0) +
        (fieldProfile?.stdev !== null && fieldProfile?.stdev !== undefined ? 1 : 0);

    return (
        <StatsSummaryRowContent>
            <TrendDetail
                tooltipTitle={`${nullCount} null ${pluralize(
                    nullCount || 0,
                    'value',
                )} found for this column across rows`}
                headline="Null Values"
                proportion={nullProportion}
                showCount
                count={nullCount || 0}
            />
            <TrendDetail
                tooltipTitle={`${uniqueCount} unique ${pluralize(
                    uniqueCount || 0,
                    'value',
                )} found for this column across rows`}
                headline="Distinct Values"
                proportion={uniqueProportion}
                showCount
                count={uniqueCount || 0}
            />
            <TrendDetail
                tooltipTitle=""
                headline="Numerical stats"
                proportion={undefined}
                showCount
                count={numericalStatsCount}
            />
        </StatsSummaryRowContent>
    );
}
