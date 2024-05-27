import { Check } from '@mui/icons-material';
import React from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { pluralize } from '../../../../../../../shared/textUtil';
import { REDESIGN_COLORS } from '../../../../../constants';
import TrendDetail from './TrendDetail';
import { extractChartValuesFromFieldProfiles } from '../../../../../utils';

const StatsSummaryRowContent = styled.div`
    display: flex;
    flex-direction: row;
    align-items: flex-start;
`;

const TrendingIconContainer = styled.div<{ color: string }>`
    border-radius: 50%;
    background: ${(props) => props.color};
    color: ${REDESIGN_COLORS.WHITE};
    height: 24px;
    width: 24px;
    padding: 4px;
    margin-top: 6px;
`;

const PRESENT_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.TERTIARY_GREEN}>
        <Check style={{ fontSize: 16 }} />
    </TrendingIconContainer>
);

interface Props {
    expandedField: SchemaField;
    fieldProfile: DatasetFieldProfile | undefined;
    profiles: any[];
}

export default function StatsSummaryRow({ expandedField, fieldProfile, profiles }: Props) {
    const historicalNullProportion = extractChartValuesFromFieldProfiles(
        profiles,
        expandedField.fieldPath,
        'nullProportion',
    );
    const historicalUniqueProportion = extractChartValuesFromFieldProfiles(
        profiles,
        expandedField.fieldPath,
        'uniqueProportion',
    );

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

    // note- 1-index represents the previous profile run
    const nullProportionChange =
        (nullProportion && historicalNullProportion[1] && nullProportion - historicalNullProportion[1].value) || 0;
    const uniqueProportionChange =
        (uniqueProportion && historicalUniqueProportion[1] && uniqueProportion - historicalUniqueProportion[1].value) ||
        0;

    return (
        <StatsSummaryRowContent>
            <TrendDetail
                tooltipTitle={`${nullCount} null ${pluralize(nullCount || 0, 'value')} found in last profile run`}
                headline="Null Values"
                proportion={nullProportion}
                change={nullProportionChange}
            />
            <TrendDetail
                tooltipTitle={`${uniqueCount} distinct ${pluralize(
                    uniqueCount || 0,
                    'value',
                )} found in last profile run`}
                headline="Distinct Values"
                proportion={uniqueProportion}
                change={uniqueProportionChange}
            />
            <TrendDetail
                tooltipTitle=""
                trendIcon={numericalStatsCount > 0 ? PRESENT_ICON : undefined}
                headline="Numerical stats"
                proportion={undefined}
                change={0}
                showCount
                count={numericalStatsCount}
            />
        </StatsSummaryRowContent>
    );
}
