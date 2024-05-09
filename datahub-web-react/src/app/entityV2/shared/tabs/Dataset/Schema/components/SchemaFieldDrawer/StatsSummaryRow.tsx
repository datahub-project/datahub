import { Check } from '@mui/icons-material';
import TrendingDownOutlinedIcon from '@mui/icons-material/TrendingDownOutlined';
import TrendingUpOutlinedIcon from '@mui/icons-material/TrendingUpOutlined';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { pluralize } from '../../../../../../../shared/textUtil';
import { REDESIGN_COLORS } from '../../../../../constants';
import { extractChartValuesFromFieldProfiles } from '../../../Stats/historical/HistoricalStats';
import { decimalToPercentStr } from '../../utils/statsUtil';

const StatsSummaryRowContent = styled.div`
    display: flex;
    flex-direction: row;
    align-items: flex-start;
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
`;
const StyledTooltip = styled(Tooltip)`
    display: flex;
    gap: 5px;
    align-items: center;
`;

const TrendDetailContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 5px;
    width: 150px;
    align-items: center;
`;
const StatSummarySubtitle = styled.div`
    display: flex;
    flex-direction: row;
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

const Headline = styled.div`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-family: Mulish;
    font-size: 12px;
    font-weight: 700;
    line-height: 24px;
`;

const SubtitleText = styled(Typography.Text)<{ color?: string }>`
    color: ${(props) => (props.color ? props.color : REDESIGN_COLORS.DARK_GREY)};
    font-family: Mulish;
    font-size: 13px;
    line-height: 14px;
    font-weight: 400;
`;

const PRESENT_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.TERTIARY_GREEN}>
        <Check style={{ fontSize: 16 }} />
    </TrendingIconContainer>
);

const TRENDING_UP_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.TERTIARY_GREEN}>
        <TrendingUpOutlinedIcon style={{ fontSize: 16 }} />
    </TrendingIconContainer>
);

const TRENDING_DOWN_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.WARNING_RED}>
        <TrendingDownOutlinedIcon style={{ fontSize: 16 }} />
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
            <TrendDetailContainer>
                <StyledTooltip
                    title={`${nullCount} null ${pluralize(nullCount || 0, 'value')} found in last profile run`}
                >
                    {nullProportionChange < 0 && TRENDING_DOWN_ICON}
                    {nullProportionChange > 0 && TRENDING_UP_ICON}
                    <TitleContainer>
                        <Headline>Null Values</Headline>
                        <StatSummarySubtitle>
                            <SubtitleText>{decimalToPercentStr(nullProportion, 2)}</SubtitleText>
                        </StatSummarySubtitle>
                    </TitleContainer>
                </StyledTooltip>
            </TrendDetailContainer>
            <TrendDetailContainer>
                <StyledTooltip
                    title={`${uniqueCount} distinct ${pluralize(uniqueCount || 0, 'value')} found in last profile run`}
                >
                    {uniqueProportionChange < 0 && TRENDING_DOWN_ICON}
                    {uniqueProportionChange > 0 && TRENDING_UP_ICON}
                    <TitleContainer>
                        <Headline>Distinct Values</Headline>
                        <StatSummarySubtitle>
                            <SubtitleText>{decimalToPercentStr(uniqueProportion, 2)}</SubtitleText>
                        </StatSummarySubtitle>
                    </TitleContainer>
                </StyledTooltip>
            </TrendDetailContainer>
            <TrendDetailContainer>
                {numericalStatsCount > 0 && PRESENT_ICON}
                <TitleContainer>
                    <Headline>Numerical stats</Headline>
                    <StatSummarySubtitle>
                        {numericalStatsCount > 0 && <SubtitleText>{numericalStatsCount} stats</SubtitleText>}
                        {numericalStatsCount < 1 && (
                            <SubtitleText color={REDESIGN_COLORS.SECONDARY_LIGHT_GREY}>None</SubtitleText>
                        )}
                    </StatSummarySubtitle>
                </TitleContainer>
            </TrendDetailContainer>
        </StatsSummaryRowContent>
    );
}
