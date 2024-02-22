import { Check } from '@mui/icons-material';
import TrendingDownOutlinedIcon from '@mui/icons-material/TrendingDownOutlined';
import TrendingUpOutlinedIcon from '@mui/icons-material/TrendingUpOutlined';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../constants';
import { extractChartValuesFromFieldProfiles } from '../../../Stats/historical/HistoricalStats';
import { decimalToPercentStr } from '../../utils/statsUtil';

const StatsSummaryRowContent = styled.div`
    display: flex;
    flex-direction: row;
    gap: 100px;
    align-items: flex-start;
`;

const TrendDetailContainer = styled.div`
    display: flex;
    flex-direction: column;
`;
const StatSummarySubtitle = styled.div`
    display: flex;
    flex-direction: row;
`;

const TrendingIconContainer = styled.div<{ color: string }>`
    border-radius: 50%;
    background: ${(props) => props.color};
    color: ${REDESIGN_COLORS.WHITE};
    height: 15px;
    width: 15px;
    padding-left: 2px;
    margin-right: 2px;
`;

const Headline = styled.div`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-family: Mulish;
    font-size: 12px;
    font-weight: 700;
    line-height: 24px;
`;

const SubtitleText = styled(Typography.Text)<{ margin?: number; color?: string }>`
    color: ${(props) => (props.color ? props.color : REDESIGN_COLORS.DARK_GREY)};
    font-family: Mulish;
    font-size: 13px;
    line-height: 14px;
    font-weight: 400;
    margin-left: ${(props) => (props.margin !== undefined ? `${props.margin}px` : '4px')};
`;

const PRESENT_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.TERTIARY_GREEN}>
        {/* <CheckIcon style={{ fontSize: 11 }} /> */}
        <Check style={{ fontSize: 11 }} />
    </TrendingIconContainer>
);

const TRENDING_UP_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.TERTIARY_GREEN}>
        <TrendingUpOutlinedIcon style={{ fontSize: 11 }} />
    </TrendingIconContainer>
);

const TRENDING_DOWN_ICON = (
    <TrendingIconContainer color={REDESIGN_COLORS.WARNING_RED}>
        <TrendingDownOutlinedIcon style={{ fontSize: 11 }} />
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

    const nullProportionChange =
        (nullProportion && historicalNullProportion[1] && nullProportion - historicalNullProportion[1].value) || 0;
    const uniqueProportionChange =
        (uniqueProportion && historicalUniqueProportion[1] && uniqueProportion - historicalUniqueProportion[1].value) ||
        0;

    return (
        <StatsSummaryRowContent>
            <TrendDetailContainer>
                <Tooltip title={`${nullCount} null value${nullCount === 1 ? '' : 's'} found in last profile run`}>
                    <Headline>Completeness</Headline>
                    <StatSummarySubtitle>
                        {nullProportionChange < 0 && TRENDING_DOWN_ICON}
                        {nullProportionChange > 0 && TRENDING_UP_ICON}
                        <SubtitleText>{decimalToPercentStr(nullProportion, 2)}</SubtitleText>
                    </StatSummarySubtitle>
                </Tooltip>
            </TrendDetailContainer>
            <TrendDetailContainer>
                <Tooltip title={`${uniqueCount} distinct value${nullCount === 1 ? '' : 's'} found in last profile run`}>
                    <Headline>Uniqueness</Headline>
                    <StatSummarySubtitle>
                        {uniqueProportionChange < 0 && TRENDING_DOWN_ICON}
                        {uniqueProportionChange > 0 && TRENDING_UP_ICON}
                        <SubtitleText>{decimalToPercentStr(uniqueProportion, 2)}</SubtitleText>
                    </StatSummarySubtitle>
                </Tooltip>
            </TrendDetailContainer>
            <TrendDetailContainer>
                <Headline>Numerical stats</Headline>
                <StatSummarySubtitle>
                    {numericalStatsCount > 0 && PRESENT_ICON}
                    {numericalStatsCount > 0 && <SubtitleText>{numericalStatsCount} stats</SubtitleText>}
                    {numericalStatsCount < 1 && (
                        <SubtitleText color={REDESIGN_COLORS.SECONDARY_LIGHT_GREY} margin={0}>
                            None
                        </SubtitleText>
                    )}
                </StatSummarySubtitle>
            </TrendDetailContainer>
        </StatsSummaryRowContent>
    );
}
