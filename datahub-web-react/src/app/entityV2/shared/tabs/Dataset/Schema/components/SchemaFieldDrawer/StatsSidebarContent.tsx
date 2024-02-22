import Icon from '@ant-design/icons/lib/components/Icon';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import NoStatsAvailble from '../../../../../../../../images/no-stats-available.svg?react';
import TrendingDownIcon from '../../../../../../../../images/trending-down-icon.svg?react';
import TrendingUpIcon from '../../../../../../../../images/trending-up-icon.svg?react';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../constants';
import { extractChartValuesFromFieldProfiles } from '../../../Stats/historical/HistoricalStats';
import SampleValueTag from '../../../Stats/snapshot/SampleValueTag';
import { decimalToPercentStr } from '../../utils/statsUtil';
import StatsSummaryRow from './StatsSummaryRow';
import { SectionHeader, StyledDivider } from './components';

const maxLabelWidth = 150;

const StatsWrapper = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    height: 100%;
    margin-bottom: 50px;
`;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
`;

const StatRow = styled.div`
    display: flex;
    flex-direction: column;
    padding-top: 12px;
    padding-bottom: 12px;
`;

const StatLabel = styled.div`
    display: flex;
    align-items: center;
    padding-top: 12px;
    padding-bottom: 12px;
    :not(:last-child) {
        border-bottom: 1px dashed;
        border-color: rgba(0, 0, 0, 0.3);
    }
`;

const LabelText = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
    width: ${maxLabelWidth}px;
    margin-right: 8px;
`;

const StatValue = styled.div<{ isDecreasing: boolean }>`
    color: ${(props) => (props.isDecreasing ? `${REDESIGN_COLORS.RED_ERROR_BORDER}` : `${REDESIGN_COLORS.DARK_GREY}`)};
    font-size: 12px;
    font-weight: 800;
    line-height: 24px;
`;

const TrendLines = styled.div`
    margin-left: 5px;
    margin-top: 4px;
`;

const NoDataContainer = styled.div`
    margin: 40px auto;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const Section = styled.div`
    color: #56668e;
    font-weight: 700;
    font-size: 12px;
    line-height: 24px;
`;

const StyledIcon = styled(Icon)`
    font-size: 80px;
    margin-bottom: 6px;
    color: #fff;
`;
interface Props {
    properties: {
        expandedField: SchemaField;
        fieldProfile: DatasetFieldProfile | undefined;
        profiles: any[];
    };
}

export function StatsSidebarContent({ properties }: Props) {
    const { expandedField, fieldProfile, profiles } = properties;

    const historicFieldProfiles = profiles.filter((profile) =>
        profile.fieldProfiles?.some((fieldProf) => fieldProf.fieldPath === expandedField.fieldPath),
    );

    const getFieldStatTrendComponent = (statName: string) => {
        const statValues = extractChartValuesFromFieldProfiles(profiles, expandedField.fieldPath, statName);

        if (!fieldProfile || !statValues[1]) return null;

        const currentValue = fieldProfile[statName];
        const lastValue = statValues[1].value;

        let trendLine: any = null;
        if (currentValue === null || currentValue === undefined || lastValue === null || lastValue === undefined)
            return null;
        if (currentValue > lastValue) {
            trendLine = <TrendingUpIcon />;
        } else if (currentValue < lastValue) {
            trendLine = <TrendingDownIcon />;
        }
        const isDecreasing = currentValue < lastValue;

        return trendLine ? [trendLine, isDecreasing] : null;
    };

    if (!fieldProfile) {
        return (
            <NoDataContainer>
                <StyledIcon component={NoStatsAvailble} />
                <Section>No column statistics found</Section>
            </NoDataContainer>
        );
    }

    const statsData = {
        stats: [
            {
                name: 'Null Count',
                value: fieldProfile.nullCount,
                trend: getFieldStatTrendComponent('nullCount'),
            },
            {
                name: 'Null %',
                value: decimalToPercentStr(fieldProfile.nullProportion, 2),
                trend: getFieldStatTrendComponent('nullProportion'),
            },
            {
                name: 'Distinct Count',
                value: fieldProfile.uniqueCount,
                trend: getFieldStatTrendComponent('uniqueCount'),
            },
            {
                name: 'Distinct %',
                value: decimalToPercentStr(fieldProfile.uniqueProportion, 2),
                trend: getFieldStatTrendComponent('uniqueProportion'),
            },
            {
                name: 'Min',
                value: fieldProfile.min,
                trend: getFieldStatTrendComponent('min'),
            },
            {
                name: 'Max',
                value: fieldProfile.max,
                trend: getFieldStatTrendComponent('max'),
            },
            {
                name: 'Median',
                value: fieldProfile.median,
                trend: getFieldStatTrendComponent('median'),
            },
            {
                name: 'Std Dev',
                value: fieldProfile.stdev,
                trend: getFieldStatTrendComponent('stdev'),
            },
            {
                name: 'Sample Values',
                value: fieldProfile.sampleValues
                    ?.filter((value) => value !== undefined)
                    .slice(0, 2)
                    .map((value) => <SampleValueTag value={value} />),
                trend: null,
            },
        ],
    };

    return (
        <StatsWrapper>
            <Header>
                <SectionHeader>Stats</SectionHeader>
            </Header>
            <StatsSummaryRow
                expandedField={expandedField}
                fieldProfile={fieldProfile}
                profiles={historicFieldProfiles}
            />
            <StyledDivider dashed />

            {fieldProfile && (
                <StatRow>
                    {statsData.stats.map((stat) => {
                        return (
                            <StatLabel key={stat.name}>
                                <LabelText>{stat.name}</LabelText>
                                <StatValue isDecreasing={stat.trend ? stat.trend[1] : false}>{stat.value}</StatValue>
                                <TrendLines>{stat.trend && stat.trend[0]}</TrendLines>
                            </StatLabel>
                        );
                    })}
                </StatRow>
            )}
        </StatsWrapper>
    );
}
