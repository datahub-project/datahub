import React from 'react';
import styled from 'styled-components';
import Icon from '@ant-design/icons/lib/components/Icon';
import StatChart, { ChartCard } from '../../../Stats/historical/charts/StatChart';
import { getFixedLookbackWindow } from '../../../../../../../shared/time/timeUtils';
import { LookbackWindow } from '../../../Stats/lookbackWindows';
import { DatasetFieldProfile, SchemaField } from '../../../../../../../../types.generated';
import NoStatsAvailble from '../../../../../../../../images/no-stats-available.svg?react';
import { REDESIGN_COLORS } from '../../../../../constants';
import { computeChartTickInterval, extractChartValuesFromFieldProfiles } from '../../../../../utils';

const CHART_WIDTH = 460;
const CHART_HEIGHT = 170;
const DEFAULT_LINE_COLOR = REDESIGN_COLORS.BACKGROUND_PURPLE;

const StatSection = styled.div`
    padding: 12px 16px;
    // Temporary solution for chart circle color, as V1 theme colors are in place.
    circle {
        fill: ${REDESIGN_COLORS.BACKGROUND_PRIMARY_1};
    }
`;

const ChartRow = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(${CHART_WIDTH}px, 1fr));
    gap: 12px 0;
    column-gap: 12px;
    & > ${ChartCard} {
        box-shadow: none;
        border-radius: 9px;
        border: 1px solid #eeeef3;
        .ant-card-body {
            padding: 5px;
        }
    }
`;

const NoDataContainer = styled.div`
    margin: 40px auto;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const Section = styled.div`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-weight: 700;
    font-size: 12px;
    line-height: 24px;
`;

const StyledIcon = styled(Icon)`
    font-size: 80px;
    margin-bottom: 6px;
    color: ${REDESIGN_COLORS.WHITE};
`;

interface Props {
    properties: {
        expandedField: SchemaField;
        fieldProfile: DatasetFieldProfile | undefined;
        profiles: any[];
    };
    lookbackWindow: LookbackWindow;
}

const getLookbackWindowSize = (window: LookbackWindow) => {
    return window.windowSize;
};

export default function StatsSidebarColumnTab({ properties, lookbackWindow }: Props) {
    const { fieldProfile, profiles } = properties;
    const selectedFieldPath = fieldProfile?.fieldPath || '';

    const selectedWindowSize = getLookbackWindowSize(lookbackWindow);
    const selectedWindow = getFixedLookbackWindow(selectedWindowSize);

    const graphTickInterval = computeChartTickInterval(selectedWindowSize);
    const graphDateRange = {
        start: selectedWindow.startTime.toString(),
        end: selectedWindow.endTime.toString(),
    };

    // Extracting chart values from field profiles
    const nullCountChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'nullCount',
    );
    const nullPercentageChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'nullProportion',
    );
    const distinctCountChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'uniqueCount',
    );
    const distinctPercentageChartValues: Array<any> = extractChartValuesFromFieldProfiles(
        profiles,
        selectedFieldPath,
        'uniqueProportion',
    );

    if (!fieldProfile) {
        return (
            <NoDataContainer>
                <StyledIcon component={NoStatsAvailble} />
                <Section>No column statistics found</Section>
            </NoDataContainer>
        );
    }

    return (
        <StatSection>
            <ChartRow>
                <StatChart
                    width={CHART_WIDTH}
                    height={CHART_HEIGHT}
                    lineColor={DEFAULT_LINE_COLOR}
                    title="Null Count Over Time"
                    tickInterval={graphTickInterval}
                    dateRange={graphDateRange}
                    values={nullCountChartValues}
                />
                <StatChart
                    width={CHART_WIDTH}
                    height={CHART_HEIGHT}
                    lineColor={DEFAULT_LINE_COLOR}
                    title="Null Percentage Over Time"
                    tickInterval={graphTickInterval}
                    dateRange={graphDateRange}
                    values={nullPercentageChartValues}
                />
                <StatChart
                    width={CHART_WIDTH}
                    height={CHART_HEIGHT}
                    lineColor={DEFAULT_LINE_COLOR}
                    title="Distinct Count Over Time"
                    tickInterval={graphTickInterval}
                    dateRange={graphDateRange}
                    values={distinctCountChartValues}
                />
                <StatChart
                    width={CHART_WIDTH}
                    height={CHART_HEIGHT}
                    lineColor={DEFAULT_LINE_COLOR}
                    title="Distinct Percentage Over Time"
                    tickInterval={graphTickInterval}
                    dateRange={graphDateRange}
                    values={distinctPercentageChartValues}
                />
            </ChartRow>
        </StatSection>
    );
}
