import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import RowCountByValuePopover from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/RowCountByValuePopover';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { BarChart, GraphCard } from '@src/alchemy-components';
import { COLOR_SCHEMES } from '@src/alchemy-components/components/BarChart/constants';
import { abbreviateNumber } from '@src/alchemy-components/components/dataviz/utils';

const BOTTOM_AXIS_NUMBER_OF_TICKS = 5;
const LEFT_AXIS_MAX_LENGTH_OF_LABEL = 5;
// additional margin for long bottom ticks
const RIGHT_MARGIN = 20;
const HEIGHT_PER_BAR = 40;
const BOTTOM_AXIS_HEIGHT = 20;
const AVAILABLE_HEIGHT = 280;

const ScrollableWrapper = styled.div`
    overflow-y: auto;
    height: ${AVAILABLE_HEIGHT}px;
    max-height: ${AVAILABLE_HEIGHT}px;
    // workaround to not show scrollbar if content is same height
    line-height: 0;
`;

const ScrollableContent = styled.div<{ $height: number }>`
    height: ${(props) => props.$height}px;
`;

export default function RowCountByValueChart() {
    const { properties } = useStatsTabContext();

    const data = useMemo(
        () =>
            (properties?.fieldProfile?.distinctValueFrequencies || []).map((entry, index) => ({
                y: index,
                x: entry.frequency,
                colorScheme: COLOR_SCHEMES?.[index % (COLOR_SCHEMES.length - 1)],
                label: entry.value,
            })),
        [properties?.fieldProfile?.distinctValueFrequencies],
    );

    const yValueToLabel = useCallback((y: number) => data.find((datum) => datum.y === y)?.label, [data]);

    const chartHeight = useMemo(
        () => Math.max(data.length * HEIGHT_PER_BAR + BOTTOM_AXIS_HEIGHT, AVAILABLE_HEIGHT),
        [data],
    );

    if (data.length === 0) return null;

    return (
        <GraphCard
            title="Row Count by Value"
            subTitle="Number of rows with each distinct value"
            loading={properties?.profilesDataLoading}
            graphHeight={`${AVAILABLE_HEIGHT}px`}
            renderGraph={() => (
                <ScrollableWrapper>
                    <ScrollableContent $height={chartHeight}>
                        <BarChart
                            data={data}
                            horizontal
                            xScale={{
                                type: 'linear',
                                nice: true,
                                round: true,
                            }}
                            yScale={{
                                type: 'band',
                                reverse: true,
                                padding: 0.1,
                            }}
                            gridProps={{
                                rows: false,
                                columns: true,
                                strokeWidth: 1,
                            }}
                            margin={{ right: RIGHT_MARGIN }}
                            leftAxisProps={{
                                tickFormat: (y) => yValueToLabel(y),
                                computeNumTicks: () => data.length,
                            }}
                            bottomAxisProps={{
                                tickFormat: (v) => abbreviateNumber(v),
                                computeNumTicks: () => BOTTOM_AXIS_NUMBER_OF_TICKS,
                            }}
                            maxLengthOfLeftAxisLabel={LEFT_AXIS_MAX_LENGTH_OF_LABEL}
                            popoverRenderer={(datum) => (
                                <RowCountByValuePopover
                                    datum={datum}
                                    labelFormatter={(labelDatum) => yValueToLabel(labelDatum.y)}
                                />
                            )}
                        />
                    </ScrollableContent>
                </ScrollableWrapper>
            )}
        />
    );
}
