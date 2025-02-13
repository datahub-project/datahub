import { BarChart, GraphCard } from '@src/alchemy-components';
import { COLOR_SCHEMES } from '@src/alchemy-components/components/BarChart/constants';
import { abbreviateNumber } from '@src/alchemy-components/components/dataviz/utils';
import React, { useCallback, useMemo } from 'react';
import RowCountByValuePopover from './components/RowCountByValuePopover';
import useStatsTabContext from '../../../../hooks/useStatsTabContext';

const BOTTOM_AXIS_NUMBER_OF_TICKS = 5;
const LEFT_AXIS_MAX_LENGTH_OF_LABEL = 5;
// additional margin for long bottom ticks
const RIGHT_MARGIN = 20;
const MAX_VALUES_TO_SHOW = 20;

export default function RowCountByValueChart() {
    const { properties } = useStatsTabContext();

    const data = useMemo(
        () =>
            (properties?.fieldProfile?.distinctValueFrequencies || [])
                .map((entry, index) => ({
                    y: index,
                    x: entry.frequency,
                    colorScheme: COLOR_SCHEMES?.[index % (COLOR_SCHEMES.length - 1)],
                    label: entry.value,
                }))
                .slice(0, MAX_VALUES_TO_SHOW),
        [properties?.fieldProfile?.distinctValueFrequencies],
    );

    const maxLengthOfLabel = useMemo(() => Math.max(...data.map((datum) => datum.label.length)), [data]);

    const yValueToLabel = useCallback((y: number) => data.find((datum) => datum.y === y)?.label, [data]);

    const leftMargin = useMemo(() => {
        // adjust left margin depending on max length of data's labels
        if (maxLengthOfLabel < 3) return 0;
        if (maxLengthOfLabel < 5) return 10;
        return 20;
    }, [maxLengthOfLabel]);

    if (data.length === 0) return null;

    return (
        <GraphCard
            title="Row Count by Value"
            subTitle="Number of rows with each distinct value"
            loading={properties?.profilesDataLoading}
            graphHeight="280px"
            renderGraph={() => (
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
                    margin={{ top: 0, right: RIGHT_MARGIN, bottom: 0, left: leftMargin }}
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
            )}
        />
    );
}
