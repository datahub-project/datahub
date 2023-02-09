import React from 'react';
import { Column } from '@ant-design/plots';
import { ColumnConfig } from '@ant-design/charts';
import { DataRunEntity, plotColorLegendMapping } from '../interfaces';
import {
    formatUTCDateString,
    getRunPlotValues,
    getRunColor,
    getCustomContentToolTip,
    convertSecsToHumanReadable,
} from '../functions';

export const TimelinessPlot = ({ runs }: { runs: DataRunEntity[] }) => {
    const runsPlotData = runs.map((r) => {
        return {
            ...r,
            executionDate: formatUTCDateString(r.execution.logicalDate),
            values: getRunPlotValues(r),
            runColor: getRunColor(r),
        };
    });

    // get y values, idx 0 = start time, idx1 = landing time
    function getYs(index: number) {
        return runsPlotData.map((r) => (r.values !== null ? r.values[index] : 0));
    }
    const minVal = Math.floor(Math.min(...getYs(0)));
    const maxVal = Math.floor(Math.max(...getYs(1)));
    const diff = Math.ceil(maxVal * 0.01);
    // round up/down to the nearest multiple of 60 to have yAxis start/end at whole hour intervals
    const minY = 60.0 * Math.floor((minVal - diff > 0 ? minVal - diff : 0) / 60.0);
    const maxY = 60.0 * Math.ceil((maxVal + diff) / 60.0);

    // config for column chart
    const config: ColumnConfig = {
        data: runsPlotData,
        padding: 'auto' as const,
        xField: 'executionDate',
        yField: 'values',
        isRange: true,
        xAxis: {
            title: {
                text: 'Execution Date',
                style: {
                    fontWeight: 'bold',
                },
            },
        },
        yAxis: {
            title: {
                text: 'Starting and Landing Time',
                style: {
                    fontWeight: 'bold',
                },
            },
            label: {
                formatter: (val) => `T+${convertSecsToHumanReadable(+val, true)}`,
            },
            minLimit: minY,
            maxLimit: maxY,
        },
        legend: {
            position: 'top',
            itemName: {
                formatter: (val) => {
                    return plotColorLegendMapping[val];
                },
            },
        },
        seriesField: 'runColor',
        color: (data) => {
            return data.runColor;
        },
        onEvent: (chart, event) => {
            if (event.type === 'plot:click') {
                const url = event.data?.data?.externalUrl;
                if (url) window.open(url, '_blank');
            }
        },
        tooltip: {
            shared: false,
            customContent: (title, items) => {
                return getCustomContentToolTip(items);
            },
        },
    };

    return (
        <div style={{ paddingLeft: '10px' }}>
            <Column {...config} />
        </div>
    );
};
