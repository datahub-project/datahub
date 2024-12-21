import { LineChart, SimpleSelect } from '@src/alchemy-components';
import { GraphCard } from '@src/alchemy-components/components/GraphCard';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { formatBytes } from '@src/app/shared/formatNumber';
import { LookbackWindow } from '../../../lookbackWindows';
import GraphPopover from '../components/GraphPopover';
import { GRAPH_LOOPBACK_WINDOWS, GRAPH_LOOPBACK_WINDOWS_OPTIONS } from '../constants';
import useStorageSizeData from './useStorageSizeData';
import MonthOverMonthPill from '../components/MonthOverMonthPill';

type RowCountGraphProps = {
    urn?: string;
};

export default function StorageSizeGraph({ urn }: RowCountGraphProps) {
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOPBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>('MONTH');

    const { data, loading } = useStorageSizeData(urn, lookbackWindow);

    useEffect(() => {
        if (rangeType) setLookbackWindow(GRAPH_LOOPBACK_WINDOWS[rangeType]);
    }, [rangeType, setLookbackWindow]);

    const bytesFormatter = (num: number) => {
        const formattedBytes = formatBytes(num);
        return `${formattedBytes.number} ${formattedBytes.unit}`;
    };

    return (
        <GraphCard
            title="Storage Size"
            isEmpty={data.length === 0}
            loading={loading}
            graphHeight="290px"
            renderControls={() => (
                <>
                    <SimpleSelect
                        options={GRAPH_LOOPBACK_WINDOWS_OPTIONS}
                        values={rangeType ? [rangeType] : []}
                        onUpdate={(values) => setRangeType(values[0])}
                        showClear={false}
                        width="full"
                    />
                </>
            )}
            renderGraph={() => (
                <LineChart
                    data={data}
                    xAccessor={(d) => d.time}
                    yAccessor={(d) => d.value}
                    yScale={{ type: 'linear', nice: true, round: true, zero: true }}
                    bottomAxisProps={{ tickFormat: (x) => dayjs(x).format('DD MMM') }}
                    leftAxisProps={{ hideZero: true, tickFormat: bytesFormatter }}
                    popoverRenderer={(datum) => (
                        <GraphPopover
                            header={dayjs(datum.time).format('dddd. MMM. D ’YY')}
                            value={bytesFormatter(datum.value)}
                            pills={<MonthOverMonthPill value={datum.mom} />}
                        />
                    )}
                />
            )}
        />
    );
}
