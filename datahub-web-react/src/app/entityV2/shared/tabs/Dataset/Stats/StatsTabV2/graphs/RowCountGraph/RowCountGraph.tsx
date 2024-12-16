import { Button, LineChart, SimpleSelect } from '@src/alchemy-components';
import { GraphCard } from '@src/alchemy-components/components/GraphCard';
import { pluralize } from '@src/app/shared/textUtil';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { LookbackWindow } from '../../../lookbackWindows';
import GraphPopover from '../components/GraphPopover';
import { GRAPH_LOOPBACK_WINDOWS, GRAPH_LOOPBACK_WINDOWS_OPTIONS } from '../constants';
import useRowCountData from './useRowCountData';
import MonthOverMonthPill from '../components/MonthOverMonthPill';

type RowCountGraphProps = {
    urn?: string;
};

export default function RowCountGraph({ urn }: RowCountGraphProps) {
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOPBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>('MONTH');

    const { data, loading } = useRowCountData(urn, lookbackWindow);

    useEffect(() => {
        if (rangeType) setLookbackWindow(GRAPH_LOOPBACK_WINDOWS[rangeType]);
    }, [rangeType, setLookbackWindow]);

    return (
        <GraphCard
            title="Row Count"
            isEmpty={data.length === 0}
            loading={loading}
            graphHeight="290px"
            renderControls={() => (
                <>
                    <Button icon="Add" variant="outline">
                        Assertion
                    </Button>
                    <SimpleSelect
                        options={GRAPH_LOOPBACK_WINDOWS_OPTIONS}
                        values={rangeType ? [rangeType] : []}
                        onUpdate={(values) => setRangeType(values[0])}
                        showClear={false}
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
                    leftAxisProps={{ hideZero: true }}
                    popoverRenderer={(datum) => (
                        <GraphPopover
                            header={dayjs(datum.time).format('dddd. MMM. D ’YY')}
                            value={`${datum.value} ${pluralize(datum.value, 'Row')}`}
                            pills={<MonthOverMonthPill value={datum.mom} />}
                        />
                    )}
                />
            )}
        />
    );
}
