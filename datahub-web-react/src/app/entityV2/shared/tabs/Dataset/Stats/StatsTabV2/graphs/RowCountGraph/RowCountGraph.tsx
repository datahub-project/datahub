import { LineChart, GraphCard } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { AssertionType } from '@src/types.generated';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { LookbackWindow } from '../../../lookbackWindows';
import AddAssertionButton from '../components/AddAssertionButton';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import { GRAPH_LOOPBACK_WINDOWS, GRAPH_LOOPBACK_WINDOWS_OPTIONS } from '../constants';
import useRowCountData from './useRowCountData';
import TimeRangeSelect from '../components/TimeRangeSelect';

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
            width="70%"
            renderControls={() => (
                <>
                    <AddAssertionButton assertionType={AssertionType.Volume} />

                    <TimeRangeSelect
                        options={GRAPH_LOOPBACK_WINDOWS_OPTIONS}
                        values={rangeType ? [rangeType] : []}
                        onUpdate={(values) => setRangeType(values[0])}
                        loading={loading}
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
