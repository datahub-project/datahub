import { CalendarChart, GraphCard } from '@components';
import { AssertionType, OperationType, TimeRange } from '@src/types.generated';
import React, { useEffect, useState } from 'react';
import AddAssertionButton from '../components/AddAssertionButton';
import TimeRangeSelect from '../components/TimeRangeSelect';
import { AGGRAGATION_TIME_RANGE_OPTIONS } from '../constants';
import { ChangeHistoryDrawer } from './components/ChangeHistoryDrawer/ChangeHistoryDrawer';
import ChangeHistoryPopover from './components/ChangeHistoryPopover';
import useChangeHistoryData from './hooks/useChangeHistoryData';
import useColorAccessors from './hooks/useColorAccessors';
import useGetCalendarRangeByTimeRange from './hooks/useGetCalendarRangeByTimeRange';
import useGetTimeRangeOptionsByTimeRange from '../hooks/useGetTimeRangeOptionsByTimeRange';
import TypesSelect from './components/TypesSelect';
import { DEFAULT_OPERATION_TYPES, OPERATION_TYPE_OPTIONS } from './constants';
import Subtitle from './components/Subtitle';
import useDataRange from './hooks/useDataRange';
import { useStatsSectionsContext } from '../../StatsSectionsContext';

// DAY, WEEKDAY, ALL time ranges are not available for the change history graph
const NOT_AVAILABLE_RANGES = [TimeRange.Day, TimeRange.Week, TimeRange.All];
const TIME_RANGE_OPTIONS = AGGRAGATION_TIME_RANGE_OPTIONS.filter(
    (option) => !NOT_AVAILABLE_RANGES.includes(option.value),
);

type ChangeHistoryGraphProps = {
    urn?: string;
};

export default function ChangeHistoryGraph({ urn }: ChangeHistoryGraphProps) {
    const {
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestOperationTime },
    } = useStatsSectionsContext();

    // The time range select
    const timeRangeOptions = useGetTimeRangeOptionsByTimeRange(TIME_RANGE_OPTIONS, oldestOperationTime);
    const [selectedTimeRange, setSelectedTimeRange] = useState<TimeRange>();
    useEffect(() => {
        setSelectedTimeRange(
            timeRangeOptions.length
                ? (timeRangeOptions[timeRangeOptions.length - 1].value as TimeRange)
                : TimeRange.Month,
        );
    }, [timeRangeOptions, setSelectedTimeRange]);

    // The day details drawer
    const [isDayDetailsDrawerShown, setIsDayDetailsDrawerShown] = useState<boolean>(false);
    const [dayOfDayDetailsDrawer, setDayOfDayDetailsDrawer] = useState<string>();
    const showDayDetailsDrawer = (selectedDay: string) => {
        setDayOfDayDetailsDrawer(selectedDay);
        setIsDayDetailsDrawerShown(true);
    };

    useEffect(() => {
        // TODO: Update hasData for 'changes' based on the change history data
        setSectionState('changes', false);
    }, [setSectionState]);

    // Operation types
    const [selectedOperationTypes, setSelectedOperationTypes] = useState<OperationType[]>(DEFAULT_OPERATION_TYPES);
    // The data of change history
    const { data, loading: dataLoading } = useChangeHistoryData(urn, selectedTimeRange);
    // Map of color accessors for day, inserts, updates, deletes
    const colorAccessors = useColorAccessors(data, selectedOperationTypes);
    // The interval of the calendar chart
    const { startDay: calendarStartDay, endDay: calendarEndDay } = useGetCalendarRangeByTimeRange(selectedTimeRange);
    // The interval of the data
    const { startDay: dataStartDay, endDay: dataEndDay } = useDataRange(data, oldestOperationTime);

    const loading = capabilitiesLoading || dataLoading;

    return (
        <>
            <GraphCard
                title="Change History"
                subTitle={
                    <Subtitle data={data} onTypeClick={(operationType) => setSelectedOperationTypes([operationType])} />
                }
                isEmpty={data.length === 0}
                renderControls={() => (
                    <>
                        <AddAssertionButton assertionType={AssertionType.Freshness} />

                        <TypesSelect
                            options={OPERATION_TYPE_OPTIONS}
                            values={selectedOperationTypes}
                            onUpdate={(values) => setSelectedOperationTypes(values as OperationType[])}
                            loading={loading}
                        />

                        <TimeRangeSelect
                            options={timeRangeOptions}
                            values={selectedTimeRange ? [selectedTimeRange] : []}
                            onUpdate={(values) => setSelectedTimeRange(values[0] as TimeRange)}
                            loading={loading}
                        />
                    </>
                )}
                loading={loading}
                renderGraph={() => (
                    <CalendarChart
                        data={data}
                        startDate={calendarStartDay}
                        endDate={calendarEndDay}
                        colorAccessor={colorAccessors.day}
                        onDayClick={(datum) => showDayDetailsDrawer(datum.day)}
                        popoverRenderer={(datum) => (
                            <ChangeHistoryPopover
                                datum={datum}
                                hasData={!!dataStartDay && datum.day >= dataStartDay && datum.day <= dataEndDay}
                                onViewDetails={() => showDayDetailsDrawer(datum.day)}
                                insertsColorAccessor={colorAccessors.inserts}
                                updatesColorAccessor={colorAccessors.updates}
                                deletesColorAccessor={colorAccessors.deletes}
                            />
                        )}
                    />
                )}
                graphHeight="fit-content"
            />

            {urn && isDayDetailsDrawerShown && (
                <ChangeHistoryDrawer
                    selectedDay={dayOfDayDetailsDrawer}
                    urn={urn}
                    open={isDayDetailsDrawerShown}
                    onClose={() => setIsDayDetailsDrawerShown(false)}
                    operationTypes={selectedOperationTypes}
                />
            )}
        </>
    );
}
