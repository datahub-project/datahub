import { CalendarChart, GraphCard } from '@components';
import React, { useEffect, useMemo, useState } from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import { ChangeHistoryDrawer } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/ChangeHistoryDrawer';
import ChangeHistoryPopover from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryPopover';
import Subtitle from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/Subtitle';
import TypesSelect from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/TypesSelect';
import useChangeHistoryData from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useChangeHistoryData';
import useColorAccessors from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useColorAccessors';
import useDataRange from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useDataRange';
import useGetCalendarRangeByTimeRange from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useGetCalendarRangeByTimeRange';
import {
    AnyOperationType,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { addPrefix } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import MoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { DayData } from '@src/alchemy-components/components/CalendarChart/types';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { OperationType, TimeRange } from '@src/types.generated';

const CHANGE_HISTORY_TIME_RANGE = TimeRange.Year;

export default function ChangeHistoryGraph() {
    const {
        dataInfo: { capabilitiesLoading, oldestOperationTime },
        statsEntityUrn,
        permissions: { canViewDatasetOperations },
        sections,
        setSectionState,
    } = useStatsSectionsContext();

    // The data of change history
    const {
        buckets,
        summary,
        defaultOperationTypes,
        customOperationTypes,
        loading: dataLoading,
    } = useChangeHistoryData(statsEntityUrn, CHANGE_HISTORY_TIME_RANGE);

    // Operation types
    const operationTypesOptions = useMemo(
        () => [
            ...defaultOperationTypes.map((operationType) => ({
                value: operationType,
                label: capitalizeFirstLetter(operationType) as string,
            })),
            ...customOperationTypes.map((operationType) => ({
                value: addPrefix(operationType),
                label: operationType,
            })),
        ],
        [defaultOperationTypes, customOperationTypes],
    );
    const prefixedCustomOperationTypes = useMemo(
        () => customOperationTypes.map((operationType) => addPrefix(operationType)),
        [customOperationTypes],
    );
    const [selectedOperationTypes, setSelectedOperationTypes] = useState<AnyOperationType[]>([
        ...defaultOperationTypes,
        ...(prefixedCustomOperationTypes ?? []),
    ]);
    useEffect(() => {
        setSelectedOperationTypes([...defaultOperationTypes, ...(prefixedCustomOperationTypes ?? [])]);
    }, [defaultOperationTypes, prefixedCustomOperationTypes]);

    const toggleOperationType = (operationType: AnyOperationType) => {
        setSelectedOperationTypes((currentOperations) => {
            // Untoggling the last type should select all available types
            if (currentOperations.length === 1 && currentOperations[0] === operationType) {
                return operationTypesOptions.map((option) => option.value);
            }

            // In other cases just select single type
            return [operationType];
        });
    };

    // Map of color accessors for day and each operation type
    const colorAccessors = useColorAccessors(summary, buckets, selectedOperationTypes);
    // The interval of the calendar chart
    const { startDay: calendarStartDay, endDay: calendarEndDay } =
        useGetCalendarRangeByTimeRange(CHANGE_HISTORY_TIME_RANGE);
    // The interval of the data
    const { startDay: dataStartDay, endDay: dataEndDay } = useDataRange(buckets, oldestOperationTime);

    // The day details drawer
    const [isDayDetailsDrawerShown, setIsDayDetailsDrawerShown] = useState<boolean>(false);
    const [dateOfDayDetailsDrawer, setDateOfDayDetailsDrawer] = useState<string | null>();
    const [drawerOperationsData, setDrawerOperationsData] = useState<OperationsData | undefined | null>();
    const showDayDetailsDrawer = (dayData: DayData<OperationsData>) => {
        setDateOfDayDetailsDrawer(dayData.day);
        setIsDayDetailsDrawerShown(true);
    };
    const hideDayDetailsDrawer = () => {
        setDateOfDayDetailsDrawer(null);
        setIsDayDetailsDrawerShown(false);
    };
    useEffect(() => {
        setDrawerOperationsData(buckets?.find((datum) => datum.day === dateOfDayDetailsDrawer)?.value ?? null);
    }, [dateOfDayDetailsDrawer, buckets]);

    const loading = capabilitiesLoading || dataLoading;

    useEffect(() => {
        const currentSection = sections.changes;
        const hasData = canViewDatasetOperations && !loading && buckets.length > 0;

        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.CHANGES, hasData, loading);
        }
    }, [buckets, loading, sections.changes, setSectionState, canViewDatasetOperations]);

    const chartName = 'Change History';

    return (
        <>
            <GraphCard
                title={chartName}
                dataTestId="change-history-card"
                subTitle={
                    <Subtitle
                        summary={summary}
                        onTypeClick={(operationType) => toggleOperationType(operationType)}
                        selectedOperationTypes={selectedOperationTypes}
                    />
                }
                isEmpty={buckets.length === 0 || !canViewDatasetOperations}
                emptyContent={!canViewDatasetOperations && <NoPermission statName="change history" />}
                renderControls={() => (
                    <>
                        <TypesSelect
                            options={operationTypesOptions}
                            values={selectedOperationTypes}
                            onUpdate={(values) => setSelectedOperationTypes(values as OperationType[])}
                            loading={loading}
                        />
                    </>
                )}
                loading={loading}
                renderGraph={() => (
                    <CalendarChart
                        data={buckets}
                        dataTestId={
                            (buckets.length === 0 || !canViewDatasetOperations) && !loading
                                ? 'change-history-chart-empty'
                                : 'change-history-chart'
                        }
                        startDate={calendarStartDay}
                        endDate={calendarEndDay}
                        colorAccessor={colorAccessors.day}
                        selectedDay={dateOfDayDetailsDrawer}
                        onDayClick={(datum) => showDayDetailsDrawer(datum)}
                        showPopover={!isDayDetailsDrawerShown}
                        popoverRenderer={(datum) => (
                            <ChangeHistoryPopover
                                datum={datum}
                                hasData={!!dataStartDay && datum.day >= dataStartDay && datum.day <= dataEndDay}
                                onViewDetails={() => showDayDetailsDrawer(datum)}
                                colorAccessors={colorAccessors}
                                defaultCustomOperationTypes={prefixedCustomOperationTypes}
                                selectedOperationTypes={selectedOperationTypes}
                            />
                        )}
                    />
                )}
                graphHeight="fit-content"
                moreInfoModalContent={<MoreInfoModalContent />}
            />

            {statsEntityUrn && isDayDetailsDrawerShown && (
                <ChangeHistoryDrawer
                    selectedDate={dateOfDayDetailsDrawer}
                    setSelectedDate={setDateOfDayDetailsDrawer}
                    urn={statsEntityUrn}
                    open={isDayDetailsDrawerShown}
                    onClose={() => hideDayDetailsDrawer()}
                    allOperationTypesOptions={operationTypesOptions}
                    operationsData={drawerOperationsData}
                />
            )}
        </>
    );
}
