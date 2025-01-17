import { CalendarChart, GraphCard } from '@components';
import { DayData } from '@src/alchemy-components/components/CalendarChart/types';
import { AssertionType, OperationType, TimeRange } from '@src/types.generated';
import React, { useEffect, useMemo, useState } from 'react';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { SectionKeys } from '../../utils';
import AddAssertionButton from '../components/AddAssertionButton';
import MoreInfoModalContent from '../components/MoreInfoModalContent';
import NoPermission from '../NoPermission';
import { ChangeHistoryDrawer } from './components/ChangeHistoryDrawer/ChangeHistoryDrawer';
import ChangeHistoryPopover from './components/ChangeHistoryPopover';
import Subtitle from './components/Subtitle';
import TypesSelect from './components/TypesSelect';
import useChangeHistoryData from './hooks/useChangeHistoryData';
import useColorAccessors from './hooks/useColorAccessors';
import useDataRange from './hooks/useDataRange';
import useGetCalendarRangeByTimeRange from './hooks/useGetCalendarRangeByTimeRange';
import { AnyOperationType, OperationsData } from './types';
import { addPrefix } from './utils';

const CHANGE_HISTORY_TIME_RANGE = TimeRange.Year;

export default function ChangeHistoryGraph() {
    const {
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestOperationTime },
        statsEntityUrn,
        permissions: { canViewDatasetOperations },
    } = useStatsSectionsContext();

    // The data of change history
    const {
        buckets,
        summary,
        defaultOperationTypes,
        customOperationTypes,
        loading: dataLoading,
    } = useChangeHistoryData(statsEntityUrn, CHANGE_HISTORY_TIME_RANGE);

    // The day details drawer
    const [isDayDetailsDrawerShown, setIsDayDetailsDrawerShown] = useState<boolean>(false);
    const [dayOfDayDetailsDrawer, setDayOfDayDetailsDrawer] = useState<string | null>();
    const [drawerOperationValue, setDrawerOperationValue] = useState<OperationsData | undefined | null>();
    const showDayDetailsDrawer = (dayData: DayData<OperationsData>) => {
        setDayOfDayDetailsDrawer(dayData.day);
        setDrawerOperationValue(dayData.value);
        setIsDayDetailsDrawerShown(true);
    };
    const hideDayDetailsDrawer = () => {
        setDayOfDayDetailsDrawer(null);
        setDrawerOperationValue(null);
        setIsDayDetailsDrawerShown(false);
    };

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

    useEffect(() => {
        if (!sections.changes.hasData && buckets.length > 0) setSectionState(SectionKeys.CHANGES, true);
        else if (!!sections.changes.hasData && !buckets.length) setSectionState(SectionKeys.CHANGES, false);
    }, [buckets, setSectionState, sections.changes]);

    const loading = capabilitiesLoading || dataLoading;

    return (
        <>
            <GraphCard
                title="Change History"
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
                        <AddAssertionButton assertionType={AssertionType.Freshness} />

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
                        startDate={calendarStartDay}
                        endDate={calendarEndDay}
                        colorAccessor={colorAccessors.day}
                        selectedDay={dayOfDayDetailsDrawer}
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
                    selectedDay={dayOfDayDetailsDrawer}
                    urn={statsEntityUrn}
                    open={isDayDetailsDrawerShown}
                    onClose={() => hideDayDetailsDrawer()}
                    operationTypesOptions={operationTypesOptions}
                    value={drawerOperationValue}
                />
            )}
        </>
    );
}
