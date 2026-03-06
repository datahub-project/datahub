import { CalendarChart, GraphCard, Text } from '@components';
import dayjs from 'dayjs';
import React, { useEffect, useMemo, useState } from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import useGetCalendarRangeByTimeRange from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useGetCalendarRangeByTimeRange';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import HistorySidebar from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar';
import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';
import { useGetTimelineQuery } from '@graphql/timeline.generated';
import { ChangeCategoryType, TimeRange } from '@types';

const EMPTY_MESSAGE = 'No schema changes recorded for this asset yet.';
const CHANGE_HISTORY_TIME_RANGE = TimeRange.Year;
const NO_DATA_COLOR = colors.gray[100];
const CHANGE_COLORS = [colors.blue[100], colors.blue[300], colors.blue[400], colors.blue[500], colors.blue[600]];

const SchemaChangeSummary = () => {
    const {
        statsEntityUrn,
        permissions: { canViewDatasetProfile },
        sections,
        setSectionState,
    } = useStatsSectionsContext();

    const { data, loading } = useGetTimelineQuery({
        skip: !statsEntityUrn,
        variables: {
            input: { urn: statsEntityUrn || '', changeCategories: [ChangeCategoryType.TechnicalSchema] },
        },
    });
    const [selectedDay, setSelectedDay] = useState<string | null>(null);
    const [isDrawerOpen, setIsDrawerOpen] = useState(false);

    const entries = useMemo(() => {
        const transactions = data?.getTimeline?.changeTransactions || [];
        return transactions.flatMap((transaction) =>
            (transaction.changes || [])
                .filter((change) => change?.category === ChangeCategoryType.TechnicalSchema)
                .map(() => ({
                    timestampMillis: transaction.timestampMillis,
                })),
        );
    }, [data]);

    const buckets = useMemo(() => {
        const counts = new Map<string, number>();
        entries.forEach((entry) => {
            const day = dayjs(entry.timestampMillis).format(CALENDAR_DATE_FORMAT);
            counts.set(day, (counts.get(day) || 0) + 1);
        });
        return Array.from(counts.entries()).map(([day, count]) => ({
            day,
            value: { count },
        }));
    }, [entries]);

    const maxCount = useMemo(() => Math.max(0, ...buckets.map((bucket) => bucket.value.count)), [buckets]);

    const { startDay: calendarStartDay, endDay: calendarEndDay } =
        useGetCalendarRangeByTimeRange(CHANGE_HISTORY_TIME_RANGE);

    useEffect(() => {
        const currentSection = sections.changes;
        const hasData = !loading && buckets.length > 0;
        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.CHANGES, hasData, loading);
        }
    }, [buckets.length, loading, sections.changes, setSectionState]);

    const colorAccessor = (value: { count: number } | undefined) => {
        const count = value?.count || 0;
        if (count <= 0 || maxCount <= 0) return NO_DATA_COLOR;
        const ratio = count / maxCount;
        const index = Math.min(CHANGE_COLORS.length - 1, Math.floor(ratio * CHANGE_COLORS.length));
        return CHANGE_COLORS[index];
    };

    return (
        <>
            <GraphCard
                title="Schema Change History"
                isEmpty={buckets.length === 0 || !canViewDatasetProfile}
                emptyContent={!canViewDatasetProfile ? <NoPermission statName="schema change history" /> : EMPTY_MESSAGE}
                loading={loading}
                graphHeight="fit-content"
                renderGraph={() => (
                    <CalendarChart
                        data={buckets}
                        startDate={calendarStartDay}
                        endDate={calendarEndDay}
                        colorAccessor={colorAccessor}
                        selectedDay={selectedDay}
                        onDayClick={(datum) => {
                            if (!datum.value?.count) return;
                            setSelectedDay(datum.day);
                            setIsDrawerOpen(true);
                        }}
                        popoverRenderer={(datum) => (
                            <Text size="xs">
                                {datum.day}: {datum.value?.count || 0} change
                                {(datum.value?.count || 0) === 1 ? '' : 's'}
                            </Text>
                        )}
                    />
                )}
            />
            {statsEntityUrn && (
                <HistorySidebar
                    open={isDrawerOpen}
                    onClose={() => setIsDrawerOpen(false)}
                    urn={statsEntityUrn}
                    versionList={[]}
                    hideSemanticVersions
                    filterDay={selectedDay || undefined}
                />
            )}
        </>
    );
};

export default SchemaChangeSummary;
