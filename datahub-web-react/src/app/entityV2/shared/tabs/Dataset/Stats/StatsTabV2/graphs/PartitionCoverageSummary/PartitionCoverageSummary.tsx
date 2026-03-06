import { CalendarChart, GraphCard, Text } from '@components';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import React, { useEffect, useMemo } from 'react';

import { FULL_TABLE_PARTITION_KEYS } from '@app/entityV2/shared/tabs/Dataset/Stats/constants';
import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import useGetCalendarRangeByTimeRange from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/hooks/useGetCalendarRangeByTimeRange';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { useGetLatestPartitionProfilesLazyQuery } from '@graphql/dataset.generated';
import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';
import { TimeRange } from '@types';

dayjs.extend(utc);

type TimePartitionUnit = 'hour' | 'day' | 'month' | 'year';

type TimePartition = {
    unit: TimePartitionUnit;
    value: number;
};

type BucketValue = {
    count: number;
    hours?: number[];
    missingHours?: number[];
};

const EMPTY_MESSAGE = 'No time-based partition stats collected for this asset yet.';
const NO_DATA_COLOR = colors.gray[100];
const COVERAGE_COLORS = [colors.green[100], colors.green[300], colors.green[400], colors.green[500], colors.green[600]];

const UNIT_SUFFIXES: Record<TimePartitionUnit, string> = {
    hour: '_hour',
    day: '_day',
    month: '_month',
    year: '_year',
};

const UNIT_PRIORITY: Record<TimePartitionUnit, number> = {
    hour: 4,
    day: 3,
    month: 2,
    year: 1,
};

const getTimePartitionFromPath = (partition?: string | null): TimePartition | null => {
    if (!partition) return null;
    const parts = partition.split('/');
    const candidates: TimePartition[] = [];

    parts.forEach((part) => {
        const [key, ...rest] = part.split('=');
        const rawValue = rest.join('=');
        if (!key || rawValue.length === 0) return;
        const numericValue = Number(rawValue);
        if (!Number.isFinite(numericValue)) return;

        (Object.keys(UNIT_SUFFIXES) as TimePartitionUnit[]).forEach((unit) => {
            if (key.endsWith(UNIT_SUFFIXES[unit])) {
                candidates.push({ unit, value: numericValue });
            }
        });
    });

    if (candidates.length === 0) return null;
    return candidates.sort((a, b) => UNIT_PRIORITY[b.unit] - UNIT_PRIORITY[a.unit])[0];
};

const getDateFromTimePartition = (unit: TimePartitionUnit, value: number) => {
    if (unit === 'hour') return dayjs.utc(value * 60 * 60 * 1000);
    if (unit === 'day') return dayjs.utc(value * 24 * 60 * 60 * 1000);
    if (unit === 'month') {
        const year = 1970 + Math.floor(value / 12);
        const month = ((value % 12) + 12) % 12;
        return dayjs.utc(Date.UTC(year, month, 1));
    }
    return dayjs.utc(Date.UTC(1970 + value, 0, 1));
};

const getAllHours = () => Array.from({ length: 24 }, (_, hour) => hour);

const PartitionCoverageSummary = () => {
    const {
        statsEntityUrn,
        permissions: { canViewDatasetProfile },
        sections,
        setSectionState,
    } = useStatsSectionsContext();
    const [getLatestPartitionProfiles, { data, loading }] = useGetLatestPartitionProfilesLazyQuery();

    useEffect(() => {
        if (!statsEntityUrn) return;
        getLatestPartitionProfiles({ variables: { urn: statsEntityUrn } });
    }, [statsEntityUrn, getLatestPartitionProfiles]);

    const partitionProfiles = useMemo(() => {
        const profiles = data?.dataset?.latestPartitionProfiles?.partitionProfiles || [];
        return profiles.filter((profile) => !FULL_TABLE_PARTITION_KEYS.includes(profile.partition || ''));
    }, [data]);

    const timePartitions = useMemo(
        () =>
            partitionProfiles
                .map((profile) => getTimePartitionFromPath(profile.partition))
                .filter((partition): partition is TimePartition => !!partition),
        [partitionProfiles],
    );

    const granularity = useMemo<TimePartitionUnit | null>(() => {
        if (timePartitions.length === 0) return null;
        return timePartitions.sort((a, b) => UNIT_PRIORITY[b.unit] - UNIT_PRIORITY[a.unit])[0].unit;
    }, [timePartitions]);

    const calendarRange = useGetCalendarRangeByTimeRange(
        granularity === 'year' || granularity === 'month' ? TimeRange.Year : TimeRange.Day,
    );

    const buckets = useMemo(() => {
        if (!granularity) return [];
        const startDay = dayjs.utc(calendarRange.startDay, CALENDAR_DATE_FORMAT);
        const endDay = dayjs.utc(calendarRange.endDay, CALENDAR_DATE_FORMAT);
        const bucketMap = new Map<string, BucketValue>();

        const addDay = (day: dayjs.Dayjs, update: (current?: BucketValue) => BucketValue) => {
            if (day.isBefore(startDay, 'day') || day.isAfter(endDay, 'day')) return;
            const dayKey = day.format(CALENDAR_DATE_FORMAT);
            bucketMap.set(dayKey, update(bucketMap.get(dayKey)));
        };

        timePartitions.forEach((partition) => {
            const date = getDateFromTimePartition(partition.unit, partition.value);

            if (granularity === 'hour') {
                const hour = date.hour();
                addDay(date, (current) => {
                    const hours = new Set(current?.hours ?? []);
                    hours.add(hour);
                    return { count: hours.size, hours: Array.from(hours).sort((a, b) => a - b) };
                });
                return;
            }

            if (granularity === 'day') {
                addDay(date, (current) => ({ count: (current?.count ?? 0) + 1 }));
                return;
            }

            if (granularity === 'month') {
                const startOfMonth = date.startOf('month');
                const endOfMonth = date.endOf('month');
                let cursor = startOfMonth;
                while (cursor.isBefore(endOfMonth, 'day') || cursor.isSame(endOfMonth, 'day')) {
                    addDay(cursor, (current) => ({ count: Math.max(current?.count ?? 0, 1) }));
                    cursor = cursor.add(1, 'day');
                }
                return;
            }

            const startOfYear = date.startOf('year');
            const endOfYear = date.endOf('year');
            let cursor = startOfYear;
            while (cursor.isBefore(endOfYear, 'day') || cursor.isSame(endOfYear, 'day')) {
                addDay(cursor, (current) => ({ count: Math.max(current?.count ?? 0, 1) }));
                cursor = cursor.add(1, 'day');
            }
        });

        return Array.from(bucketMap.entries()).map(([day, value]) => {
            if (granularity !== 'hour') {
                return { day, value };
            }
            const presentHours = new Set(value.hours ?? []);
            const missingHours = getAllHours().filter((hour) => !presentHours.has(hour));
            return {
                day,
                value: { ...value, missingHours },
            };
        });
    }, [calendarRange.endDay, calendarRange.startDay, granularity, timePartitions]);

    const maxCount = useMemo(() => Math.max(0, ...buckets.map((bucket) => bucket.value.count)), [buckets]);

    useEffect(() => {
        const currentSection = sections.partitionCoverage;
        const hasData = !loading && buckets.length > 0;
        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.PARTITION_COVERAGE, hasData, loading);
        }
    }, [buckets.length, loading, sections.partitionCoverage, setSectionState]);

    const colorAccessor = (value: BucketValue | undefined) => {
        const count = value?.count || 0;
        if (count <= 0 || maxCount <= 0) return NO_DATA_COLOR;
        const ratio = count / maxCount;
        const index = Math.min(COVERAGE_COLORS.length - 1, Math.floor(ratio * COVERAGE_COLORS.length));
        return COVERAGE_COLORS[index];
    };

    const renderPopover = (datum) => {
        if (granularity === 'hour') {
            const present = datum.value?.count || 0;
            const missing = datum.value?.missingHours || [];
            return (
                <Text size="xs">
                    {datum.day}: {present}/24 hours present
                    {missing.length > 0 ? ` (missing: ${missing.join(', ')})` : ''}
                </Text>
            );
        }
        return (
            <Text size="xs">
                {datum.day}: {datum.value?.count || 0} partition{(datum.value?.count || 0) === 1 ? '' : 's'}
            </Text>
        );
    };

    const subtitle = granularity ? `Partitioned by ${granularity}` : undefined;

    return (
        <GraphCard
            title="Partition Coverage"
            subTitle={subtitle}
            isEmpty={buckets.length === 0 || !canViewDatasetProfile}
            emptyContent={!canViewDatasetProfile ? <NoPermission statName="partition coverage" /> : EMPTY_MESSAGE}
            loading={loading}
            graphHeight="fit-content"
            renderGraph={() => (
                <CalendarChart
                    data={buckets}
                    startDate={calendarRange.startDay}
                    endDate={calendarRange.endDay}
                    colorAccessor={colorAccessor}
                    popoverRenderer={renderPopover}
                />
            )}
        />
    );
};

export default PartitionCoverageSummary;
