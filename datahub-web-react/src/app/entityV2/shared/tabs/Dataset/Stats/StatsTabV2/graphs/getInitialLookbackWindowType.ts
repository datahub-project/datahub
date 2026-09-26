import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import {
    getCalendarStartTimeByTimeRange,
    roundTimeByTimeRange,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { TimeRange } from '@src/types.generated';

const WINDOWS_FROM_MONTH: LookbackWindowType[] = [
    LookbackWindowType.Month,
    LookbackWindowType.Quarter,
    LookbackWindowType.HalfOfYear,
    LookbackWindowType.Year,
];

const USAGE_RANGES_FROM_MONTH: TimeRange[] = [TimeRange.Month, TimeRange.Quarter, TimeRange.HalfYear, TimeRange.Year];

type ProfileWithField = {
    timestampMillis?: number | null;
    rowCount?: number | null;
    sizeInBytes?: number | null;
} | null;

function smallestWindowContaining<T>(
    windows: readonly T[],
    time: number,
    windowStart: (window: T) => number | undefined,
    fallback: T,
): T {
    const containing = windows.find((window) => {
        const start = windowStart(window);
        return start !== undefined && start <= time;
    });
    return containing ?? fallback;
}

/**
 * Latest timestamp among profiles that actually have the plotted field.
 * A newer partition with no row count must not keep the row-count chart on 30 days
 * while the full-table row count on the card is older.
 */
export function latestProfileTimeWithField(
    profiles: Array<ProfileWithField | undefined>,
    field: 'rowCount' | 'sizeInBytes',
): number | undefined {
    let latest: number | undefined;
    profiles.forEach((profile) => {
        const value = profile?.[field];
        const time = profile?.timestampMillis;
        if (value == null || typeof time !== 'number' || time <= 0) return;
        if (latest == null || time > latest) latest = time;
    });
    return latest;
}

/**
 * Smallest lookback that still includes `profileTimeMillis`.
 * Week is not a candidate, so a recent profile stays on 30 days.
 */
export function getInitialLookbackWindowType(profileTimeMillis?: number | null): LookbackWindowType {
    if (profileTimeMillis == null || profileTimeMillis <= 0) return LookbackWindowType.Month;

    return smallestWindowContaining(WINDOWS_FROM_MONTH, profileTimeMillis, profileWindowStart, LookbackWindowType.Year);
}

function profileWindowStart(windowType: LookbackWindowType): number {
    return getFixedLookbackWindow(GRAPH_LOOKBACK_WINDOWS[windowType].windowSize).startTime;
}

export function isProfileOutsideMaxLookback(profileTimeMillis?: number | null): boolean {
    if (profileTimeMillis == null || profileTimeMillis <= 0) return false;
    return profileTimeMillis < profileWindowStart(LookbackWindowType.Year);
}

function usageWindowStart(range: TimeRange): number | undefined {
    return roundTimeByTimeRange(getCalendarStartTimeByTimeRange(Date.now(), range), range);
}

/**
 * Query-count ranges use calendar windows, matching `useQueryCountData`.
 * Recent usage stays on a month. With no queries in that month, widen to the
 * oldest known usage instead of leaving the chart empty.
 */
export function getInitialUsageTimeRange(oldestUsageTimeMillis?: number | null, hasRecentUsage?: boolean): TimeRange {
    if (hasRecentUsage || oldestUsageTimeMillis == null || oldestUsageTimeMillis <= 0) return TimeRange.Month;

    return smallestWindowContaining(USAGE_RANGES_FROM_MONTH, oldestUsageTimeMillis, usageWindowStart, TimeRange.Year);
}
