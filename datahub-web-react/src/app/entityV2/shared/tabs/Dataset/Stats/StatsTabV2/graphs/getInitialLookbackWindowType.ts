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

/**
 * Windows at least as wide as the default 30-day view, shortest first.
 * Week is omitted so a recent profile does not shrink the chart below 30 days.
 */
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

/**
 * Latest timestamp among profiles that actually have the plotted field.
 * A newer partition with no row count must not keep the row-count chart on 30 days
 * while the full-table row count on the card is older.
 */
export function latestProfileTimeWithField(
    profiles: Array<ProfileWithField | undefined>,
    field: 'rowCount' | 'sizeInBytes',
): number | undefined {
    return profiles.reduce<number | undefined>((latest, profile) => {
        const value = profile?.[field];
        const time = profile?.timestampMillis;
        if (value == null || typeof time !== 'number' || time <= 0) return latest;
        if (latest == null || time > latest) return time;
        return latest;
    }, undefined);
}

/**
 * Smallest lookback that still includes `profileTimeMillis`.
 *
 * Stay on 30 days when that profile is inside the month window, or when no time
 * is known. Past a year the window cannot grow any further.
 */
export function getInitialLookbackWindowType(profileTimeMillis?: number | null): LookbackWindowType {
    if (profileTimeMillis == null || profileTimeMillis <= 0) return LookbackWindowType.Month;

    const containing = WINDOWS_FROM_MONTH.find((windowType) => {
        // Same start the chart query uses, so a profile we count as inside the window is actually fetched.
        const { startTime } = getFixedLookbackWindow(GRAPH_LOOKBACK_WINDOWS[windowType].windowSize);
        return startTime <= profileTimeMillis;
    });

    return containing ?? LookbackWindowType.Year;
}

export function isProfileOutsideMaxLookback(profileTimeMillis?: number | null): boolean {
    if (profileTimeMillis == null || profileTimeMillis <= 0) return false;
    const { startTime } = getFixedLookbackWindow(GRAPH_LOOKBACK_WINDOWS[LookbackWindowType.Year].windowSize);
    return profileTimeMillis < startTime;
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

    const containing = USAGE_RANGES_FROM_MONTH.find((range) => {
        const startTime = usageWindowStart(range);
        return startTime !== undefined && startTime <= oldestUsageTimeMillis;
    });

    return containing ?? TimeRange.Year;
}
