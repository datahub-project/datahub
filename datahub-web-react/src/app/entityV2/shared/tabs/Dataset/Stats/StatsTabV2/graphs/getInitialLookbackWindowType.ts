import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';

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

/**
 * Smallest lookback that still includes the most recent profile.
 *
 * Row count and storage charts default to 30 days. When every profile is older
 * than that, the chart is empty and the empty state used to read as "this asset
 * was never profiled". Widen the initial window instead of claiming that.
 * Stay on 30 days when the latest profile is inside it, or when no profile time
 * is known.
 */
export function getInitialLookbackWindowType(profileTimesMillis: Array<number | null | undefined>): LookbackWindowType {
    const latestProfileTime = profileTimesMillis.reduce<number>((latest, time) => {
        if (typeof time === 'number' && time > latest) return time;
        return latest;
    }, 0);

    if (latestProfileTime === 0) return LookbackWindowType.Month;

    const containing = WINDOWS_FROM_MONTH.find((windowType) => {
        // Same start the chart query uses, so a profile we count as inside the window is actually fetched.
        const { startTime } = getFixedLookbackWindow(GRAPH_LOOKBACK_WINDOWS[windowType].windowSize);
        return startTime <= latestProfileTime;
    });

    return containing ?? LookbackWindowType.Year;
}
