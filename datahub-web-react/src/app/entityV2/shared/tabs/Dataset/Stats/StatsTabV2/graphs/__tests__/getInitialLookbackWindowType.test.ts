import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import {
    getInitialLookbackWindowType,
    getInitialUsageTimeRange,
    isProfileOutsideMaxLookback,
    latestProfileTimeWithField,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/getInitialLookbackWindowType';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { TimeRange } from '@src/types.generated';

const DAY_MS = 24 * 60 * 60 * 1000;

function windowStart(windowType: LookbackWindowType): number {
    return getFixedLookbackWindow(GRAPH_LOOKBACK_WINDOWS[windowType].windowSize).startTime;
}

describe('getInitialLookbackWindowType', () => {
    it('stays on 30 days when the latest profile is inside that window', () => {
        const recent = windowStart(LookbackWindowType.Month) + DAY_MS;

        expect(getInitialLookbackWindowType(recent)).toBe(LookbackWindowType.Month);
    });

    it('widens when every profile is older than 30 days', () => {
        const olderThanMonth = windowStart(LookbackWindowType.Month) - DAY_MS;

        const windowType = getInitialLookbackWindowType(olderThanMonth);

        expect(windowType).not.toBe(LookbackWindowType.Month);
        expect(windowStart(windowType)).toBeLessThanOrEqual(olderThanMonth);
    });

    it('uses the 6 month window when the latest profile is just outside 3 months', () => {
        const olderThanQuarter = windowStart(LookbackWindowType.Quarter) - DAY_MS;

        expect(getInitialLookbackWindowType(olderThanQuarter)).toBe(LookbackWindowType.HalfOfYear);
    });

    it('uses the year window when the latest profile is older than every shorter window', () => {
        const olderThanYear = windowStart(LookbackWindowType.Year) - DAY_MS;

        expect(getInitialLookbackWindowType(olderThanYear)).toBe(LookbackWindowType.Year);
        expect(isProfileOutsideMaxLookback(olderThanYear)).toBe(true);
    });

    it('defaults to 30 days when no profile time is known', () => {
        expect(getInitialLookbackWindowType(null)).toBe(LookbackWindowType.Month);
        expect(getInitialLookbackWindowType(undefined)).toBe(LookbackWindowType.Month);
    });
});

describe('latestProfileTimeWithField', () => {
    it('ignores a newer partition that does not have the plotted field', () => {
        const fullTableTime = 1_000;
        const newerPartitionTime = 2_000;

        expect(
            latestProfileTimeWithField(
                [
                    { timestampMillis: fullTableTime, rowCount: 100, sizeInBytes: null },
                    { timestampMillis: newerPartitionTime, rowCount: null, sizeInBytes: 50 },
                ],
                'rowCount',
            ),
        ).toBe(fullTableTime);
        expect(
            latestProfileTimeWithField(
                [
                    { timestampMillis: fullTableTime, rowCount: 100, sizeInBytes: null },
                    { timestampMillis: newerPartitionTime, rowCount: null, sizeInBytes: 50 },
                ],
                'sizeInBytes',
            ),
        ).toBe(newerPartitionTime);
    });
});

describe('getInitialUsageTimeRange', () => {
    it('stays on a month when the last month has queries', () => {
        const ancient = Date.now() - 400 * DAY_MS;

        expect(getInitialUsageTimeRange(ancient, true)).toBe(TimeRange.Month);
    });

    it('widens when the last month is empty and older usage exists', () => {
        const ancient = Date.now() - 200 * DAY_MS;

        expect(getInitialUsageTimeRange(ancient, false)).not.toBe(TimeRange.Month);
    });
});
