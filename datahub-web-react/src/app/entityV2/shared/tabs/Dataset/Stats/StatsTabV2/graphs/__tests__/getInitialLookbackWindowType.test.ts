import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import { getInitialLookbackWindowType } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/getInitialLookbackWindowType';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';

const DAY_MS = 24 * 60 * 60 * 1000;

function windowStart(windowType: LookbackWindowType): number {
    return getFixedLookbackWindow(GRAPH_LOOKBACK_WINDOWS[windowType].windowSize).startTime;
}

describe('getInitialLookbackWindowType', () => {
    it('stays on 30 days when the latest profile is inside that window', () => {
        const recent = windowStart(LookbackWindowType.Month) + DAY_MS;

        expect(getInitialLookbackWindowType([recent])).toBe(LookbackWindowType.Month);
    });

    it('stays on 30 days when a recent profile exists alongside an older one', () => {
        const recent = windowStart(LookbackWindowType.Month) + DAY_MS;
        const ancient = windowStart(LookbackWindowType.Year) - DAY_MS;

        expect(getInitialLookbackWindowType([ancient, recent])).toBe(LookbackWindowType.Month);
    });

    it('widens when every profile is older than 30 days', () => {
        const olderThanMonth = windowStart(LookbackWindowType.Month) - DAY_MS;

        const windowType = getInitialLookbackWindowType([olderThanMonth]);

        expect(windowType).not.toBe(LookbackWindowType.Month);
        expect(windowStart(windowType)).toBeLessThanOrEqual(olderThanMonth);
    });

    it('uses the 6 month window when the latest profile is just outside 3 months', () => {
        const olderThanQuarter = windowStart(LookbackWindowType.Quarter) - DAY_MS;

        expect(getInitialLookbackWindowType([olderThanQuarter])).toBe(LookbackWindowType.HalfOfYear);
    });

    it('uses the year window when the latest profile is older than every shorter window', () => {
        const olderThanYear = windowStart(LookbackWindowType.Year) - DAY_MS;

        expect(getInitialLookbackWindowType([olderThanYear])).toBe(LookbackWindowType.Year);
    });

    it('defaults to 30 days when no profile time is known', () => {
        expect(getInitialLookbackWindowType([null, undefined])).toBe(LookbackWindowType.Month);
    });
});
