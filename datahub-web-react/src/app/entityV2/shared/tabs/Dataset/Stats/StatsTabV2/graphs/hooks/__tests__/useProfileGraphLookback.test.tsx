import { act, renderHook } from '@testing-library/react-hooks';

import { LookbackWindowType } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import { getInitialLookbackWindowType } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/getInitialLookbackWindowType';
import useProfileGraphLookback from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useProfileGraphLookback';

const DAY_MS = 24 * 60 * 60 * 1000;

describe('useProfileGraphLookback', () => {
    it('keeps a manual range when the profile time changes and resets it for a new dataset', () => {
        const olderThanMonth = Date.now() - 40 * DAY_MS;
        const recent = Date.now() - DAY_MS;
        const widened = getInitialLookbackWindowType(olderThanMonth);

        const { result, rerender } = renderHook(({ time, urn }) => useProfileGraphLookback(time, urn), {
            initialProps: { time: olderThanMonth, urn: 'urn:li:dataset:one' },
        });

        expect(result.current.rangeType).toBe(widened);
        expect(widened).not.toBe(LookbackWindowType.Month);

        act(() => {
            result.current.selectRangeType(LookbackWindowType.Week);
        });
        rerender({ time: recent, urn: 'urn:li:dataset:one' });

        expect(result.current.rangeType).toBe(LookbackWindowType.Week);

        rerender({ time: recent, urn: 'urn:li:dataset:two' });

        expect(result.current.rangeType).toBe(LookbackWindowType.Month);
    });
});
