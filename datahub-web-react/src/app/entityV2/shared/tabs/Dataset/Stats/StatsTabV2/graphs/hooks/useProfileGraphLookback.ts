import { useCallback, useState } from 'react';

import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import {
    getInitialLookbackWindowType,
    isProfileOutsideMaxLookback,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/getInitialLookbackWindowType';
import { toLocalDateString } from '@app/shared/time/timeUtils';

type ManualRange<T extends string> = {
    urn: string | null | undefined;
    range: T;
};

/**
 * Returns `automaticRange` unless the user picked a range for this dataset.
 * The chosen range is computed during render, so a chart fetch started in the
 * same commit does not use the previous dataset's window.
 * A manual pick survives a later timestamp refresh and is dropped when the urn changes.
 */
export function useManualLookback<T extends string>(
    statsEntityUrn: string | null | undefined,
    automaticRange: T,
    isValidRange: (value: string) => value is T,
): { range: T; selectRange: (value: string) => void } {
    const [manualRange, setManualRange] = useState<ManualRange<T> | null>(null);

    if (manualRange && manualRange.urn !== statsEntityUrn) {
        setManualRange(null);
    }

    const range = manualRange && manualRange.urn === statsEntityUrn ? manualRange.range : automaticRange;

    const selectRange = useCallback(
        (value: string) => {
            if (!isValidRange(value)) return;
            setManualRange({ urn: statsEntityUrn, range: value });
        },
        [isValidRange, statsEntityUrn],
    );

    return { range, selectRange };
}

function isLookbackWindowType(value: string): value is LookbackWindowType {
    return value in GRAPH_LOOKBACK_WINDOWS;
}

export default function useProfileGraphLookback(
    profileTimeMillis: number | null | undefined,
    statsEntityUrn: string | null | undefined,
) {
    const automaticRange = getInitialLookbackWindowType(profileTimeMillis);
    const { range, selectRange } = useManualLookback(statsEntityUrn, automaticRange, isLookbackWindowType);

    return {
        rangeType: range,
        lookbackWindow: GRAPH_LOOKBACK_WINDOWS[range],
        selectRangeType: selectRange,
        profileTimeMillis,
        outsideMaxLookback: isProfileOutsideMaxLookback(profileTimeMillis),
    };
}

type Translate = (key: string, options?: Record<string, unknown>) => string;

export function profileChartEmptyMessage(
    t: Translate,
    profileTimeMillis: number | null | undefined,
    outsideMaxLookback: boolean,
): string {
    if (outsideMaxLookback && profileTimeMillis) {
        return t('graph.emptyReportedOutsideRange', { date: toLocalDateString(profileTimeMillis) });
    }
    return t('graph.emptyInSelectedRange');
}
