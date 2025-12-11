/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import isEqual from 'lodash/isEqual';
import { useMemo, useRef } from 'react';

export function useStableValue<T>(value: T, compare: (a: T, b: T) => boolean = isEqual): T {
    const prevValueRef = useRef<T | null | undefined>(undefined);

    const stableValue = useMemo(() => {
        if (
            prevValueRef.current === undefined ||
            prevValueRef.current === null ||
            !compare(prevValueRef.current, value)
        ) {
            prevValueRef.current = value;
        }
        return prevValueRef.current as T;
    }, [value, compare]);

    return stableValue;
}
