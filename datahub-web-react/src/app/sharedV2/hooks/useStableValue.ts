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
