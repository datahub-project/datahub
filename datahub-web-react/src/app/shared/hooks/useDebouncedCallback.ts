import { DebouncedFunc, debounce } from 'lodash';
import { useEffect, useMemo, useRef } from 'react';

import { DEBOUNCE_SEARCH_MS } from '@app/shared/constants';

/**
 * Debounces `callback` behind a wrapper whose identity is stable for the lifetime of the component.
 *
 * The latest `callback` is read from a ref rather than captured by the debounced function. Without
 * that, passing an inline lambda — the common case for a `Select`'s `onSearch` — would rebuild the
 * debouncer on every render and discard the pending timer, silently defeating the debounce. Any
 * pending invocation is cancelled on unmount so a query can't fire against a torn-down component.
 *
 * Only debounce the expensive part of a handler. Local state that drives what the user sees while
 * typing (an input's value, a dropdown's visibility) must stay synchronous.
 */
export default function useDebouncedCallback<TArgs extends unknown[]>(
    callback: (...args: TArgs) => void,
    delayMs: number = DEBOUNCE_SEARCH_MS,
): DebouncedFunc<(...args: TArgs) => void> {
    const callbackRef = useRef(callback);

    useEffect(() => {
        callbackRef.current = callback;
    }, [callback]);

    const debounced = useMemo(() => debounce((...args: TArgs) => callbackRef.current(...args), delayMs), [delayMs]);

    useEffect(() => () => debounced.cancel(), [debounced]);

    return debounced;
}
