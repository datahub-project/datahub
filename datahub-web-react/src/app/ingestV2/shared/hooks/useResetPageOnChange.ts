import { useEffect, useRef } from 'react';

/**
 * Goes back to the first page when a filter value changes. The value from the first render is the
 * starting point, so a page number from the URL is kept on load.
 */
export default function useResetPageOnChange(value: unknown, setPage: (page: number) => void) {
    const previous = useRef(value);

    useEffect(() => {
        if (previous.current !== value) {
            previous.current = value;
            setPage(1);
        }
    }, [value, setPage]);
}
