import { useCallback, useMemo, useState } from 'react';

export default function useParentValuesToLoadChildren() {
    const [parentValuesSet, setParentValuesSet] = useState<Set<string>>(new Set([]));

    const addParentValue = useCallback((parentValue: string) => {
        setParentValuesSet((prev) => new Set([...prev, parentValue]));
    }, []);

    const removeParentValue = useCallback((parentValue: string) => {
        setParentValuesSet((prev) => new Set(Array.from(prev).filter((value) => value !== parentValue)));
    }, []);

    const parentValues = useMemo(() => Array.from(parentValuesSet), [parentValuesSet]);

    return {
        parentValues,
        addParentValue,
        removeParentValue,
    };
}
