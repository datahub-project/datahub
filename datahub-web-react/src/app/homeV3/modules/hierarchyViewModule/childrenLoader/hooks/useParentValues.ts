/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
