/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useState } from 'react';

export function useEditStructuredProperty(initialValues?: (string | number | null)[]) {
    const [hasEdited, setHasEdited] = useState(false);
    const [selectedValues, setSelectedValues] = useState<any[]>(initialValues || []);

    function selectSingleValue(value: string | number) {
        setHasEdited(true);
        setSelectedValues([value as string]);
    }

    function toggleSelectedValue(value: string | number) {
        setHasEdited(true);
        if (selectedValues.includes(value)) {
            setSelectedValues((prev) => prev.filter((v) => v !== value));
        } else {
            setSelectedValues((prev) => [...prev, value]);
        }
    }

    function updateSelectedValues(values: any[]) {
        setSelectedValues(values);
        setHasEdited(true);
    }

    return {
        selectedValues,
        setSelectedValues,
        selectSingleValue,
        toggleSelectedValue,
        updateSelectedValues,
        hasEdited,
        setHasEdited,
    };
}
