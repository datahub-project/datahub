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
