import { SelectOption } from '@components';
import { useCallback, useEffect, useMemo, useState } from 'react';

interface Option<T> extends SelectOption {
    item: T;
}

function mergeArrays<T>(arrayA: Array<T>, arrayB: Array<T>, keyGetter: (item: T) => any): Array<T> {
    const keysFromArrayB = arrayB.map(keyGetter);
    return [...arrayA.filter((item) => !keysFromArrayB.includes(keyGetter(item))), ...arrayB];
}

interface Response<T> {
    options: Option<T>[];
    onSelectedValuesChanged: (selectedValues: string[]) => void;
}

export default function useOptions<T>(
    defaultItems: T[],
    items: T[],
    itemToOptionConverter: (item: T) => Option<T>,
): Response<T> {
    const [appliedOptions, setAppliedOptions] = useState<Option<T>[]>([]);

    const optionsFromDefaultItems = useMemo(
        () => defaultItems.map(itemToOptionConverter),
        [defaultItems, itemToOptionConverter],
    );
    const optionsFromItems = useMemo(() => items.map(itemToOptionConverter), [items, itemToOptionConverter]);

    useEffect(() => setAppliedOptions(optionsFromDefaultItems), [optionsFromDefaultItems]);

    const options: Option<T>[] = useMemo(
        () => mergeArrays(appliedOptions, optionsFromItems, (item) => item.value),
        [appliedOptions, optionsFromItems],
    );

    const onSelectedValuesChanged = useCallback(
        (selectedValues: string[]) => {
            setAppliedOptions((prevAppliedOptions) =>
                mergeArrays(
                    // remove unselected options
                    prevAppliedOptions.filter((option) => selectedValues.includes(option.value)),
                    // add selected options
                    optionsFromItems.filter((option) => selectedValues.includes(option.value)),
                    (item) => item.value,
                ),
            );
        },
        [optionsFromItems],
    );

    return {
        options,
        onSelectedValuesChanged,
    };
}
