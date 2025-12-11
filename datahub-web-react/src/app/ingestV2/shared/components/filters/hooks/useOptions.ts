/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SelectOption } from '@components';
import { useCallback, useEffect, useMemo, useState } from 'react';

import { mergeArraysOfObjects } from '@app/utils/arrayUtils';

interface Option<T> extends SelectOption {
    item: T;
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
        () => mergeArraysOfObjects(appliedOptions, optionsFromItems, (item) => item.value),
        [appliedOptions, optionsFromItems],
    );

    const onSelectedValuesChanged = useCallback(
        (selectedValues: string[]) => {
            setAppliedOptions((prevAppliedOptions) =>
                mergeArraysOfObjects(
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
