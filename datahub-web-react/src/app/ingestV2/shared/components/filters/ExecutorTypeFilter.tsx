/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SimpleSelect } from '@components';
import React, { useCallback, useState } from 'react';

export const EXECUTOR_TYPE_ALL_VALUE = 'All';
export const EXECUTOR_TYPE_UI_VALUE = 'UI';
export const EXECUTOR_TYPE_CLI_VALUE = 'CLI';

interface Props {
    defaultValues?: string[];
    onUpdate?: (selectedValues: string[]) => void;
}

export default function ExecutorTypeFilter({ defaultValues, onUpdate }: Props) {
    const [values, setValues] = useState<string[]>(defaultValues || [EXECUTOR_TYPE_ALL_VALUE]);

    const onUpdateHandler = useCallback(
        (selectedValues: string[]) => {
            setValues(selectedValues);
            onUpdate?.(selectedValues);
        },
        [onUpdate],
    );

    return (
        <SimpleSelect
            options={[
                { label: 'All', value: EXECUTOR_TYPE_ALL_VALUE },
                { label: 'UI', value: EXECUTOR_TYPE_UI_VALUE },
                { label: 'CLI', value: EXECUTOR_TYPE_CLI_VALUE },
            ]}
            values={values}
            onUpdate={onUpdateHandler}
            showClear={false}
            width="fit-content"
        />
    );
}
