import { SimpleSelect } from '@components';
import React, { useCallback } from 'react';

export const EXECUTOR_TYPE_ALL_VALUE = 'All';
export const EXECUTOR_TYPE_UI_VALUE = 'UI';
export const EXECUTOR_TYPE_CLI_VALUE = 'CLI';

interface Props {
    values: string[];
    onUpdate: (selectedValues: string[]) => void;
}

export default function ExecutorTypeFilter({ values, onUpdate }: Props) {
    const onUpdateHandler = useCallback(
        (selectedValues: string[]) => {
            onUpdate(selectedValues);
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
