import { SimpleSelect } from '@components';
import React, { useCallback, useState } from 'react';

import {
    EXECUTION_REQUEST_STATUS_ABORTED,
    EXECUTION_REQUEST_STATUS_CANCELLED,
    EXECUTION_REQUEST_STATUS_DUPLICATE,
    EXECUTION_REQUEST_STATUS_FAILURE,
    EXECUTION_REQUEST_STATUS_PENDING,
    EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED,
    EXECUTION_REQUEST_STATUS_ROLLED_BACK,
    EXECUTION_REQUEST_STATUS_ROLLING_BACK,
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
    EXECUTION_REQUEST_STATUS_UP_FOR_RETRY,
} from '@app/ingestV2/executions/constants';

export const RESULT_STATUS_ALL_VALUE = 'All Statuses';

interface Props {
    defaultValues?: string[];
    onUpdate?: (selectedValues: string[]) => void;
}

export default function ResultStatusFilter({ defaultValues, onUpdate }: Props) {
    const [values, setValues] = useState<string[]>(defaultValues || [RESULT_STATUS_ALL_VALUE]);

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
                { label: 'All Statuses', value: RESULT_STATUS_ALL_VALUE },
                { label: 'Success', value: EXECUTION_REQUEST_STATUS_SUCCESS },
                { label: 'Success with Warnings', value: EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS },
                { label: 'Failed', value: EXECUTION_REQUEST_STATUS_FAILURE },
                { label: 'Running', value: EXECUTION_REQUEST_STATUS_RUNNING },
                { label: 'Cancelled', value: EXECUTION_REQUEST_STATUS_CANCELLED },
                { label: 'Aborted', value: EXECUTION_REQUEST_STATUS_ABORTED },
                { label: 'Pending', value: EXECUTION_REQUEST_STATUS_PENDING },
                { label: 'Up for Retry', value: EXECUTION_REQUEST_STATUS_UP_FOR_RETRY },
                { label: 'Rolled Back', value: EXECUTION_REQUEST_STATUS_ROLLED_BACK },
                { label: 'Rolling Back', value: EXECUTION_REQUEST_STATUS_ROLLING_BACK },
                { label: 'Rollback Failed', value: EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED },
                { label: 'Duplicate', value: EXECUTION_REQUEST_STATUS_DUPLICATE },
            ]}
            values={values}
            onUpdate={onUpdateHandler}
            showClear={false}
            width="fit-content"
            placeholder="Result Status"
        />
    );
}
