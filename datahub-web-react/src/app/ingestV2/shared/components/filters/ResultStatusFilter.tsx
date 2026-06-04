import { SimpleSelect } from '@components';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';

import {
    EXECUTION_REQUEST_STATUS_ABORTED,
    EXECUTION_REQUEST_STATUS_CANCELLED,
    EXECUTION_REQUEST_STATUS_DUPLICATE,
    EXECUTION_REQUEST_STATUS_FAILURE,
    EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED,
    EXECUTION_REQUEST_STATUS_ROLLED_BACK,
    EXECUTION_REQUEST_STATUS_ROLLING_BACK,
    EXECUTION_REQUEST_STATUS_RUNNING,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';

export const RESULT_STATUS_ALL_VALUE = 'All Statuses';

interface Props {
    defaultValues?: string[];
    onUpdate?: (selectedValues: string[]) => void;
}

export default function ResultStatusFilter({ defaultValues, onUpdate }: Props) {
    const { t } = useTranslation('ingestion');
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
                { label: t('filters.allStatuses'), value: RESULT_STATUS_ALL_VALUE },
                { label: t('status.success'), value: EXECUTION_REQUEST_STATUS_SUCCESS },
                { label: t('status.failed'), value: EXECUTION_REQUEST_STATUS_FAILURE },
                { label: t('status.running'), value: EXECUTION_REQUEST_STATUS_RUNNING },
                { label: t('status.cancelled'), value: EXECUTION_REQUEST_STATUS_CANCELLED },
                { label: t('status.aborted'), value: EXECUTION_REQUEST_STATUS_ABORTED },
                { label: t('status.rolledBack'), value: EXECUTION_REQUEST_STATUS_ROLLED_BACK },
                { label: t('status.rollingBack'), value: EXECUTION_REQUEST_STATUS_ROLLING_BACK },
                { label: t('status.rollbackFailed'), value: EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED },
                { label: t('status.duplicate'), value: EXECUTION_REQUEST_STATUS_DUPLICATE },
            ]}
            values={values}
            onUpdate={onUpdateHandler}
            showClear={false}
            width="fit-content"
            placeholder={t('filters.resultStatus')}
        />
    );
}
