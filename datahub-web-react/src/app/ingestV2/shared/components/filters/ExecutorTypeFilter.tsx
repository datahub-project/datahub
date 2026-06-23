import { SimpleSelect } from '@components';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';

export const EXECUTOR_TYPE_ALL_VALUE = 'All';
const EXECUTOR_TYPE_UI_VALUE = 'UI';
export const EXECUTOR_TYPE_CLI_VALUE = 'CLI';

interface Props {
    defaultValues?: string[];
    onUpdate?: (selectedValues: string[]) => void;
}

export default function ExecutorTypeFilter({ defaultValues, onUpdate }: Props) {
    const { t } = useTranslation('ingestion');
    const { t: tc } = useTranslation('common.actions');
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
                { label: tc('all'), value: EXECUTOR_TYPE_ALL_VALUE },
                { label: t('filters.ui'), value: EXECUTOR_TYPE_UI_VALUE },
                { label: t('filters.cli'), value: EXECUTOR_TYPE_CLI_VALUE },
            ]}
            values={values}
            onUpdate={onUpdateHandler}
            showClear={false}
            width="fit-content"
        />
    );
}
