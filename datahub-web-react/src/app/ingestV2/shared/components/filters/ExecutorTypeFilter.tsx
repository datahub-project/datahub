import { SimpleSelect } from '@components';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';

export const EXECUTOR_TYPE_ALL_VALUE = 'All';
const EXECUTOR_TYPE_UI_VALUE = 'UI';
export const EXECUTOR_TYPE_CLI_VALUE = 'CLI';

interface Props {
    values: string[];
    onUpdate: (selectedValues: string[]) => void;
}

export default function ExecutorTypeFilter({ values, onUpdate }: Props) {
    const { t } = useTranslation('ingestion');
    const { t: tc } = useTranslation('common.actions');

    const onUpdateHandler = useCallback(
        (selectedValues: string[]) => {
            onUpdate(selectedValues);
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
