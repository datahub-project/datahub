import React from 'react';
import { useTranslation } from 'react-i18next';

import { SelectSkeleton } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/SelectSkeleton';
import { SelectOption, SimpleSelect } from '@src/alchemy-components';

type UsersSelectProps = {
    options: SelectOption[];
    values: string[];
    onUpdate: (selectedValues: string[]) => void;
    loading: boolean;
};

export default function UsersSelect({ options, values, onUpdate, loading }: UsersSelectProps) {
    const { t } = useTranslation('entity.profile.stats');
    if (loading) return <SelectSkeleton active />;

    if (options.length < 2) return null;

    return (
        <SimpleSelect
            selectLabelProps={{ variant: 'labeled', label: t('usersSelect.label') }}
            values={values}
            options={options}
            onUpdate={onUpdate}
            width="full"
            showClear={false}
            isMultiSelect
        />
    );
}
