import { SelectOption, SimpleSelect } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { SelectSkeleton } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/SelectSkeleton';

type TypesSelectProps = {
    options: SelectOption[];
    values: string[];
    loading: boolean;
    onUpdate: (values: string[]) => void;
};

export default function TypesSelect({ options, values, loading, onUpdate }: TypesSelectProps) {
    const { t } = useTranslation('entity.profile.stats');
    if (loading) return <SelectSkeleton active />;

    // Hide TypesSelect when there are only one option or less
    if (options.length < 2) return null;

    return (
        <SimpleSelect
            dataTestId="types-select"
            placeholder={t('typesSelect.placeholder')}
            selectLabelProps={{ variant: 'labeled', label: t('typesSelect.label') }}
            options={options}
            values={values}
            onUpdate={onUpdate}
            width="full"
            showClear={false}
            isMultiSelect
        />
    );
}
