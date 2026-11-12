import { SimpleSelect } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { UnionType } from '@app/searchV2/utils/constants';

type Props = {
    unionType: UnionType;
    onUpdate: (newValue: UnionType) => void;
    disabled?: boolean;
};

export const AdvancedSearchFilterOverallUnionTypeSelect = ({ unionType, onUpdate, disabled = false }: Props) => {
    const { t } = useTranslation('search');

    // UnionType is a numeric enum; the select works with string values.
    const options = useMemo(
        () => [
            { value: String(UnionType.AND), label: t('advancedSearch.allFilters') },
            { value: String(UnionType.OR), label: t('advancedSearch.anyFilter') },
        ],
        [t],
    );

    return (
        <SimpleSelect
            options={options}
            values={[String(unionType)]}
            onUpdate={(values) => {
                const newValue = Number(values[0]) as UnionType;
                if (values.length && newValue !== unionType) {
                    onUpdate(newValue);
                }
            }}
            isDisabled={disabled}
            showClear={false}
            width="fit-content"
            size="sm"
        />
    );
};
