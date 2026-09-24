import { Input } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { getValuesSelectLabel } from '@app/sharedV2/queryBuilder/ValuesSelect.utils';
import {
    AggregationParams,
    SelectParams,
    ValueInputType,
    ValueOptions,
} from '@app/sharedV2/queryBuilder/builder/property/types/values';
import AggregationValueInput from '@app/sharedV2/queryBuilder/valueInputs/AggregationValueInput';
import { EntitySearchValueInput } from '@app/sharedV2/queryBuilder/valueInputs/EntitySearchValueInput';
import SelectValueInput from '@app/sharedV2/queryBuilder/valueInputs/SelectValueInput';
import TimeSelectValueInput from '@app/sharedV2/queryBuilder/valueInputs/TimeSelectValueInput';

const DEFAULT_SINGLE_MODE = 'single' as const;
const DEFAULT_MULTIPLE_MODE = 'multiple' as const;

interface Props {
    selectedValues?: string[];
    options?: ValueOptions;
    onChangeValues: (newValues: string[]) => void;
    property?: string;
    propertyDisplayName?: string;
}

const ValuesSelect = ({ selectedValues, options, onChangeValues, property, propertyDisplayName }: Props) => {
    const { t } = useTranslation('shared.query-builder');
    const label = useMemo(
        () => getValuesSelectLabel(property, t, propertyDisplayName),
        [property, t, propertyDisplayName],
    );
    const placeholder = propertyDisplayName
        ? t('value.placeholder', { propertyDisplayName: propertyDisplayName.toLowerCase() })
        : t('value.defaultPlaceholder');

    return (
        <>
            {options?.inputType === ValueInputType.AGGREGATION && (
                <AggregationValueInput
                    facetField={(options.options as AggregationParams)?.facetField}
                    selectedValues={selectedValues || []}
                    onChangeSelectedValues={(newSelected) => onChangeValues(newSelected)}
                    mode={(options.options as any)?.mode || DEFAULT_MULTIPLE_MODE}
                    label={label}
                    placeholder={placeholder}
                />
            )}
            {options?.inputType === ValueInputType.ENTITY_SEARCH && (
                <EntitySearchValueInput
                    selectedUrns={selectedValues || []}
                    onChangeSelectedUrns={(newSelected) => onChangeValues(newSelected)}
                    entityTypes={(options.options as any)?.entityTypes || []}
                    mode={(options.options as any)?.mode || DEFAULT_SINGLE_MODE}
                    label={label}
                    placeholder={placeholder}
                />
            )}
            {options?.inputType === ValueInputType.SELECT && (
                <SelectValueInput
                    selected={selectedValues}
                    onChangeSelected={(selected) => onChangeValues(selected as string[])}
                    placeholder={placeholder}
                    options={(options.options as SelectParams)?.options}
                    mode={(options.options as any)?.mode || DEFAULT_SINGLE_MODE}
                />
            )}
            {options?.inputType === ValueInputType.TIME_SELECT && (
                <TimeSelectValueInput
                    selected={selectedValues}
                    onChangeSelected={(newSelected) => onChangeValues(newSelected)}
                    placeholder={placeholder}
                />
            )}
            {options?.inputType === ValueInputType.TEXT && (
                <Input
                    value={selectedValues?.[0] ?? ''}
                    setValue={(val) => onChangeValues(val ? [val] : [])}
                    placeholder={placeholder}
                />
            )}
        </>
    );
};

export default ValuesSelect;
