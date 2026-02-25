import { Input } from '@components';
import React, { useMemo } from 'react';

import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import {
    AggregationParams,
    SelectParams,
    ValueInputType,
    ValueOptions,
} from '@app/sharedV2/queryBuilder/builder/property/types/values';
import AggregationValueInput from '@app/sharedV2/queryBuilder/valueInputs/AggregationValueInput';
import { EntitySearchValueInput } from '@app/sharedV2/queryBuilder/valueInputs/EntitySearchValueInput';
import SelectValueInput from '@app/sharedV2/queryBuilder/valueInputs/SelectValueInput';

function propertyToValueInputLabel(property: string | undefined): string | undefined {
    switch (property) {
        case 'urn':
            return 'Assets';
        case 'glossaryTerms':
            return 'Terms';
        case '_entityType':
            return 'Types';
        case 'typeNames':
            return 'Sub Types';
        case 'fieldPaths':
            return 'Columns';
        case 'platformInstance':
            return 'Instances';
        case 'owners':
            return 'Owners';
        default:
            return property ? capitalizeFirstLetterOnly(property) : undefined;
    }
}

interface Props {
    selectedValues?: string[];
    options?: ValueOptions;
    onChangeValues: (newValues: string[]) => void;
    property?: string;
    propertyDisplayName?: string;
}

const ValuesSelect = ({ selectedValues, options, onChangeValues, property, propertyDisplayName }: Props) => {
    const label = useMemo(() => propertyToValueInputLabel(property), [property]);
    const placeholder = propertyDisplayName ? `Select ${propertyDisplayName.toLowerCase()}...` : 'Select a value...';

    return (
        <>
            {options?.inputType === ValueInputType.AGGREGATION && (
                <AggregationValueInput
                    facetField={(options.options as AggregationParams)?.facetField}
                    selectedValues={selectedValues || []}
                    onChangeSelectedValues={(newSelected) => onChangeValues(newSelected)}
                    mode={(options.options as any)?.mode || 'multiple'}
                    label={label}
                    placeholder={placeholder}
                />
            )}
            {options?.inputType === ValueInputType.ENTITY_SEARCH && (
                <EntitySearchValueInput
                    selectedUrns={selectedValues || []}
                    onChangeSelectedUrns={(newSelected) => onChangeValues(newSelected)}
                    entityTypes={(options.options as any)?.entityTypes || []}
                    mode={(options.options as any)?.mode || 'single'}
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
                    mode={(options.options as any)?.mode || 'single'}
                    label={label}
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
