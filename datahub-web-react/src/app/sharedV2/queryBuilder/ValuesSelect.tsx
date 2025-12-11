/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { SelectParams, ValueInputType, ValueOptions } from '@app/sharedV2/queryBuilder/builder/property/types/values';
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
        default:
            return property ? capitalizeFirstLetterOnly(property) : undefined;
    }
}

interface Props {
    selectedValues?: string[];
    options?: ValueOptions;
    onChangeValues: (newValues: string[]) => void;
    property?: string;
}

const ValuesSelect = ({ selectedValues, options, onChangeValues, property }: Props) => {
    const label = useMemo(() => propertyToValueInputLabel(property), [property]);

    return (
        <>
            {options?.inputType === ValueInputType.ENTITY_SEARCH && (
                <EntitySearchValueInput
                    selectedUrns={selectedValues || []}
                    onChangeSelectedUrns={(newSelected) => onChangeValues(newSelected)}
                    entityTypes={(options.options as any)?.entityTypes || []}
                    mode={(options.options as any)?.mode || 'single'}
                    label={label}
                />
            )}
            {options?.inputType === ValueInputType.SELECT && (
                <SelectValueInput
                    selected={selectedValues}
                    onChangeSelected={(selected) => onChangeValues(selected as string[])}
                    placeholder="Select a value..."
                    options={(options.options as SelectParams)?.options}
                    mode={(options.options as any)?.mode || 'single'}
                    label={label}
                />
            )}
        </>
    );
};

export default ValuesSelect;
