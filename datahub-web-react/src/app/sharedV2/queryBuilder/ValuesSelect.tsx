import { colors, typography } from '@components';
import React from 'react';

import { SelectInput } from '@app/sharedV2/queryBuilder/builder/property/input/SelectInput';
import { SelectParams, ValueInputType, ValueOptions } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { EntitySearchInput } from '@src/app/entityV2/shared/EntitySearchInput/EntitySearchInput';

const EntitySearchInputStyle = {
    minWidth: 250,
    margin: 12,
};

const SelectInputStyle = {
    minWidth: 250,
    margin: 12,
};

interface Props {
    selectedValues?: string[];
    options?: ValueOptions;
    onChangeValues: (newValues: string[]) => void;
}

const ValuesSelect = ({ selectedValues, options, onChangeValues }: Props) => {
    return (
        <>
            {options?.inputType === ValueInputType.ENTITY_SEARCH && (
                <EntitySearchInput
                    selectedUrns={selectedValues || []}
                    onChangeSelectedUrns={(newSelected) => onChangeValues(newSelected)}
                    entityTypes={(options.options as any)?.entityTypes || []}
                    mode={(options.options as any)?.mode || 'single'}
                    style={EntitySearchInputStyle}
                    tagStyle={{ fontSize: 12, color: colors.gray[500] }}
                    optionStyle={{ fontSize: 14, fontFamily: typography.fonts.body, color: colors.gray[500] }}
                />
            )}
            {options?.inputType === ValueInputType.SELECT && (
                <SelectInput
                    selected={selectedValues}
                    onChangeSelected={(selected) => onChangeValues(selected as string[])}
                    placeholder="Select a value..."
                    options={(options.options as SelectParams)?.options}
                    mode={(options.options as any)?.mode || 'single'}
                    style={SelectInputStyle}
                    tagStyle={{ fontSize: 12, color: colors.gray[500] }}
                    optionStyle={{ fontSize: 14, fontFamily: typography.fonts.body, color: colors.gray[500] }}
                />
            )}
        </>
    );
};

export default ValuesSelect;
