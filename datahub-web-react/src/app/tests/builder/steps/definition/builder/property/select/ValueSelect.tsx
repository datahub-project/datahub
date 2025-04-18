import React from 'react';
import styled from 'styled-components';
import { Select } from 'antd';
import { SelectParams, ValueInputType, ValueOptions } from '../types/values';
import { SelectInput } from '../input/SelectInput';
import { EntitySearchInput } from '../../../../../../../entity/shared/EntitySearchInput/EntitySearchInput';
import { TimeSelectInput } from '../input/TimeSelectInput';

const StyledSelect = styled(Select)`
    & {
        min-width: 240px;
        margin-right: 12px;
    }
`;

const SelectInputStyle = {
    minWidth: 240,
    marginRight: 12,
};

const EntitySearchInputStyle = {
    minWidth: 240,
};

type Props = {
    selectedValues?: string[];
    options: ValueOptions;
    onChangeValues: (newValues: string[]) => void;
};

/**
 * A single node in a menu tree. This node can have children corresponding
 * to properties that should appear nested inside of it.
 */
export const ValueSelect = ({ selectedValues, options, onChangeValues }: Props) => {
    return (
        <>
            {options.inputType === ValueInputType.TEXT && (
                <StyledSelect
                    value={selectedValues}
                    onChange={(e) => onChangeValues(e as string[])}
                    mode="tags"
                    placeholder="Type a case-insensitive value..."
                />
            )}
            {options.inputType === ValueInputType.ENTITY_SEARCH && (
                <EntitySearchInput
                    selectedUrns={selectedValues || []}
                    onChangeSelectedUrns={(newSelected) => onChangeValues(newSelected)}
                    entityTypes={(options.options as any)?.entityTypes || []}
                    mode={(options.options as any)?.mode || 'single'}
                    style={EntitySearchInputStyle}
                />
            )}
            {options.inputType === ValueInputType.SELECT && (
                <SelectInput
                    selected={selectedValues}
                    onChangeSelected={(selected) => onChangeValues(selected as string[])}
                    placeholder="Select a value..."
                    options={(options.options as SelectParams)?.options}
                    mode={(options.options as any)?.mode || 'single'}
                    style={SelectInputStyle}
                />
            )}
            {options.inputType === ValueInputType.TIME_SELECT && (
                <TimeSelectInput
                    selected={selectedValues}
                    onChangeSelected={(selected) => onChangeValues(selected as string[])}
                    placeholder="Select a time (local)..."
                    style={SelectInputStyle}
                />
            )}
        </>
    );
};
