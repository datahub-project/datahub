import { Checkbox, Divider, Select, Input } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { debounce } from 'lodash';
import { ANTD_GRAY } from '../../constants';

const StyledSelect = styled(Select)`
    display: flex;
    flex-direction: column;
    align-items: flex-start;

    .ant-select-selector {
        min-width: 150px;
    }
`;

const StyledInput = styled(Input)`
    margin-bottom: 4px;
`;

const StyledDivider = styled(Divider)`
    margin: 4px 0;
`;

const StyledCheckbox = styled(Checkbox)`
    display: flex;
    font-size: 14px;
    margin-left: 0 !important;
    padding: 8px 4px;
    border-radius: 4px;
    .ant-checkbox-inner {
        border-color: ${ANTD_GRAY[7]};
    }
    .ant-checkbox-wrapper {
        margin-left: 0;
    }

    &:hover {
        background-color: ${ANTD_GRAY[4]};
    }
`;

const DropdownWrapper = styled.div`
    padding: 8px;
    max-height: 350px;
    overflow: auto;
`;

const DROPDOWN_STYLE = { minWidth: 320, maxWidth: 320, textAlign: 'left' };

export interface Option {
    value: string;
    label: string;
}

interface Props {
    options: Option[];
    isDisabled?: boolean;
    selectedValues: string[];
    setSelectedValues: (fieldPaths: string[]) => void;
    placeholder?: string;
    searchPlaceholder?: string;
    valuesLabel?: string;
}

export default function MultiSelectSelector({
    options,
    isDisabled,
    selectedValues,
    setSelectedValues,
    placeholder,
    searchPlaceholder,
    valuesLabel,
}: Props) {
    // isShowingFullList helps us render long lists efficiently by deferring the render of the bottom of the list
    const [isShowingFullList, setIsShowingFullList] = useState(false);
    const [filterText, setFilterText] = useState('');
    const filteredOptions = options.filter((o) => o.label.includes(filterText));
    const areAllSelected = useMemo(
        () => !!options.map((o) => o.value).every((value) => selectedValues.includes(value)),
        [options, selectedValues],
    );

    function handleSelectAll() {
        if (selectedValues.length === 0) {
            const allOptions = filteredOptions.map((o) => o.value);
            setSelectedValues(allOptions);
        } else {
            setSelectedValues([]);
        }
    }

    const allSelectedText = `All ${valuesLabel || 'values'} (${selectedValues.length})`;

    return (
        <StyledSelect
            style={DROPDOWN_STYLE as any}
            mode="multiple"
            value={areAllSelected ? allSelectedText : selectedValues}
            placeholder={placeholder}
            disabled={isDisabled}
            maxTagCount={3}
            onChange={(values) => {
                setSelectedValues(values as string[]);
            }}
            onDropdownVisibleChange={(open) => {
                if (open) {
                    debounce(() => setIsShowingFullList(true), 500)();
                } else {
                    setIsShowingFullList(false);
                }
            }}
            dropdownRender={() => (
                <DropdownWrapper>
                    <StyledInput
                        placeholder={searchPlaceholder}
                        onChange={(e) => setFilterText(e.target.value)}
                        value={filterText}
                        allowClear
                    />
                    <StyledCheckbox
                        checked={areAllSelected}
                        onChange={handleSelectAll}
                        indeterminate={selectedValues.length > 0 && !areAllSelected}
                    >
                        Select all
                    </StyledCheckbox>
                    <StyledDivider />
                    {filteredOptions.slice(0, isShowingFullList ? undefined : 100).map((option) => {
                        const isSelected = selectedValues.includes(option.value);
                        function handleChange() {
                            if (isSelected) {
                                setSelectedValues(selectedValues.filter((f) => f !== option.value));
                            } else {
                                setSelectedValues([...selectedValues, option.value]);
                            }
                        }
                        return (
                            <StyledCheckbox checked={isSelected} onChange={handleChange} key={option.value}>
                                {option.label}
                            </StyledCheckbox>
                        );
                    })}
                </DropdownWrapper>
            )}
        />
    );
}
