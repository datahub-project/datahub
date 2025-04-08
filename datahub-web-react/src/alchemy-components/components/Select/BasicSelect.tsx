import { Dropdown, Text } from '@components';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
    ActionButtonsContainer,
    Container,
    DropdownContainer,
    LabelContainer,
    OptionContainer,
    OptionLabel,
    OptionList,
    SelectBase,
    SelectLabel,
    SelectLabelContainer,
    StyledCheckbox,
    StyledClearButton,
    StyledIcon,
} from '@components/components/Select/components';
import DropdownFooterActions from '@components/components/Select/private/DropdownFooterActions';
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import DropdownSelectAllOption from '@components/components/Select/private/DropdownSelectAllOption';
import SelectLabelRenderer from '@components/components/Select/private/SelectLabelRenderer/SelectLabelRenderer';
import { ActionButtonsProps, SelectOption, SelectProps } from '@components/components/Select/types';
import { getFooterButtonSize } from '@components/components/Select/utils';

const SelectActionButtons = ({
    selectedValues,
    isOpen,
    isDisabled,
    isReadOnly,
    showClear,
    handleClearSelection,
}: ActionButtonsProps) => {
    return (
        <ActionButtonsContainer>
            {showClear && selectedValues.length > 0 && !isDisabled && !isReadOnly && (
                <StyledClearButton
                    icon={{ icon: 'Close', source: 'material', size: 'lg' }}
                    isCircle
                    onClick={handleClearSelection}
                />
            )}
            <StyledIcon icon="CaretDown" source="phosphor" rotate={isOpen ? '180' : '0'} size="md" color="gray" />
        </ActionButtonsContainer>
    );
};

// Updated main component
export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    isMultiSelect: false,
    showClear: false,
    placeholder: 'Select an option',
    showSelectAll: false,
    selectAllLabel: 'Select All',
    showDescriptions: false,
};

export const BasicSelect = <OptionType extends SelectOption = SelectOption>({
    options = [],
    label = selectDefaults.label,
    values = [],
    initialValues,
    onCancel,
    onUpdate,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    showClear = selectDefaults.showClear,
    size = selectDefaults.size,
    isMultiSelect = selectDefaults.isMultiSelect,
    placeholder = selectDefaults.placeholder,
    disabledValues = [],
    showSelectAll = selectDefaults.showSelectAll,
    selectAllLabel = selectDefaults.selectAllLabel,
    showDescriptions = selectDefaults.showDescriptions,
    icon,
    renderCustomOptionText,
    selectLabelProps,
    onSearchChange,
    ...props
}: SelectProps<OptionType>) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || values);
    const [tempValues, setTempValues] = useState<string[]>(values);
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const [areAllSelected, setAreAllSelected] = useState(false);

    useEffect(() => {
        if (values?.length > 0 && !isEqual(selectedValues, values)) {
            setSelectedValues(values);
        }
    }, [values, selectedValues]);

    useEffect(() => {
        setAreAllSelected(tempValues.length === options.length);
    }, [options, tempValues]);

    const filteredOptions = useMemo(
        () => options.filter((option) => option.label.toLowerCase().includes(searchQuery.toLowerCase())),
        [options, searchQuery],
    );

    const handleDocumentClick = useCallback((e: MouseEvent) => {
        const clickedOutsideOfSelect = selectRef.current && !selectRef.current.contains(e.target as Node);
        const clickedOutsideOfDropdown = dropdownRef.current && !dropdownRef.current.contains(e.target as Node);

        if (clickedOutsideOfSelect && clickedOutsideOfDropdown) {
            setIsOpen(false);
        }
    }, []);

    useEffect(() => {
        document.addEventListener('click', handleDocumentClick);
        return () => {
            document.removeEventListener('click', handleDocumentClick);
        };
    }, [handleDocumentClick]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            setTempValues(selectedValues);
            setIsOpen((prev) => !prev);
        }
    }, [isDisabled, isReadOnly, selectedValues]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            const updatedValues = tempValues.includes(option.value)
                ? tempValues.filter((val) => val !== option.value)
                : [...tempValues, option.value];

            setTempValues(isMultiSelect ? updatedValues : [option.value]);
        },
        [tempValues, isMultiSelect],
    );

    const removeOption = useCallback(
        (option: SelectOption) => {
            const updatedValues = selectedValues.filter((val) => val !== option.value);
            setSelectedValues(updatedValues);
        },
        [selectedValues],
    );

    const handleUpdateClick = useCallback(() => {
        setSelectedValues(tempValues);
        setIsOpen(false);
        if (onUpdate) {
            onUpdate(tempValues);
        }
    }, [tempValues, onUpdate]);

    const handleCancelClick = useCallback(() => {
        setIsOpen(false);
        setTempValues(selectedValues);
        if (onCancel) {
            onCancel();
        }
    }, [selectedValues, onCancel]);

    const handleClearSelection = useCallback(() => {
        setSelectedValues([]);
        setAreAllSelected(false);
        setTempValues([]);
        setIsOpen(false);
        if (onUpdate) {
            onUpdate([]);
        }
    }, [onUpdate]);

    const handleSelectAll = () => {
        if (areAllSelected) {
            setTempValues([]);
            onUpdate?.([]);
        } else {
            const allValues = options.map((option) => option.value);
            setTempValues(allValues);
            onUpdate?.(allValues);
        }
        setAreAllSelected(!areAllSelected);
    };

    const handleSearchChange = useCallback(
        (value: string) => {
            onSearchChange?.(value);
            setSearchQuery(value);
        },
        [onSearchChange],
    );

    return (
        <Container ref={selectRef} size={size || 'md'} width={props.width}>
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            <Dropdown
                open={isOpen}
                disabled={isDisabled}
                placement="bottomRight"
                dropdownRender={() => (
                    <DropdownContainer ref={dropdownRef}>
                        {showSearch && (
                            <DropdownSearchBar
                                placeholder="Search…"
                                value={searchQuery}
                                onChange={(value) => handleSearchChange(value)}
                                size={size}
                            />
                        )}
                        <OptionList>
                            {showSelectAll && isMultiSelect && (
                                <DropdownSelectAllOption
                                    label={selectAllLabel}
                                    selected={areAllSelected}
                                    disabled={disabledValues.length === options.length}
                                    onClick={() => !(disabledValues.length === options.length) && handleSelectAll()}
                                />
                            )}
                            {filteredOptions.map((option) => (
                                <OptionLabel
                                    key={option.value}
                                    onClick={() => !isMultiSelect && handleOptionChange(option)}
                                    isSelected={tempValues.includes(option.value)}
                                    isMultiSelect={isMultiSelect}
                                    isDisabled={disabledValues?.includes(option.value)}
                                >
                                    {isMultiSelect ? (
                                        <LabelContainer>
                                            {renderCustomOptionText ? (
                                                renderCustomOptionText(option)
                                            ) : (
                                                <span>{option.label}</span>
                                            )}
                                            <StyledCheckbox
                                                onClick={() => handleOptionChange(option)}
                                                checked={tempValues.includes(option.value)}
                                                disabled={disabledValues?.includes(option.value)}
                                            />
                                        </LabelContainer>
                                    ) : (
                                        <OptionContainer>
                                            <ActionButtonsContainer>
                                                {option.icon}
                                                <Text
                                                    color={selectedValues.includes(option.value) ? 'violet' : 'gray'}
                                                    weight="semiBold"
                                                    size="md"
                                                >
                                                    {option.label}
                                                </Text>
                                            </ActionButtonsContainer>
                                            {!!option.description && (
                                                <Text color="gray" weight="normal" size="sm">
                                                    {option.description}
                                                </Text>
                                            )}
                                        </OptionContainer>
                                    )}
                                </OptionLabel>
                            ))}
                        </OptionList>
                        <DropdownFooterActions
                            onCancel={handleCancelClick}
                            onUpdate={handleUpdateClick}
                            size={getFooterButtonSize(size)}
                        />
                    </DropdownContainer>
                )}
            >
                <SelectBase
                    isDisabled={isDisabled}
                    isReadOnly={isReadOnly}
                    isRequired={isRequired}
                    isOpen={isOpen}
                    onClick={handleSelectClick}
                    fontSize={size}
                    {...props}
                >
                    <SelectLabelContainer>
                        {icon && <StyledIcon icon={icon} size="lg" />}
                        <SelectLabelRenderer
                            selectedValues={selectedValues}
                            options={options}
                            placeholder={placeholder || 'Select an option'}
                            isMultiSelect={isMultiSelect}
                            removeOption={removeOption}
                            disabledValues={disabledValues}
                            showDescriptions={showDescriptions}
                            {...(selectLabelProps || {})}
                        />
                    </SelectLabelContainer>
                    <SelectActionButtons
                        selectedValues={selectedValues}
                        isOpen={isOpen}
                        isDisabled={!!isDisabled}
                        isReadOnly={!!isReadOnly}
                        handleClearSelection={handleClearSelection}
                        showClear={!!showClear}
                    />
                </SelectBase>
            </Dropdown>
        </Container>
    );
};

export default BasicSelect;
