import { Dropdown, Text, colors } from '@components';
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
    StyledIcon,
} from '@components/components/Select/components';
import DropdownFooterActions from '@components/components/Select/private/DropdownFooterActions';
import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';
import DropdownSelectAllOption from '@components/components/Select/private/DropdownSelectAllOption';
import SelectActionButtons from '@components/components/Select/private/SelectActionButtons';
import SelectLabelRenderer from '@components/components/Select/private/SelectLabelRenderer/SelectLabelRenderer';
import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';
import { SelectOption, SelectProps } from '@components/components/Select/types';
import { getFooterButtonSize } from '@components/components/Select/utils';

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
    values,
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
    emptyState,
    descriptionMaxWidth,
    dataTestId,
    autoUpdate = false,
    ...props
}: SelectProps<OptionType>) => {
    const [searchQuery, setSearchQuery] = useState('');
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const {
        isOpen,
        isVisible,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef);

    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || values || []);
    const [tempValues, setTempValues] = useState<string[]>(values || []);
    const [areAllSelected, setAreAllSelected] = useState(false);

    const prevIsOpen = useRef(isOpen);

    const handleUpdateClick = useCallback(() => {
        setSelectedValues(tempValues);
        closeDropdown();
        if (onUpdate) {
            onUpdate(tempValues);
        }
    }, [closeDropdown, tempValues, onUpdate]);

    useEffect(() => {
        if (prevIsOpen.current && !isOpen && autoUpdate) {
            handleUpdateClick();
        }
        prevIsOpen.current = isOpen;
    }, [isOpen, autoUpdate, handleUpdateClick]);

    useEffect(() => {
        if (values !== undefined && !isEqual(selectedValues, values)) {
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

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            setTempValues(selectedValues);
            toggleDropdown();
        }
    }, [isDisabled, isReadOnly, selectedValues, toggleDropdown]);

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
            if (onUpdate) {
                onUpdate(updatedValues);
            }
        },
        [selectedValues, onUpdate],
    );

    const handleCancelClick = useCallback(() => {
        closeDropdown();
        setTempValues(selectedValues);
        if (onCancel) {
            onCancel();
        }
    }, [closeDropdown, selectedValues, onCancel]);

    const handleClearSelection = useCallback(() => {
        setSelectedValues([]);
        setAreAllSelected(false);
        setTempValues([]);
        closeDropdown();
        if (onUpdate) {
            onUpdate([]);
        }
    }, [closeDropdown, onUpdate]);

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
            {isVisible && (
                <Dropdown
                    open={isOpen}
                    disabled={isDisabled}
                    placement="bottomRight"
                    dropdownRender={() => (
                        <DropdownContainer
                            ref={dropdownRef}
                            data-testid={dataTestId ? `${dataTestId}-dropdown` : undefined}
                        >
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
                                {!filteredOptions.length && emptyState}
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
                                                    <div>
                                                        <span>{option.label}</span>
                                                        {!!option.description && [
                                                            <br />,
                                                            <span style={{ color: colors.gray[400] }}>
                                                                {option.description}
                                                            </span>,
                                                        ]}
                                                    </div>
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
                                                        color={
                                                            selectedValues.includes(option.value) ? 'violet' : 'gray'
                                                        }
                                                        weight="semiBold"
                                                        size="md"
                                                    >
                                                        {option.label}
                                                    </Text>
                                                </ActionButtonsContainer>
                                                {!!option.description && (
                                                    <Text
                                                        color="gray"
                                                        weight="normal"
                                                        size="sm"
                                                        style={{ maxWidth: descriptionMaxWidth }}
                                                    >
                                                        {option.description}
                                                    </Text>
                                                )}
                                            </OptionContainer>
                                        )}
                                    </OptionLabel>
                                ))}
                            </OptionList>
                            {!autoUpdate && (
                                <DropdownFooterActions
                                    onCancel={handleCancelClick}
                                    onUpdate={handleUpdateClick}
                                    size={getFooterButtonSize(size)}
                                />
                            )}
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
                        data-testid={dataTestId ? `${dataTestId}-base` : undefined}
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
                            hasSelectedValues={selectedValues.length > 0}
                            isOpen={isOpen}
                            isDisabled={!!isDisabled}
                            isReadOnly={!!isReadOnly}
                            handleClearSelection={handleClearSelection}
                            showClear={!!showClear}
                            fontSize={size}
                        />
                    </SelectBase>
                </Dropdown>
            )}
        </Container>
    );
};

export default BasicSelect;
