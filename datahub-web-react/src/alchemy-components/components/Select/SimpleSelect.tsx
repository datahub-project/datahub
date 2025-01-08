import { Text } from '@components';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
    ActionButtonsContainer,
    Container,
    Dropdown,
    LabelContainer,
    OptionContainer,
    OptionLabel,
    OptionList,
    SearchIcon,
    SearchInput,
    SearchInputContainer,
    SelectAllOption,
    SelectBase,
    SelectLabel,
    SelectLabelContainer,
    StyledCheckbox,
    StyledClearButton,
    StyledIcon,
} from './components';
import SelectLabelRenderer from './private/SelectLabelRenderer/SelectLabelRenderer';
import { ActionButtonsProps, SelectOption, SelectProps } from './types';

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
                <StyledClearButton icon="Close" isCircle onClick={handleClearSelection} iconSize="lg" />
            )}
            <StyledIcon icon="ChevronLeft" rotate={isOpen ? '90' : '270'} size="lg" />
        </ActionButtonsContainer>
    );
};

export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    showClear: true,
    width: 255,
    isMultiSelect: false,
    placeholder: 'Select an option ',
    showSelectAll: false,
    selectAllLabel: 'Select All',
    showDescriptions: false,
};

export const SimpleSelect = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    values = [],
    initialValues,
    onUpdate,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    showClear = selectDefaults.showClear,
    size = selectDefaults.size,
    icon,
    isMultiSelect = selectDefaults.isMultiSelect,
    placeholder = selectDefaults.placeholder,
    disabledValues = [],
    showSelectAll = selectDefaults.showSelectAll,
    selectAllLabel = selectDefaults.selectAllLabel,
    showDescriptions = selectDefaults.showDescriptions,
    optionListTestId,
    optionSwitchable,
    selectLabelProps,
    ...props
}: SelectProps) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || values);
    const selectRef = useRef<HTMLDivElement>(null);
    const [areAllSelected, setAreAllSelected] = useState(false);

    useEffect(() => {
        if (values?.length > 0 && !isEqual(selectedValues, values)) {
            setSelectedValues(values);
        }
    }, [values, selectedValues]);

    useEffect(() => {
        setAreAllSelected(selectedValues.length === options.length);
    }, [options, selectedValues]);

    const filteredOptions = useMemo(
        () => options.filter((option) => option.label.toLowerCase().includes(searchQuery.toLowerCase())),
        [options, searchQuery],
    );

    const handleDocumentClick = useCallback((e: MouseEvent) => {
        if (selectRef.current && !selectRef.current.contains(e.target as Node)) {
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
            setIsOpen((prev) => !prev);
        }
    }, [isDisabled, isReadOnly]);

    const handleOptionChange = useCallback(
        (option: SelectOption) => {
            const updatedValues = selectedValues.includes(option.value)
                ? selectedValues.filter((val) => val !== option.value)
                : [...selectedValues, option.value];

            setSelectedValues(isMultiSelect ? updatedValues : [option.value]);
            if (onUpdate) {
                onUpdate(isMultiSelect ? updatedValues : [option.value]);
            }
            if (!isMultiSelect) setIsOpen(false);
        },
        [onUpdate, isMultiSelect, selectedValues],
    );

    const handleClearSelection = useCallback(() => {
        setSelectedValues([]);
        setAreAllSelected(false);
        setIsOpen(false);
        if (onUpdate) {
            onUpdate([]);
        }
    }, [onUpdate]);

    const handleSelectAll = () => {
        if (areAllSelected) {
            setSelectedValues([]);
            onUpdate?.([]);
        } else {
            const allValues = options.map((option) => option.value);
            setSelectedValues(allValues);
            onUpdate?.(allValues);
        }
        setAreAllSelected(!areAllSelected);
    };

    return (
        <Container
            ref={selectRef}
            size={size || 'md'}
            width={props.width || 255}
            $selectLabelVariant={selectLabelProps?.variant}
            isSelected={selectedValues.length > 0}
        >
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
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
                        removeOption={handleOptionChange}
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
            {isOpen && (
                <Dropdown>
                    {showSearch && (
                        <SearchInputContainer>
                            <SearchInput
                                type="text"
                                placeholder="Search…"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                style={{ fontSize: size || 'md' }}
                            />
                            <SearchIcon icon="Search" size={size} color="gray" />
                        </SearchInputContainer>
                    )}
                    <OptionList data-testid={optionListTestId}>
                        {showSelectAll && isMultiSelect && (
                            <SelectAllOption
                                isSelected={areAllSelected}
                                onClick={() => !(disabledValues.length === options.length) && handleSelectAll()}
                                isDisabled={disabledValues.length === options.length}
                            >
                                <LabelContainer>
                                    <span>{selectAllLabel}</span>
                                    <StyledCheckbox
                                        checked={areAllSelected}
                                        disabled={disabledValues.length === options.length}
                                    />
                                </LabelContainer>
                            </SelectAllOption>
                        )}
                        {filteredOptions.map((option) => (
                            <OptionLabel
                                key={option.value}
                                onClick={() => {
                                    if (!isMultiSelect) {
                                        if (optionSwitchable && selectedValues.includes(option.value)) {
                                            handleClearSelection();
                                        } else {
                                            handleOptionChange(option);
                                        }
                                    }
                                }}
                                isSelected={selectedValues.includes(option.value)}
                                isMultiSelect={isMultiSelect}
                                isDisabled={disabledValues?.includes(option.value)}
                            >
                                {isMultiSelect ? (
                                    <LabelContainer>
                                        <span>{option.label}</span>
                                        <StyledCheckbox
                                            onClick={() => handleOptionChange(option)}
                                            checked={selectedValues.includes(option.value)}
                                            disabled={disabledValues?.includes(option.value)}
                                        />
                                    </LabelContainer>
                                ) : (
                                    <OptionContainer>
                                        <ActionButtonsContainer>
                                            {option.icon}
                                            <Text
                                                weight="semiBold"
                                                size="md"
                                                color={selectedValues.includes(option.value) ? 'violet' : 'gray'}
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
                </Dropdown>
            )}
        </Container>
    );
};
