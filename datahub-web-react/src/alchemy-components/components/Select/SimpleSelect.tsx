import { Text } from '@components';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import Dropdown from '../Dropdown/Dropdown';
import {
    ActionButtonsContainer,
    Container,
    LabelContainer,
    OptionContainer,
    OptionLabel,
    OptionList,
    PortalDropdownContainer,
    SelectBase,
    SelectLabel,
    SelectLabelContainer,
    StyledCheckbox,
    StyledClearButton,
    StyledIcon,
} from './components';
import { simpleSelectDefaults } from './defaults';
import DropdownSearchBar from './private/dropdown/DropdownSearchBar';
import DropdownSelectAllOption from './private/dropdown/DropdownSelectAllOption';
import SelectLabelRenderer from './private/SelectLabelRenderer/SelectLabelRenderer';
import { ActionButtonsProps, SelectOption, SelectProps } from './types';
import { defaultFilteringPredicate } from './utils';

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

export const SimpleSelect = <OptionType extends SelectOption>({
    options = [],
    label = simpleSelectDefaults.label,
    values = [],
    initialValues,
    onUpdate,
    showSearch = simpleSelectDefaults.showSearch,
    onSearch,
    isDisabled = simpleSelectDefaults.isDisabled,
    isReadOnly = simpleSelectDefaults.isReadOnly,
    isRequired = simpleSelectDefaults.isRequired,
    showClear = simpleSelectDefaults.showClear,
    size = simpleSelectDefaults.size,
    icon,
    isMultiSelect = simpleSelectDefaults.isMultiSelect,
    placeholder = simpleSelectDefaults.placeholder,
    disabledValues = [],
    showSelectAll = simpleSelectDefaults.showSelectAll,
    selectAllLabel = simpleSelectDefaults.selectAllLabel,
    showDescriptions = simpleSelectDefaults.showDescriptions,
    optionListTestId,
    optionSwitchable,
    selectLabelProps,
    filteringPredicate,
    ...props
}: SelectProps<OptionType>) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || values);
    const [areAllSelected, setAreAllSelected] = useState(false);

    useEffect(() => {
        if (values?.length > 0 && !isEqual(selectedValues, values)) {
            setSelectedValues(values);
        }
    }, [values, selectedValues]);

    const onSearchHandler = useCallback(
        (query: string) => {
            setSearchQuery(query);
            onSearch?.(query);
        },
        [onSearch],
    );

    const filteredOptions = useMemo(() => {
        const predicate = filteringPredicate ?? defaultFilteringPredicate;
        return options.filter((option) => predicate(option, searchQuery));
    }, [options, searchQuery, filteringPredicate]);

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
            size={size || 'md'}
            width={props.width || 255}
            $selectLabelVariant={selectLabelProps?.variant}
            isSelected={selectedValues.length > 0}
        >
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            <Dropdown
                open={isOpen}
                onOpenChange={(open) => setIsOpen(open)}
                disabled={isDisabled}
                dropdownRender={() => (
                    <PortalDropdownContainer>
                        {showSearch && (
                            <DropdownSearchBar
                                placeholder="Search…"
                                value={searchQuery}
                                onChange={onSearchHandler}
                                size={size}
                            />
                        )}
                        <OptionList data-testid={optionListTestId}>
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
                    </PortalDropdownContainer>
                )}
            >
                <SelectBase
                    isDisabled={isDisabled}
                    isReadOnly={isReadOnly}
                    isRequired={isRequired}
                    isOpen={isOpen}
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
            </Dropdown>
        </Container>
    );
};
