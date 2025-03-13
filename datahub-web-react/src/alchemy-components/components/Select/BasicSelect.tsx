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
import { basicSelectDefaults } from './defaults';
import DropdownFooter from './private/dropdown/DropdownFooter';
import DropdownSearchBar from './private/dropdown/DropdownSearchBar';
import DropdownSelectAllOption from './private/dropdown/DropdownSelectAllOption';
import SelectLabelRenderer from './private/SelectLabelRenderer/SelectLabelRenderer';
import { ActionButtonsProps, SelectOption, SelectProps } from './types';
import { defaultFilteringPredicate, getFooterButtonSize } from './utils';

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

export const BasicSelect = <OptionType extends SelectOption>({
    options = [],
    label = basicSelectDefaults.label,
    values = [],
    initialValues,
    onCancel,
    onUpdate,
    showSearch = basicSelectDefaults.showSearch,
    isDisabled = basicSelectDefaults.isDisabled,
    isReadOnly = basicSelectDefaults.isReadOnly,
    isRequired = basicSelectDefaults.isRequired,
    showClear = basicSelectDefaults.showClear,
    size = basicSelectDefaults.size,
    isMultiSelect = basicSelectDefaults.isMultiSelect,
    placeholder = basicSelectDefaults.placeholder,
    disabledValues = [],
    showSelectAll = basicSelectDefaults.showSelectAll,
    selectAllLabel = basicSelectDefaults.selectAllLabel,
    showDescriptions = basicSelectDefaults.showDescriptions,
    icon,
    onSearch,
    filteringPredicate,
    selectLabelProps,
    ...props
}: SelectProps<OptionType>) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || values);
    const [tempValues, setTempValues] = useState<string[]>(values);
    const [areAllSelected, setAreAllSelected] = useState(false);

    useEffect(() => {
        if (values?.length > 0 && !isEqual(selectedValues, values)) {
            setSelectedValues(values);
        }
    }, [values, selectedValues]);

    useEffect(() => {
        setAreAllSelected(tempValues.length === options.length);
    }, [options, tempValues]);

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
            onUpdate?.(updatedValues);
        },
        [selectedValues, onUpdate],
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

    return (
        <Container size={size || 'md'} width={props.width}>
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            <Dropdown
                open={isOpen}
                onOpenChange={(open) => setIsOpen(open)}
                disabled={isDisabled}
                dropdownRender={() => {
                    return (
                        <PortalDropdownContainer>
                            {showSearch && (
                                <DropdownSearchBar
                                    placeholder="Search…"
                                    value={searchQuery}
                                    onChange={onSearchHandler}
                                    size={size}
                                />
                            )}
                            <OptionList>
                                {showSelectAll && isMultiSelect && (
                                    <DropdownSelectAllOption
                                        label={selectAllLabel}
                                        selected={areAllSelected}
                                        onClick={() => !(disabledValues.length === options.length) && handleSelectAll()}
                                        disabled={disabledValues.length === options.length}
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
                                                <span>{option.label}</span>
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
                                                    <Text color="gray" weight="normal" size="sm">
                                                        {option.description}
                                                    </Text>
                                                )}
                                            </OptionContainer>
                                        )}
                                    </OptionLabel>
                                ))}
                            </OptionList>
                            <DropdownFooter
                                onCancel={handleCancelClick}
                                onUpdate={handleUpdateClick}
                                size={getFooterButtonSize(size)}
                            />
                        </PortalDropdownContainer>
                    );
                }}
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
