import { Button, Icon, Pill, Text } from '@components';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
    ActionButtonsContainer,
    Container,
    DescriptionContainer,
    Dropdown,
    FooterBase,
    LabelContainer,
    LabelsWrapper,
    OptionContainer,
    OptionLabel,
    OptionList,
    Placeholder,
    SearchIcon,
    SearchInput,
    SearchInputContainer,
    SelectAllOption,
    SelectBase,
    SelectLabel,
    SelectValue,
    StyledCancelButton,
    StyledCheckbox,
    StyledClearButton,
} from './components';
import { ActionButtonsProps, SelectLabelDisplayProps, SelectOption, SelectProps } from './types';
import { getFooterButtonSize } from './utils';

const SelectLabelDisplay = ({
    selectedValues,
    options,
    placeholder,
    isMultiSelect,
    removeOption,
    disabledValues,
    showDescriptions,
}: SelectLabelDisplayProps) => {
    const selectedOptions = options.filter((opt) => selectedValues.includes(opt.value));
    return (
        <LabelsWrapper>
            {!!selectedOptions.length &&
                isMultiSelect &&
                selectedOptions.map((o) => {
                    const isDisabled = disabledValues?.includes(o.value);
                    return (
                        <Pill
                            label={o.label}
                            rightIcon={!isDisabled ? 'Close' : ''}
                            size="sm"
                            key={o.value}
                            onClickRightIcon={(e) => {
                                e.stopPropagation();
                                removeOption?.(o);
                            }}
                            clickable={!isDisabled}
                        />
                    );
                })}
            {!selectedValues.length && <Placeholder>{placeholder}</Placeholder>}
            {!isMultiSelect && (
                <>
                    <SelectValue>{selectedOptions[0]?.label}</SelectValue>
                    {showDescriptions && !!selectedValues.length && (
                        <DescriptionContainer>{selectedOptions[0]?.description}</DescriptionContainer>
                    )}
                </>
            )}
        </LabelsWrapper>
    );
};

const SelectActionButtons = ({
    selectedValues,
    isOpen,
    isDisabled,
    isReadOnly,
    showClear,
    handleClearSelection,
    fontSize = 'md',
}: ActionButtonsProps) => {
    return (
        <ActionButtonsContainer>
            {showClear && selectedValues.length > 0 && !isDisabled && !isReadOnly && (
                <StyledClearButton icon="Close" isCircle onClick={handleClearSelection} size={fontSize} />
            )}
            <Icon icon="ChevronLeft" rotate={isOpen ? '90' : '270'} size="xl" color="gray" />
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

export const BasicSelect = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    values = [],
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
    ...props
}: SelectProps) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValues, setSelectedValues] = useState<string[]>(values);
    const [tempValues, setTempValues] = useState<string[]>(values);
    const selectRef = useRef<HTMLDivElement>(null);
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

    return (
        <Container ref={selectRef} size={size || 'md'} width={props.width}>
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
                <SelectLabelDisplay
                    selectedValues={selectedValues}
                    options={options}
                    placeholder={placeholder || 'Select an option'}
                    isMultiSelect={isMultiSelect}
                    removeOption={removeOption}
                    disabledValues={disabledValues}
                    showDescriptions={showDescriptions}
                />
                <SelectActionButtons
                    selectedValues={selectedValues}
                    isOpen={isOpen}
                    isDisabled={!!isDisabled}
                    isReadOnly={!!isReadOnly}
                    handleClearSelection={handleClearSelection}
                    fontSize={size}
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
                    <OptionList>
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
                                        <Text color="gray" weight="semiBold" size="md">
                                            {option.label}
                                        </Text>
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
                    <FooterBase>
                        <StyledCancelButton
                            onClick={handleCancelClick}
                            variant="filled"
                            size={getFooterButtonSize(size)}
                        >
                            Cancel
                        </StyledCancelButton>
                        <Button onClick={handleUpdateClick} size={getFooterButtonSize(size)}>
                            Update
                        </Button>
                    </FooterBase>
                </Dropdown>
            )}
        </Container>
    );
};

export default BasicSelect;
