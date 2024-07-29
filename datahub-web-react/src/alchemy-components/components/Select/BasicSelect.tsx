import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';

import { Button, Icon } from '@components';

import {
    SelectBase,
    OptionList,
    OptionLabel,
    FooterBase,
    Container,
    Dropdown,
    SearchInputContainer,
    StyledCancelButton,
    SearchInput,
    SelectLabel,
    SearchIcon,
    StyledClearButton,
    SelectValue,
    Placeholder,
    ActionButtonsContainer,
} from './components';

import { SelectProps, SelectOption, ActionButtonsProps, SelectLabelDisplayProps } from './types';
import { getFooterButtonSize } from './utils';

const SelectLabelDisplay = ({ selectedValue, options, placeholder }: SelectLabelDisplayProps) => {
    const selectedOption = options.find((opt) => opt.value === selectedValue);
    return (
        <div>
            {selectedOption ? (
                <SelectValue>{selectedOption.label}</SelectValue>
            ) : (
                <Placeholder>{placeholder}</Placeholder>
            )}
        </div>
    );
};

const SelectActionButtons = ({
    selectedValue,
    isOpen,
    isDisabled,
    isReadOnly,
    handleClearSelection,
    fontSize = 'md',
}: ActionButtonsProps) => {
    return (
        <ActionButtonsContainer>
            {selectedValue && !isDisabled && !isReadOnly && (
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
};

export const BasicSelect = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    value = '',
    onCancel,
    onUpdate,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    size = selectDefaults.size,
    ...props
}: SelectProps) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const [selectedValue, setSelectedValue] = useState<string>(value);
    const [tempValue, setTempValue] = useState<string>(value);
    const selectRef = useRef<HTMLDivElement>(null);

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
            setTempValue(selectedValue);
            setIsOpen((prev) => !prev);
        }
    }, [isDisabled, isReadOnly, selectedValue]);

    const handleOptionChange = useCallback((option: SelectOption) => {
        setTempValue(option.value);
    }, []);

    const handleUpdateClick = useCallback(() => {
        setSelectedValue(tempValue);
        setIsOpen(false);
        if (onUpdate) {
            onUpdate([tempValue]);
        }
    }, [tempValue, onUpdate]);

    const handleCancelClick = useCallback(() => {
        setIsOpen(false);
        setTempValue(selectedValue);
        if (onCancel) {
            onCancel();
        }
    }, [selectedValue, onCancel]);

    const handleClearSelection = useCallback(() => {
        setSelectedValue('');
        setTempValue('');
        setIsOpen(false);
        if (onUpdate) {
            onUpdate([]);
        }
    }, [onUpdate]);

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
                <SelectLabelDisplay selectedValue={selectedValue} options={options} placeholder="Select an option" />
                <SelectActionButtons
                    selectedValue={selectedValue}
                    isOpen={isOpen}
                    isDisabled={!!isDisabled}
                    isReadOnly={!!isReadOnly}
                    handleClearSelection={handleClearSelection}
                    fontSize={size}
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
                                fontSize={size || 'md'}
                            />
                            <SearchIcon icon="Search" size={size} color="gray" />
                        </SearchInputContainer>
                    )}
                    <OptionList>
                        {filteredOptions.map((option) => (
                            <OptionLabel
                                key={option.value}
                                onClick={() => handleOptionChange(option)}
                                isSelected={option.value === tempValue}
                            >
                                {option.label}
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
